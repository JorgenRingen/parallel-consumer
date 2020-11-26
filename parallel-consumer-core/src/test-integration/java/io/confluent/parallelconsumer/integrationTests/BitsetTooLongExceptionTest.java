package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.StringUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.presentation.StandardRepresentation;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Just a very simple POC-test do demonstrate bitset-too-long exception while running with
 * very high options.
 *
 * This fairly consistently occurs in output-log while running and then the consumer shuts down:
 * "Caused by: java.lang.RuntimeException: Bitset too long to encode: 45975. (max: 32767)"
 *
 */
@Slf4j
public class BitsetTooLongExceptionTest extends BrokerIntegrationTest<String, String> {

    int HIGH_MAX_POLL_RECORDS_CONFIG = 10_000;

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    public List<String> producedKeysAcknowledged = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger producedCount = new AtomicInteger(0);


    @Test
    public void shouldNotThrowBitsetTooLongException() {
        runTest(HIGH_MAX_POLL_RECORDS_CONFIG, CommitMode.CONSUMER_ASYNCHRONOUS, ProcessingOrder.UNORDERED);
    }

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());
        String outputName = setupTopic(this.getClass().getSimpleName() + "-output-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();
        int expectedMessageCount = 1_000_000;
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < expectedMessageCount; i++) {
                String key = "key-" + i;
                Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
                });
                sends.add(send);
                expectedKeys.add(key);
            }
            log.debug("Finished sending test data");
        }
        // make sure we finish sending before next stage
        log.debug("Waiting for broker acks");
        for (Future<RecordMetadata> send : sends) {
            send.get();
        }
        assertThat(sends).hasSize(expectedMessageCount);

        // run parallel-consumer
        log.debug("Starting test");
        KafkaProducer<String, String> newProducer = kcu.createNewProducer(commitMode.equals(TRANSACTIONAL_PRODUCER));

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(true, consumerProps);

        var pc = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .consumer(newConsumer)
                .producer(newProducer)
                .commitMode(commitMode)
                .numberOfThreads(100)
                .maxNumberMessagesBeyondBaseCommitOffset(10_000)
                .maxMessagesToQueue(10_000)
                .build());
        pc.subscribe(of(inputName));

        // sanity
        TopicPartition tp = new TopicPartition(inputName, 0);
        Map<TopicPartition, Long> beginOffsets = newConsumer.beginningOffsets(of(tp));
        Map<TopicPartition, Long> endOffsets = newConsumer.endOffsets(of(tp));
        assertThat(endOffsets.get(tp)).isEqualTo(expectedMessageCount);
        assertThat(beginOffsets.get(tp)).isEqualTo(0L);


        pc.pollAndProduce(record -> {
                    log.trace("Still going {}", record);
                    consumedKeys.add(record.key());
                    processedCount.incrementAndGet();
                    return new ProducerRecord<>(outputName, record.key(), "data");
                }, consumeProduceResult -> {
                    producedCount.incrementAndGet();
                    producedKeysAcknowledged.add(consumeProduceResult.getIn().key());
                }
        );

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = StringUtils.msg("All keys sent to input-topic should be processed and produced, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        try {
            waitAtMost(ofSeconds(20)).alias(failureMessage).untilAsserted(() -> {
                log.info("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
                SoftAssertions all = new SoftAssertions();
                all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
                all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
                all.assertAll();
            });
        } catch (ConditionTimeoutException e) {
//            log.debug("Expected keys (size {})", expectedKeys.size());
//            log.debug("Consumed keys ack'd (size {})", consumedKeys.size());
//            log.debug("Produced keys (size {})", producedKeysAcknowledged.size());
//            expectedKeys.removeAll(consumedKeys);
//            log.info("Missing keys from consumed: {}", expectedKeys);
            fail(failureMessage + "\n" + e.getMessage());
        }

        pc.closeDrainFirst();

        assertThat(processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(producedCount.get());

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
        assertThat(producedKeysAcknowledged).hasSameSizeAs(expectedKeys);
    }

    /**
     * Trims long lists to make large List assertions more readable
     */
    @Slf4j
    private static class TrimListRepresentation extends StandardRepresentation {

        private final int sizeLimit = 10;

        protected static String msg = "Collection has been trimmed...";

        @Override
        public String toStringOf(Object o) {
            if (o instanceof Set) {
                Set aSet = (Set) o;
                if (aSet.size() > sizeLimit)
                    o = aSet.stream().collect(Collectors.toList());
            }
            if (o instanceof Object[]) {
                Object[] anObjectArray = (Object[]) o;
                if (anObjectArray.length > sizeLimit)
                    o = Arrays.stream(anObjectArray).collect(Collectors.toList());
            }
            if (o instanceof String[]) {
                Object[] anObjectArray = (Object[]) o;
                if (anObjectArray.length > sizeLimit)
                    o = Arrays.stream(anObjectArray).collect(Collectors.toList());
            }
            if (o instanceof List) {
                List<?> aList = (List<?>) o;
                if (aList.size() > sizeLimit) {
                    log.trace("List too long ({}), trimmed...", aList.size());
                    List trimmedListView = aList.subList(0, sizeLimit);
                    // don't mutate backing lists
                    CopyOnWriteArrayList copy = Lists.newCopyOnWriteArrayList(trimmedListView);
                    copy.add(msg);
                    return super.toStringOf(copy);
                }
            }
            return super.toStringOf(o);
        }
    }

    @Test
    void customRepresentationFail() {
        List<Integer> one = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        List<Integer> two = IntStream.range(999, 2000).boxed().collect(Collectors.toList());
        assertThatThrownBy(() -> assertThat(one).withRepresentation(new TrimListRepresentation()).containsAll(two)).hasMessageContaining(TrimListRepresentation.msg);
    }

    @Test
    void customRepresentationPass() {
        Assertions.useRepresentation(new TrimListRepresentation());
        List<Integer> one = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        List<Integer> two = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        SoftAssertions all = new SoftAssertions();
        all.assertThat(one).containsAll(two);
        all.assertAll();
    }


}
