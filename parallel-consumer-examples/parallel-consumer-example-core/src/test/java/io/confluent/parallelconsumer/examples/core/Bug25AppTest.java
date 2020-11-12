package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.integrationTests.KafkaTest;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
public class Bug25AppTest extends KafkaTest<String, String> {

    int LOW_MAX_POLL_RECORDS_CONFIG = 1;
    int DEFAULT_MAX_POLL_RECORDS_CONFIG = 500;
    int HIGH_MAX_POLL_RECORDS_CONFIG = 10_000;

    @RepeatedTest(5)
    public void testTransactionalDefaultMaxPoll() {
        boolean tx = true;
        runTest(tx, DEFAULT_MAX_POLL_RECORDS_CONFIG);
    }

    @Test
    public void testNonTransactionalDefaultMaxPoll() {
        boolean tx = false;
        runTest(tx, DEFAULT_MAX_POLL_RECORDS_CONFIG);
    }

    @Test
    public void testTransactionalLowMaxPoll() {
        boolean tx = true;
        runTest(tx, LOW_MAX_POLL_RECORDS_CONFIG);
    }

    @Test
    public void testNonTransactionalLowMaxPoll() {
        boolean tx = false;
        runTest(tx, LOW_MAX_POLL_RECORDS_CONFIG);
    }

    @Test
    public void testTransactionalHighMaxPoll() {
        boolean tx = true;
        runTest(tx, HIGH_MAX_POLL_RECORDS_CONFIG);
    }

    @Test
    public void testNonTransactionalHighMaxPoll() {
        boolean tx = false;
        runTest(tx, HIGH_MAX_POLL_RECORDS_CONFIG);
    }

    @SneakyThrows
    private void runTest(boolean tx, int maxPoll) {
        AppUnderTest coreApp = new AppUnderTest(tx, ParallelConsumerOptions.builder()
                .ordering(KEY)
                .usingTransactionalProducer(tx)
                .build(),
                maxPoll);

        ensureTopic(coreApp.inputTopic, 1);
        ensureTopic(coreApp.outputTopic, 1);

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();
        int expectedMessageCount = 1000;
        log.info("Producing {} messages before starting application", expectedMessageCount);
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < expectedMessageCount; i++) {
                String key = "key-" + i;
                kafkaProducer.send(new ProducerRecord<>(coreApp.inputTopic, key, "value-" + i));
                expectedKeys.add(key);
            }
        }

        // run parallel-consumer
        log.info("Starting application...");
        coreApp.runPollAndProduce();

        // wait for all pre-produced messages to be processed and produced
        try {
            waitAtMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                log.debug("Processed-count: " + coreApp.processedCount.get());
                log.debug("Produced-count: " + coreApp.producedCount.get());
                List<String> processedAndProducedKeys = new ArrayList<>(coreApp.processedAndProducedKeys); // avoid concurrent-modification in assert
                assertThat(processedAndProducedKeys).contains(expectedKeys.toArray(new String[0]));
            });
        } catch (ConditionTimeoutException e) {
            String failureMessage = "All keys sent to input-topic should be processed and produced";
            log.warn(failureMessage);
            log.debug("Expected keys=" + expectedKeys + "");
            log.debug("Processed and produced keys=" + coreApp.processedAndProducedKeys + "");
            log.debug("Missing keys=" + expectedKeys.removeAll(coreApp.processedAndProducedKeys));
            fail(failureMessage);
        }


        assertThat(coreApp.processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(coreApp.producedCount.get());


        coreApp.close();
    }

    @AllArgsConstructor
    class AppUnderTest extends CoreApp {

        final boolean tx;
        final ParallelConsumerOptions options;
        int MAX_POLL_RECORDS_CONFIG;

        @Override
        Consumer<String, String> getKafkaConsumer() {
            Properties props = kcu.props;
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS_CONFIG);
            return new KafkaConsumer<>(props);
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            return kcu.createNewProducer(tx);
        }

        @Override
        ParallelConsumerOptions getOptions() {
            return options;
        }
    }
}
