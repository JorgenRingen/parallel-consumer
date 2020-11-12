package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * Basic core examples
 */
@Slf4j
public class CoreApp {


    String inputTopic = "input-topic-" + RandomUtils.nextInt();
    String outputTopic = "output-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    ParallelStreamProcessor<String, String> parallelConsumer;

    public List<String> processedAndProducedKeys = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger producedCount = new AtomicInteger(0);

    @SuppressWarnings("UnqualifiedFieldAccess")
    void run() {
        this.parallelConsumer = setupParallelConsumer();

        // tag::example[]
        parallelConsumer.poll(record ->
                log.info("Concurrently processing a record: {}", record)
        );
        // end::example[]
    }

    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupParallelConsumer() {
        // tag::exampleSetup[]
        ParallelConsumerOptions options = getOptions();

        Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // <4>
//        if (!(kafkaConsumer instanceof MockConsumer)) {
//            kafkaConsumer.subscribe(UniLists.of(inputTopic)); // <5>
//        }

        Producer<String, String> kafkaProducer = getKafkaProducer();
        ParallelStreamProcessor<String, String> processor = ParallelStreamProcessor.createEosStreamProcessor(kafkaConsumer, kafkaProducer, options);
        processor.subscribe(UniLists.of(inputTopic));
        return processor;
        // end::exampleSetup[]
    }

    ParallelConsumerOptions getOptions() {
        var options = ParallelConsumerOptions.builder()
                .ordering(KEY) // <1>
//                .maxConcurrency(1000) // <2>
//                .numberOfThreads(1000) // <2>
//                .maxUncommittedMessagesToHandlePerPartition(100) // <3>
                .build();
        return options;
    }

    void close() {
        this.parallelConsumer.close();
    }

    void runPollAndProduce() {
        this.parallelConsumer = setupParallelConsumer();

        // tag::exampleProduce[]
        this.parallelConsumer.pollAndProduce(record -> {
                    var result = processBrokerRecord(record);
                    ProducerRecord<String, String> produceRecord =
                            new ProducerRecord<>(outputTopic, record.key(), result.payload);

                    processedCount.incrementAndGet();
                    return UniLists.of(produceRecord);
                }, consumeProduceResult -> {
                    producedCount.incrementAndGet();
                    processedAndProducedKeys.add(consumeProduceResult.getIn().key());
                    log.info("Message {} saved to broker at offset {}",
                            consumeProduceResult.getOut(),
                            consumeProduceResult.getMeta().offset());
                }
        );
        // end::exampleProduce[]
    }

    private Result processBrokerRecord(ConsumerRecord<String, String> record) {
        return new Result("Some payload from " + record.value());
    }

    @Value
    static class Result {
        String payload;
    }

}
