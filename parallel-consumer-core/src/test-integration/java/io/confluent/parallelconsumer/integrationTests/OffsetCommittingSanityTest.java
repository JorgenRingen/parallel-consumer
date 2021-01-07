package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Test that reproduces issue 62: https://github.com/confluentinc/parallel-consumer/issues/62
 */
@Slf4j
public class OffsetCommittingSanityTest extends BrokerIntegrationTest<String, String> {

    @Test
    void shouldNotSkipAnyMessagesOnRestart() throws Exception {
        String topic = setupTopic("foo");
        List<Long> producedOffsets = new ArrayList<>();
        List<Long> consumedOffsets = new ArrayList<>();

        var kafkaProducer = kcu.createNewProducer(false);

        // offset 0
        producedOffsets.add(kafkaProducer.send(new ProducerRecord<>(topic, "key-0", "value-0")).get().offset());

        var pc = createParallelConsumer(topic);
        pc.poll(consumerRecord -> consumedOffsets.add(consumerRecord.offset()));
        waitAtMost(ofSeconds(5)).untilAsserted(() -> assertThat(consumedOffsets).isEqualTo(producedOffsets));
        pc.closeDrainFirst();

        // offset 1
        producedOffsets.add(kafkaProducer.send(new ProducerRecord<>(topic, "key-1", "value-1")).get().offset());

        pc = createParallelConsumer(topic); // create new consumer with same cg (simulating restart)
        pc.poll(consumerRecord -> consumedOffsets.add(consumerRecord.offset()));
        waitAtMost(ofSeconds(5)).untilAsserted(() -> assertThat(consumedOffsets).isEqualTo(producedOffsets));
        pc.closeDrainFirst();

        // sanity
        assertThat(producedOffsets).containsExactly(0L, 1L);
        assertThat(consumedOffsets).containsExactly(0L, 1L);
    }

    private ParallelEoSStreamProcessor<String, String> createParallelConsumer(String topicName) {
        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.builder()
                .consumer(kcu.createNewConsumer(false))
                .build()
        );
        pc.subscribe(List.of(topicName));
        return pc;
    }

}
