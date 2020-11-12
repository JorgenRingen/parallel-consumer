package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractOffsetCommitter<K, V> implements OffsetCommitter<K, V> {

    protected final Consumer<K, V> consumer;
    protected final WorkManager<K, V> wm;

    /**
     * Get offsets from {@link WorkManager} that are ready to commit
     */
    @Override
    public void commit() {
        log.trace("Loop: Find completed work to commit offsets");
        // todo shouldn't be removed until commit succeeds (there's no harm in committing the same offset twice)
        preAcquireWork();
        try {
            Map<TopicPartition, OffsetAndMetadata> offsetsToSend = wm.findCompletedEligibleOffsetsAndRemove();
            if (offsetsToSend.isEmpty()) {
                log.trace("No offsets ready");
            } else {
                log.debug("Committing offsets for {} partition(s): {}", offsetsToSend.size(), offsetsToSend);
                ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();

                commitOffsets(offsetsToSend, groupMetadata);
            }
        } finally {
            postCommit();
        }
    }

    protected abstract void postCommit();

    protected abstract void preAcquireWork();

    protected void onOffsetCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        wm.onOffsetCommitSuccess(offsetsToSend);
    }

    protected abstract void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata);

}
