package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.plugins.kafka.consumer.TopicEmptinessMetadata.IS_EMPTY_CHECK_INTERVAL_MS;

public class TopicEmptinessMetadataTest {
    @Mock
    private TopicPartition topicPartition;
    @Mock
    private TopicPartition topicPartition2;

    private TopicEmptinessMetadata topicEmptinessMetadata;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        this.topicEmptinessMetadata = new TopicEmptinessMetadata();
    }

    @Test
    void updateTopicEmptinessStatus_AddEntry() {
        topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, false);
        assertThat(topicEmptinessMetadata.getTopicPartitionToIsEmpty().containsKey(topicPartition), equalTo(true));
        assertThat(topicEmptinessMetadata.getTopicPartitionToIsEmpty().get(topicPartition), equalTo(false));
    }

    @Test
    void updateTopicEmptinessStatus_UpdateEntry() {
        topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, false);
        assertThat(topicEmptinessMetadata.getTopicPartitionToIsEmpty().containsKey(topicPartition), equalTo(true));
        assertThat(topicEmptinessMetadata.getTopicPartitionToIsEmpty().get(topicPartition), equalTo(false));

        topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, true);
        assertThat(topicEmptinessMetadata.getTopicPartitionToIsEmpty().containsKey(topicPartition), equalTo(true));
        assertThat(topicEmptinessMetadata.getTopicPartitionToIsEmpty().get(topicPartition), equalTo(true));
    }

    @Test
    void isTopicEmpty_NoItems() {
        assertThat(topicEmptinessMetadata.isTopicEmpty(), equalTo(true));
    }

    @Test
    void isTopicEmpty_OnePartition_IsNotEmpty() {
        topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, false);
        assertThat(topicEmptinessMetadata.isTopicEmpty(), equalTo(false));
    }

    @Test
    void isTopicEmpty_OnePartition_IsEmpty() {
        topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, true);
        assertThat(topicEmptinessMetadata.isTopicEmpty(), equalTo(true));
    }

    @Test
    void isTopicEmpty_MultiplePartitions_OneNotEmpty() {
        topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, true);
        topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition2, false);
        assertThat(topicEmptinessMetadata.isTopicEmpty(), equalTo(false));
    }

    @Test
    void isCheckDurationExceeded_NoPreviousChecks() {
        assertThat(topicEmptinessMetadata.isWithinCheckInterval(System.currentTimeMillis()), equalTo(false));
    }

    @Test
    void isCheckDurationExceeded_CurrentTimeBeforeLastCheck() {
        final long time = System.currentTimeMillis();
        topicEmptinessMetadata.setLastIsEmptyCheckTime(time);
        assertThat(topicEmptinessMetadata.isWithinCheckInterval(time - 1), equalTo(true));
    }

    @Test
    void isCheckDurationExceeded_CurrentTimeAfterLastCheck_BeforeInterval() {
        final long time = System.currentTimeMillis();
        topicEmptinessMetadata.setLastIsEmptyCheckTime(time);
        assertThat(topicEmptinessMetadata.isWithinCheckInterval((time + IS_EMPTY_CHECK_INTERVAL_MS) - 1), equalTo(true));
    }

    @Test
    void isCheckDurationExceeded_CurrentTimeAfterLastCheck_AtInterval() {
        final long time = System.currentTimeMillis();
        topicEmptinessMetadata.setLastIsEmptyCheckTime(time);
        assertThat(topicEmptinessMetadata.isWithinCheckInterval(time + IS_EMPTY_CHECK_INTERVAL_MS), equalTo(false));
    }

    @Test
    void isCheckDurationExceeded_CurrentTimeAfterLastCheck_AfterInterval() {
        final long time = System.currentTimeMillis();
        topicEmptinessMetadata.setLastIsEmptyCheckTime(time);
        assertThat(topicEmptinessMetadata.isWithinCheckInterval(time + IS_EMPTY_CHECK_INTERVAL_MS + 1), equalTo(false));
    }
}
