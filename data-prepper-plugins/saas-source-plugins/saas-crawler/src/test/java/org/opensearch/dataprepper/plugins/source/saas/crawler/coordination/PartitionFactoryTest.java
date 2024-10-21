package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.LeaderProgressState;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PartitionFactoryTest {

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;
    private final String sourceIdentifier = UUID.randomUUID().toString();


    @Test
    void testCreateLeaderPartition() {
        String sourceId = sourceIdentifier + "|" + LeaderPartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);

        String state = "{\"last_poll_time\":1729391235717}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);


        PartitionFactory factory = new PartitionFactory();
        EnhancedSourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        LeaderPartition leaderParition = (LeaderPartition) sourcePartition;
        assertThat(leaderParition.getPartitionType(), equalTo(LeaderPartition.PARTITION_TYPE));
        assertThat(leaderParition.getPartitionKey(), equalTo(LeaderPartition.DEFAULT_PARTITION_KEY));

        Optional<LeaderProgressState> progressState = leaderParition.getProgressState();
        assertThat(progressState.isPresent(), equalTo(true));
        assertThat(progressState.get().getLastPollTime(), equalTo(1729391235717L));

        //Update leader progress state and then verify
        LeaderProgressState updatedState = new LeaderProgressState(12345L);
        leaderParition.setLeaderProgressState(updatedState);
        assertThat(progressState.get().getLastPollTime(), equalTo(12345L));
    }

    @Test
    void testCreatWorkerPartition() {
        final String saasProject = "project-1";
        final String projectCategory = "category-1";
        String sourceId = sourceIdentifier + "|" + SaasSourcePartition.PARTITION_TYPE;
        String partitionKey = saasProject + "|" + projectCategory + UUID.randomUUID();
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);

        String state = "{\"keyAttributes\":{\"project\":\"project-1\"},\"totalItems\":0,\"loadedItems\":20,\"exportStartTime\":1729391235717,\"itemIds\":[\"GTMS-25\",\"GTMS-24\"]}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);

        PartitionFactory factory = new PartitionFactory();
        EnhancedSourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        SaasSourcePartition saasSourcePartition = (SaasSourcePartition) sourcePartition;
        assertThat(saasSourcePartition.getPartitionType(), equalTo(SaasSourcePartition.PARTITION_TYPE));
        assertThat(saasSourcePartition.getPartitionKey(), equalTo(partitionKey));
        assertThat(saasSourcePartition.getProgressState().isPresent(), equalTo(true));
    }

    @Test
    void testCreatWorkerPartitionWithNullState() {

        String sourceId = sourceIdentifier + "|" + SaasSourcePartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);


        PartitionFactory factory = new PartitionFactory();
        EnhancedSourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        SaasSourcePartition saasSourcePartition = (SaasSourcePartition) sourcePartition;
        assertThat(saasSourcePartition.getPartitionType(), equalTo(SaasSourcePartition.PARTITION_TYPE));
        assertThat(saasSourcePartition.getProgressState().isPresent(), equalTo(false));
    }

    @Test
    void testUnknownPartition() {

        String sourceId = sourceIdentifier + "|" + "UNKNOWN";
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);

        PartitionFactory factory = new PartitionFactory();
        assertThrows(RuntimeException.class, () ->factory.apply(sourcePartitionStoreItem));
    }


}
