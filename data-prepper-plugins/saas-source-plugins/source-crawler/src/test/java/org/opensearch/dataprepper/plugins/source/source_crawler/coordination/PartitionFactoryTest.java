package org.opensearch.dataprepper.plugins.source.source_crawler.coordination;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.AtlassianLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.AtlassianWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PartitionFactoryTest {

    private final String sourceIdentifier = UUID.randomUUID().toString();
    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    @ParameterizedTest
    @MethodSource("provideLeaderPartitionTestInputs")
    void testCreateLeaderPartitionParameterized(String jsonState, Class<? extends LeaderProgressState> expectedClass, Instant expectedTime, LeaderProgressState updatedState) {
        String sourceId = sourceIdentifier + "|" + LeaderPartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(jsonState);

        PartitionFactory factory = new PartitionFactory();
        EnhancedSourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);

        assertThat(sourcePartition, notNullValue());
        assertTrue(sourcePartition instanceof LeaderPartition);

        LeaderPartition leaderPartition = (LeaderPartition) sourcePartition;
        assertThat(leaderPartition.getPartitionType(), equalTo(LeaderPartition.PARTITION_TYPE));
        assertThat(leaderPartition.getPartitionKey(), equalTo(LeaderPartition.DEFAULT_PARTITION_KEY));

        Optional<LeaderProgressState> progressState = leaderPartition.getProgressState();
        assertTrue(progressState.isPresent());
        assertTrue(expectedClass.isInstance(progressState.get()));
        assertEquals(expectedTime, progressState.get().getLastPollTime());

        // Update and verify
        leaderPartition.setLeaderProgressState(updatedState);
        assertEquals(updatedState.getLastPollTime(), leaderPartition.getProgressState().get().getLastPollTime());
    }

    private static Stream<Arguments> provideLeaderPartitionTestInputs() {
        return Stream.of(
                Arguments.of(
                        "{\n" +
                                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.AtlassianLeaderProgressState\",\n" +
                                "  \"last_poll_time\": \"2024-10-30T20:17:46.332Z\"\n" +
                                "}",
                        AtlassianLeaderProgressState.class,
                        Instant.parse("2024-10-30T20:17:46.332Z"),
                        new AtlassianLeaderProgressState(Instant.ofEpochMilli(12345L))
                ),
                Arguments.of(
                        "{\n" +
                                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeLeaderProgressState\",\n" +
                                "  \"last_poll_time\": \"2024-10-30T08:17:46.332Z\",\n" +
                                "  \"remaining_days\": 3\n" +
                                "}",
                        CrowdStrikeLeaderProgressState.class,
                        Instant.parse("2024-10-30T08:17:46.332Z"),
                        new CrowdStrikeLeaderProgressState(Instant.ofEpochMilli(12345L), 5)
                )
        );
    }


    @ParameterizedTest
    @MethodSource("provideWorkerPartitionInputs")
    void testCreateWorkerPartition(String sourceIdSuffix, String partitionProgressStateJson, Class<? extends SaasWorkerProgressState> expectedClass) {
        final String saasProject = "project-1";
        final String projectCategory = "category-1";
        String sourceId = sourceIdentifier + "|" + sourceIdSuffix;
        String partitionKey = saasProject + "|" + projectCategory + UUID.randomUUID();

        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(partitionProgressStateJson);

        PartitionFactory factory = new PartitionFactory();
        EnhancedSourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);

        assertThat(sourcePartition, notNullValue());
        assertTrue(sourcePartition instanceof SaasSourcePartition);

        SaasSourcePartition saasSourcePartition = (SaasSourcePartition) sourcePartition;
        assertEquals(SaasSourcePartition.PARTITION_TYPE, saasSourcePartition.getPartitionType());
        assertEquals(partitionKey, saasSourcePartition.getPartitionKey());
        assertTrue(saasSourcePartition.getProgressState().isPresent());
        assertTrue(expectedClass.isInstance(saasSourcePartition.getProgressState().get()));
    }

    private static Stream<Arguments> provideWorkerPartitionInputs() {
        String atlassianJson = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.AtlassianWorkerProgressState\",\n" +
                "  \"keyAttributes\": {\"project\": \"project-1\"},\n" +
                "  \"totalItems\": 0,\n" +
                "  \"loadedItems\": 20,\n" +
                "  \"exportStartTime\": 1729391235717,\n" +
                "  \"itemIds\": [\"GTMS-25\", \"GTMS-24\"]\n" +
                "}";

        String crowdStrikeJson = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState\",\n" +
                "  \"startTime\": \"2024-10-30T08:17:46.332Z\",\n" +
                "  \"endTime\": \"2024-10-31T08:17:46.332Z\",\n" +
                "  \"marker\": 5\n" +
                "}";

        return Stream.of(
                Arguments.of(SaasSourcePartition.PARTITION_TYPE, atlassianJson, AtlassianWorkerProgressState.class),
                Arguments.of(SaasSourcePartition.PARTITION_TYPE, crowdStrikeJson, CrowdStrikeWorkerProgressState.class)
        );
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
        assertThrows(RuntimeException.class, () -> factory.apply(sourcePartitionStoreItem));
    }


}
