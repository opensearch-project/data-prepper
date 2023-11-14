package org.opensearch.dataprepper.plugins.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.plugins.kafka.consumer.TopicEmptinessMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class KafkaAdminAccessorTest {
    private static final String CONSUMER_GROUP_ID = UUID.randomUUID().toString();
    private static final String TOPIC_NAME = UUID.randomUUID().toString();
    private static final Random RANDOM = new Random();

    @Mock
    private AdminClient kafkaAdminClient;
    @Mock
    private OffsetAndMetadata offsetAndMetadata;
    @Mock
    private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult;
    @Mock
    private ListOffsetsResult listOffsetsResult;
    @Mock
    private KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> committedOffsetsFuture;
    @Mock
    private KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> endOffsetsFuture;
    @Mock
    private ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfo;
    private TopicEmptinessMetadata topicEmptinessMetadata;

    private KafkaAdminAccessor kafkaAdminAccessor;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public KafkaAdminAccessor createObjectUnderTest() {
        topicEmptinessMetadata = new TopicEmptinessMetadata();
        return new KafkaAdminAccessor(kafkaAdminClient, topicEmptinessMetadata, List.of(CONSUMER_GROUP_ID));
    }

    @Test
    public void areTopicsEmpty_OnePartition_IsEmpty() throws ExecutionException, InterruptedException {
        final Long offset = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(1);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset);

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(true));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_OnePartition_PartitionNeverHadData() throws ExecutionException, InterruptedException {
        final Long offset = 0L;
        final List<TopicPartition> topicPartitions = buildTopicPartitions(1);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset);
        when(offsetAndMetadata.offset()).thenReturn(offset - 1);

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(true));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_OnePartition_IsNotEmpty() throws ExecutionException, InterruptedException {
        final Long offset = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(1);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset);
        when(offsetAndMetadata.offset()).thenReturn(offset - 1);

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(false));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_OnePartition_NoCommittedPartition() throws ExecutionException, InterruptedException {
        final Long offset = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(1);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset);
        final HashMap<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(topicPartitions.get(0), null);
        when(committedOffsetsFuture.get()).thenReturn(committedOffsets);

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(false));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_MultiplePartitions_AllEmpty() throws ExecutionException, InterruptedException {
        final Long offset1 = RANDOM.nextLong();
        final Long offset2 = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(2);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset1);
        when(listOffsetsResultInfo.offset()).thenReturn(offset1).thenReturn(offset2);
        when(offsetAndMetadata.offset()).thenReturn(offset1).thenReturn(offset2);

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(true));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_MultiplePartitions_OneNotEmpty() throws ExecutionException, InterruptedException {
        final Long offset1 = RANDOM.nextLong();
        final Long offset2 = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(2);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset1);
        when(listOffsetsResultInfo.offset()).thenReturn(offset1).thenReturn(offset2);
        when(offsetAndMetadata.offset()).thenReturn(offset1).thenReturn(offset2 - 1);

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(false));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_NonCheckerThread_ShortCircuits() {
        kafkaAdminAccessor = createObjectUnderTest();

        topicEmptinessMetadata.setTopicEmptyCheckingOwnerThreadId(Thread.currentThread().getId() - 1);
        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(true));

        verifyNoInteractions(kafkaAdminClient);
    }

    @Test
    public void areTopicsEmpty_CheckedWithinDelay_ShortCircuits() {
        kafkaAdminAccessor = createObjectUnderTest();

        topicEmptinessMetadata.setLastIsEmptyCheckTime(System.currentTimeMillis());
        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(true));

        verifyNoInteractions(kafkaAdminClient);
    }

    @Test
    public void areTopicsEmpty_ExceptionGettingCommittedOffsets_ReturnsFalse() throws ExecutionException, InterruptedException {
        final Long offset = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(1);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset);
        when(committedOffsetsFuture.get()).thenThrow(new InterruptedException());

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(false));

        verify(kafkaAdminClient).listConsumerGroupOffsets(CONSUMER_GROUP_ID);
        verifyNoMoreInteractions(kafkaAdminClient);
    }

    @Test
    public void areTopicsEmpty_ExceptionGettingEndOffsets_ReturnsFalse() throws ExecutionException, InterruptedException {
        final Long offset = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(1);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset);
        when(endOffsetsFuture.get()).thenThrow(new InterruptedException());

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(false));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_MissingEndOffset_ReturnsFalse() throws ExecutionException, InterruptedException {
        final Long offset = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions = buildTopicPartitions(1);

        kafkaAdminAccessor = createObjectUnderTest();
        mockAdminClientCalls(topicPartitions, offset);
        when(endOffsetsFuture.get()).thenReturn(Collections.emptyMap());

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(false));

        verifyAdminClientCalls(topicPartitions);
    }

    @Test
    public void areTopicsEmpty_MultipleConsumerGroups() throws ExecutionException, InterruptedException {
        final Long offset = RANDOM.nextLong();
        final List<TopicPartition> topicPartitions1 = List.of(new TopicPartition(TOPIC_NAME, 0));
        final List<TopicPartition> topicPartitions2 = List.of(new TopicPartition(UUID.randomUUID().toString(), 0));

        topicEmptinessMetadata = new TopicEmptinessMetadata();
        final String consumerGroupId2 = UUID.randomUUID().toString();
        kafkaAdminAccessor = new KafkaAdminAccessor(kafkaAdminClient, topicEmptinessMetadata,
                List.of(CONSUMER_GROUP_ID, consumerGroupId2));

        mockAdminClientCalls(topicPartitions1, offset);
        when(kafkaAdminClient.listConsumerGroupOffsets(CONSUMER_GROUP_ID)).thenReturn(listConsumerGroupOffsetsResult);
        when(kafkaAdminClient.listConsumerGroupOffsets(consumerGroupId2)).thenReturn(listConsumerGroupOffsetsResult);
        when(committedOffsetsFuture.get())
                .thenReturn(getTopicPartitionToMap(topicPartitions1, offsetAndMetadata))
                .thenReturn(getTopicPartitionToMap(topicPartitions2, offsetAndMetadata));
        final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = getTopicPartitionToMap(topicPartitions1, listOffsetsResultInfo);
        endOffsets.putAll(getTopicPartitionToMap(topicPartitions2, listOffsetsResultInfo));
        when(endOffsetsFuture.get()).thenReturn(endOffsets);

        assertThat(kafkaAdminAccessor.areTopicsEmpty(), equalTo(true));

        final ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaAdminClient, times(2)).listConsumerGroupOffsets(argumentCaptor.capture());
        assertThat(argumentCaptor.getAllValues().size(), equalTo(2));
        assertThat(argumentCaptor.getAllValues().get(0), equalTo(CONSUMER_GROUP_ID));
        assertThat(argumentCaptor.getAllValues().get(1), equalTo(consumerGroupId2));

        verify(kafkaAdminClient).listOffsets(argThat(r -> {
            final List<TopicPartition> combinedTopicPartitions = Stream.concat(topicPartitions1.stream(), topicPartitions2.stream())
                    .collect(Collectors.toList());
            assertAll("ListOffsets request fields match",
                    () -> assertThat("Request map size matches", r.size(), equalTo(combinedTopicPartitions.size())),
                    () -> assertThat("TopicPartitions in keyset",
                            combinedTopicPartitions.stream().allMatch(topicPartition -> r.containsKey(topicPartition))),
                    () -> assertThat("OffsetSpec matches",
                            combinedTopicPartitions.stream().allMatch(topicPartition -> r.get(topicPartition) instanceof OffsetSpec.LatestSpec))
            );
            return true;
        }));
    }

    private void mockAdminClientCalls(final List<TopicPartition> topicPartitions, final long offset) throws ExecutionException, InterruptedException {
        when(kafkaAdminClient.listConsumerGroupOffsets(CONSUMER_GROUP_ID)).thenReturn(listConsumerGroupOffsetsResult);
        when(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata()).thenReturn(committedOffsetsFuture);
        when(committedOffsetsFuture.get()).thenReturn(getTopicPartitionToMap(topicPartitions, offsetAndMetadata));
        when(kafkaAdminClient.listOffsets(any())).thenReturn(listOffsetsResult);
        when(listOffsetsResult.all()).thenReturn(endOffsetsFuture);
        when(endOffsetsFuture.get()).thenReturn(getTopicPartitionToMap(topicPartitions, listOffsetsResultInfo));
        when(listOffsetsResultInfo.offset()).thenReturn(offset);
        when(offsetAndMetadata.offset()).thenReturn(offset);
    }

    private void verifyAdminClientCalls(final List<TopicPartition> topicPartitions) {
        verify(kafkaAdminClient).listConsumerGroupOffsets(CONSUMER_GROUP_ID);
        verify(kafkaAdminClient).listOffsets(argThat(r -> {
            assertAll("ListOffsets request fields match",
                    () -> assertThat("Request map size matches", r.size(), equalTo(topicPartitions.size())),
                    () -> assertThat("TopicPartitions in keyset",
                            topicPartitions.stream().allMatch(topicPartition -> r.containsKey(topicPartition))),
                    () -> assertThat("OffsetSpec matches",
                            topicPartitions.stream().allMatch(topicPartition -> r.get(topicPartition) instanceof OffsetSpec.LatestSpec))
            );
            return true;
        }));
    }

    private List<TopicPartition> buildTopicPartitions(final int partitionCount) {
        return IntStream.range(0, partitionCount)
                .mapToObj(i -> new TopicPartition(TOPIC_NAME, i))
                .collect(Collectors.toList());
    }

    private <T> Map<TopicPartition, T> getTopicPartitionToMap(final List<TopicPartition> topicPartitions, final T value) {
        return topicPartitions.stream()
                .collect(Collectors.toMap(i -> i, i -> value));
    }
}
