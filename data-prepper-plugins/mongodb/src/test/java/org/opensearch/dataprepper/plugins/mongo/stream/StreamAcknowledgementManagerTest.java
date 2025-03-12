package org.opensearch.dataprepper.plugins.mongo.stream;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamAcknowledgementManager.GIVE_UP_PARTITION_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamAcknowledgementManager.NEGATIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamAcknowledgementManager.NO_DATA_EXTEND_LEASE_COUNT;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamAcknowledgementManager.POSITIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamAcknowledgementManager.RECORDS_CHECKPOINTED;

@ExtendWith(MockitoExtension.class)
public class StreamAcknowledgementManagerTest {

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private DataStreamPartitionCheckpoint partitionCheckpoint;
    @Mock
    private Duration timeout;
    @Mock
    private AcknowledgementSet acknowledgementSet;
    @Mock
    private Consumer<Void> stopWorkerConsumer;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter positiveAcknowledgementSets;
    @Mock
    private Counter negativeAcknowledgementSets;
    @Mock
    private Counter recordsCheckpointed;
    @Mock
    private Counter noDataExtendLeaseCount;
    @Mock
    private Counter giveupPartitionCount;

    private StreamAcknowledgementManager streamAckManager;



    @BeforeEach
    public void setup() {
        when(pluginMetrics.counter(POSITIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME)).thenReturn(positiveAcknowledgementSets);
        when(pluginMetrics.counter(NEGATIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME)).thenReturn(negativeAcknowledgementSets);
        when(pluginMetrics.counter(RECORDS_CHECKPOINTED)).thenReturn(recordsCheckpointed);
        when(pluginMetrics.counter(NO_DATA_EXTEND_LEASE_COUNT)).thenReturn(noDataExtendLeaseCount);
        when(pluginMetrics.counter(GIVE_UP_PARTITION_COUNT)).thenReturn(giveupPartitionCount);

        streamAckManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint, timeout, 0, 0, pluginMetrics);
    }

    @Test
    public void createAcknowledgementSet_disabled_emptyAckSet() {
        final Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(UUID.randomUUID().toString(), new Random().nextInt());
        assertThat(ackSet.isEmpty(), is(true));
    }

    @Test
    public void createAcknowledgementSet_enabled_ackSetWithAck() {
        lenient().when(timeout.getSeconds()).thenReturn(10_000L);
        streamAckManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint, timeout, 0, 0, pluginMetrics);
        streamAckManager.init(stopWorkerConsumer);
        final String resumeToken = UUID.randomUUID().toString();
        final long recordCount = new Random().nextLong();
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        final Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(resumeToken, recordCount);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getResumeToken(), is(resumeToken));
        assertThat(streamAckManager.getCheckpoints().peek().getRecordCount(), is(recordCount));
        final ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager).create(argumentCaptor.capture(), eq(timeout));
        final Consumer<Boolean> consumer = argumentCaptor.getValue();
        consumer.accept(true);
        final ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        final CheckpointStatus ackCheckpointStatus = ackStatus.get(resumeToken);
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(true));
        await()
           .atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                verify(partitionCheckpoint, times(2)).checkpoint(resumeToken, recordCount));
        assertThat(streamAckManager.getCheckpoints().peek(), is(nullValue()));
        verify(positiveAcknowledgementSets).increment();
        verifyNoInteractions(negativeAcknowledgementSets);
        verify(recordsCheckpointed, atLeastOnce()).increment();
    }

    @Test
    public void createAcknowledgementSet_enabled_multipleAckSetWithAck() {
        lenient().when(timeout.getSeconds()).thenReturn(10_000L);
        streamAckManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint, timeout, 0, 0, pluginMetrics);
        streamAckManager.init(stopWorkerConsumer);
        final String resumeToken1 = UUID.randomUUID().toString();
        final long recordCount1 = new Random().nextLong();
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(resumeToken1, recordCount1);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getResumeToken(), is(resumeToken1));
        assertThat(streamAckManager.getCheckpoints().peek().getRecordCount(), is(recordCount1));

        final String resumeToken2 = UUID.randomUUID().toString();
        final long recordCount2 = new Random().nextLong();
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        ackSet = streamAckManager.createAcknowledgementSet(resumeToken2, recordCount2);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getResumeToken(), is(resumeToken1));
        assertThat(streamAckManager.getCheckpoints().peek().getRecordCount(), is(recordCount1));
        ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager, times(2)).create(argumentCaptor.capture(), eq(timeout));
        List<Consumer<Boolean>> consumers = argumentCaptor.getAllValues();
        consumers.get(0).accept(true);
        consumers.get(1).accept(true);
        ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        CheckpointStatus ackCheckpointStatus = ackStatus.get(resumeToken2);
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(true));
        await()
            .atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                verify(partitionCheckpoint, times(2)).checkpoint(resumeToken2, recordCount2));
        assertThat(streamAckManager.getCheckpoints().peek(), is(nullValue()));

        verify(positiveAcknowledgementSets, atLeastOnce()).increment();
        verifyNoInteractions(negativeAcknowledgementSets);
        verify(recordsCheckpointed, atLeastOnce()).increment();
    }

    @Test
    public void createAcknowledgementSet_enabled_multipleAckSetWithAckFailure() {
        streamAckManager.init(stopWorkerConsumer);
        final String resumeToken1 = UUID.randomUUID().toString();
        final long recordCount1 = new Random().nextLong();
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(resumeToken1, recordCount1);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getResumeToken(), is(resumeToken1));
        assertThat(streamAckManager.getCheckpoints().peek().getRecordCount(), is(recordCount1));

        final String resumeToken2 = UUID.randomUUID().toString();
        final long recordCount2 = new Random().nextLong();
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        ackSet = streamAckManager.createAcknowledgementSet(resumeToken2, recordCount2);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getResumeToken(), is(resumeToken1));
        assertThat(streamAckManager.getCheckpoints().peek().getRecordCount(), is(recordCount1));
        ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager, times(2)).create(argumentCaptor.capture(), eq(timeout));
        List<Consumer<Boolean>> consumers = argumentCaptor.getAllValues();
        consumers.get(0).accept(false);
        consumers.get(1).accept(true);
        ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        CheckpointStatus ackCheckpointStatus = ackStatus.get(resumeToken2);
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(true));
        await()
            .atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                verify(partitionCheckpoint).giveUpPartition());
        assertThat(streamAckManager.getCheckpoints().peek().getResumeToken(), is(resumeToken1));
        assertThat(streamAckManager.getCheckpoints().peek().getRecordCount(), is(recordCount1));
        verify(positiveAcknowledgementSets).increment();
        verify(negativeAcknowledgementSets).increment();
        verify(giveupPartitionCount).increment();
        verify(stopWorkerConsumer).accept(null);
    }

    @Test
    public void createAcknowledgementSet_enabled_ackSetWithNoAck() {
        streamAckManager.init(stopWorkerConsumer);
        final String resumeToken = UUID.randomUUID().toString();
        final long recordCount = new Random().nextLong();
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        final Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(resumeToken, recordCount);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getResumeToken(), is(resumeToken));
        assertThat(streamAckManager.getCheckpoints().peek().getRecordCount(), is(recordCount));
        final ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager).create(argumentCaptor.capture(), eq(timeout));
        final Consumer<Boolean> consumer = argumentCaptor.getValue();
        consumer.accept(false);
        final ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        final CheckpointStatus ackCheckpointStatus = ackStatus.get(resumeToken);
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(false));
        await()
            .atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                verify(stopWorkerConsumer).accept(null));
        verify(negativeAcknowledgementSets).increment();
    }
}
