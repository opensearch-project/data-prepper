package org.opensearch.dataprepper.plugins.source.neptune.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamCheckpoint;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamPosition;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private StreamAcknowledgementManager streamAckManager;

    @BeforeEach
    public void setup() {
        streamAckManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint, timeout, 0, 0);
    }

    @Test
    public void createAcknowledgementSet_disabled_emptyAckSet() {
        final StreamCheckpoint checkpoint = createCheckpoint(new Random().nextLong(), new Random().nextLong(), new Random().nextLong());
        final Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(checkpoint);
        assertThat(ackSet.isEmpty(), is(true));
    }

    private StreamCheckpoint createCheckpoint(final long commNum, final long opNum, final long numOfRecords) {
        return new StreamCheckpoint(new StreamPosition(commNum, opNum), numOfRecords);
    }

    @Test
    public void createAcknowledgementSet_enabled_ackSetWithAck() {
        lenient().when(timeout.getSeconds()).thenReturn(10_000L);
        streamAckManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint, timeout, 0, 0);
        streamAckManager.init(stopWorkerConsumer);
        final long recordCount = new Random().nextLong();
        final StreamCheckpoint checkpoint = createCheckpoint(1L, 1L, recordCount);

        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        final Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(checkpoint);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getCheckpoint(), is(checkpoint));

        final ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager).create(argumentCaptor.capture(), eq(timeout));
        final Consumer<Boolean> consumer = argumentCaptor.getValue();
        consumer.accept(true);

        final ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        final CheckpointStatus ackCheckpointStatus = ackStatus.get("1-1");
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(true));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(partitionCheckpoint).checkpoint(checkpoint));
        assertThat(streamAckManager.getCheckpoints().peek(), is(nullValue()));
    }

    @Test
    public void createAcknowledgementSet_enabled_multipleAckSetWithAck() {
        lenient().when(timeout.getSeconds()).thenReturn(10_000L);
        streamAckManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint, timeout, 0, 0);
        streamAckManager.init(stopWorkerConsumer);

        final long recordCount1 = new Random().nextLong();
        final StreamCheckpoint checkpoint1 = createCheckpoint(1L, 1L, recordCount1);
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(checkpoint1);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getCheckpoint(), is(checkpoint1));

        final long recordCount2 = new Random().nextLong();
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        final StreamCheckpoint checkpoint2 = createCheckpoint(1L, 2L, recordCount2);
        ackSet = streamAckManager.createAcknowledgementSet(checkpoint2);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getCheckpoint().getRecordCount(), is(recordCount1));

        ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager, times(2)).create(argumentCaptor.capture(), eq(timeout));
        List<Consumer<Boolean>> consumers = argumentCaptor.getAllValues();
        consumers.get(0).accept(true);
        consumers.get(1).accept(true);
        ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        CheckpointStatus ackCheckpointStatus = ackStatus.get("1-2");
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(true));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(partitionCheckpoint).checkpoint(checkpoint2));
        assertThat(streamAckManager.getCheckpoints().peek(), is(nullValue()));
    }

    @Test
    public void createAcknowledgementSet_enabled_multipleAckSetWithAckFailure() {
        streamAckManager.init(stopWorkerConsumer);

        final long recordCount1 = new Random().nextLong();
        final StreamCheckpoint checkpoint1 = createCheckpoint(1L, 1L, recordCount1);
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(checkpoint1);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getCheckpoint().getRecordCount(), is(recordCount1));

        final long recordCount2 = new Random().nextLong();
        final StreamCheckpoint checkpoint2 = createCheckpoint(1L, 2L, recordCount2);
        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        ackSet = streamAckManager.createAcknowledgementSet(checkpoint2);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getCheckpoint().getRecordCount(), is(recordCount1));
        ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager, times(2)).create(argumentCaptor.capture(), eq(timeout));
        List<Consumer<Boolean>> consumers = argumentCaptor.getAllValues();
        consumers.get(0).accept(false);
        consumers.get(1).accept(true);
        ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        CheckpointStatus ackCheckpointStatus = ackStatus.get("1-2");
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(true));
        await()
                .atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        verify(partitionCheckpoint).giveUpPartition());
        assertThat(streamAckManager.getCheckpoints().peek().getCheckpoint().getRecordCount(), is(recordCount1));
        verify(stopWorkerConsumer).accept(null);
    }

    @Test
    public void createAcknowledgementSet_enabled_ackSetWithNoAck() {
        streamAckManager.init(stopWorkerConsumer);
        final long recordCount = new Random().nextLong();
        final StreamCheckpoint checkpoint = createCheckpoint(1L, 1L, recordCount);

        when(acknowledgementSetManager.create(any(Consumer.class), eq(timeout))).thenReturn(acknowledgementSet);
        final Optional<AcknowledgementSet> ackSet = streamAckManager.createAcknowledgementSet(checkpoint);
        assertThat(ackSet.isEmpty(), is(false));
        assertThat(ackSet.get(), is(acknowledgementSet));
        assertThat(streamAckManager.getCheckpoints().peek().getCheckpoint().getRecordCount(), is(recordCount));
        final ArgumentCaptor<Consumer<Boolean>> argumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager).create(argumentCaptor.capture(), eq(timeout));
        final Consumer<Boolean> consumer = argumentCaptor.getValue();
        consumer.accept(false);
        final ConcurrentHashMap<String, CheckpointStatus> ackStatus = streamAckManager.getAcknowledgementStatus();
        final CheckpointStatus ackCheckpointStatus = ackStatus.get("1-1");
        assertThat(ackCheckpointStatus.isPositiveAcknowledgement(), is(false));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> verify(stopWorkerConsumer).accept(null));
    }
}
