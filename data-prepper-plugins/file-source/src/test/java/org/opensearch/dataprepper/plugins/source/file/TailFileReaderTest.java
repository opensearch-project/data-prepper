/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TailFileReaderTest {

    @TempDir
    Path tempDir;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private EventFactory eventFactory;

    @Mock
    private FileSystemOperations fileOps;

    @Mock
    private FileTailMetrics metrics;

    @Mock
    private RotationDetector rotationDetector;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private CheckpointEntry checkpointEntry;
    private FileIdentity fileIdentity;
    private AtomicBoolean onCompleteCalled;

    @BeforeEach
    void setUp() {
        checkpointEntry = new CheckpointEntry();
        onCompleteCalled = new AtomicBoolean(false);
    }

    private TailFileReaderContext createContext(final StartPosition startPosition) {
        return new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofSeconds(30),
                Duration.ofSeconds(30), startPosition, false,
                Duration.ofSeconds(30), 1000,
                Duration.ofSeconds(5), 3, null);
    }

    private TailFileReaderContext createContext(final int readBufferSize, final int maxLineLength,
                                                final boolean includeMetadata, final StartPosition startPosition) {
        return new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                readBufferSize, maxLineLength, 5000, Duration.ofSeconds(30),
                Duration.ofSeconds(30), startPosition, includeMetadata,
                Duration.ofSeconds(30), 1000,
                Duration.ofSeconds(5), 3, null);
    }

    private TailFileReader createReader(final Path path) {
        return createReader(path, 4096, 1048576, false, StartPosition.BEGINNING);
    }

    private TailFileReader createReader(final Path path, final int readBufferSize,
                                         final int maxLineLength, final boolean includeMetadata) {
        return createReader(path, readBufferSize, maxLineLength, includeMetadata, StartPosition.BEGINNING);
    }

    private TailFileReader createReader(final Path path, final int readBufferSize,
                                         final int maxLineLength, final boolean includeMetadata,
                                         final StartPosition startPosition) {
        fileIdentity = mock(FileIdentity.class);
        final TailFileReaderContext context = createContext(readBufferSize, maxLineLength, includeMetadata, startPosition);
        return new TailFileReader(path, fileIdentity, checkpointEntry, context,
                () -> onCompleteCalled.set(true));
    }

    private void stubEventFactory() {
        when(eventFactory.eventBuilder(EventBuilder.class)).thenAnswer(invocation -> {
            EventBuilder mockBuilder = mock(EventBuilder.class);
            Event mockEvent = mock(Event.class);
            when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
            when(mockBuilder.withData(any(Map.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockEvent);
            return mockBuilder;
        });
    }

    private void lenientStubEventFactory() {
        lenient().when(eventFactory.eventBuilder(EventBuilder.class)).thenAnswer(invocation -> {
            EventBuilder mockBuilder = mock(EventBuilder.class);
            Event mockEvent = mock(Event.class);
            lenient().when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
            lenient().when(mockBuilder.withData(any(Map.class))).thenReturn(mockBuilder);
            lenient().when(mockBuilder.build()).thenReturn(mockEvent);
            return mockBuilder;
        });
    }

    private void stubReadMetrics() {
        Counter linesRead = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        when(metrics.getLinesRead()).thenReturn(linesRead);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));
    }

    @Test
    void run_reads_lines_from_file() throws Exception {
        Path testFile = tempDir.resolve("test.log");
        Files.writeString(testFile, "line1\nline2\nline3\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(buffer, times(3)).write(any(Record.class), eq(5000));
        verify(metrics.getLinesRead(), times(3)).increment();
        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void run_tracks_read_offset() throws Exception {
        Path testFile = tempDir.resolve("offset.log");
        final String content = "hello\nworld\n";
        Files.writeString(testFile, content);
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();

        final TailFileReader reader = createReader(testFile);
        reader.run();

        assertThat(reader.getReadOffset(), equalTo((long) content.getBytes(StandardCharsets.UTF_8).length));
        assertThat(checkpointEntry.getReadOffset(), equalTo((long) content.getBytes(StandardCharsets.UTF_8).length));
    }

    @Test
    void run_handles_partial_line_without_trailing_newline() throws Exception {
        Path testFile = tempDir.resolve("partial.log");
        Files.writeString(testFile, "complete\nno-newline-at-end");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(buffer, times(2)).write(any(Record.class), eq(5000));
    }

    @Test
    void run_truncates_line_exceeding_max_line_length() throws Exception {
        Path testFile = tempDir.resolve("longline.log");
        final String longContent = "A".repeat(200);
        Files.writeString(testFile, longContent);
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        Counter linesTruncated = mock(Counter.class);
        when(metrics.getLinesTruncated()).thenReturn(linesTruncated);
        stubEventFactory();

        final TailFileReader reader = createReader(testFile, 4096, 50, false);
        reader.run();

        verify(linesTruncated).increment();
    }

    @Test
    void run_truncates_complete_line_exceeding_max_line_length() throws Exception {
        Path testFile = tempDir.resolve("longcomplete.log");
        Files.writeString(testFile, "B".repeat(200) + "\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        Counter linesTruncated = mock(Counter.class);
        when(metrics.getLinesTruncated()).thenReturn(linesTruncated);
        stubEventFactory();

        final TailFileReader reader = createReader(testFile, 4096, 50, false);
        reader.run();

        verify(linesTruncated).increment();
    }

    @Test
    void run_retries_on_buffer_back_pressure() throws Exception {
        Path testFile = tempDir.resolve("backpressure.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        Counter writeTimeouts = mock(Counter.class);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);
        stubEventFactory();

        doThrow(new TimeoutException("buffer full"))
                .doNothing()
                .when(buffer).write(any(Record.class), anyInt());

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(writeTimeouts).increment();
        verify(buffer, times(2)).write(any(Record.class), eq(5000));
    }

    @Test
    void run_handles_deleted_file_via_rotation_detector() throws Exception {
        Path testFile = tempDir.resolve("deleted.log");
        Files.writeString(testFile, "data\n");

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.DELETED);

        final TailFileReader reader = createReader(testFile);
        reader.run();

        assertThat(reader.getLastRotationType(), equalTo(RotationType.DELETED));
        verify(buffer, never()).write(any(), anyInt());
        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void run_handles_no_such_file_exception_during_read() throws Exception {
        Path testFile = tempDir.resolve("gone.log");
        Files.writeString(testFile, "data\n");

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        when(fileOps.openReadChannel(testFile)).thenThrow(new NoSuchFileException(testFile.toString()));
        Counter filesClosed = mock(Counter.class);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);

        final TailFileReader reader = createReader(testFile);
        reader.run();

        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void run_handles_copytruncate_rotation() throws Exception {
        Path testFile = tempDir.resolve("truncated.log");
        Files.writeString(testFile, "new-data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        Counter filesRotated = mock(Counter.class);
        when(metrics.getFilesRotated()).thenReturn(filesRotated);
        stubReadMetrics();
        stubEventFactory();

        fileIdentity = mock(FileIdentity.class);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(new RotationResult(RotationType.COPYTRUNCATE, fileIdentity));

        checkpointEntry.setReadOffset(500);
        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(filesRotated).increment();
        assertThat(reader.getLastRotationType(), equalTo(RotationType.COPYTRUNCATE));
    }

    @Test
    void run_handles_create_rename_rotation() throws Exception {
        Path testFile = tempDir.resolve("renamed.log");
        Files.writeString(testFile, "tail-data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        Counter filesRotated = mock(Counter.class);
        when(metrics.getFilesRotated()).thenReturn(filesRotated);
        stubReadMetrics();
        stubEventFactory();

        FileIdentity newIdentity = mock(FileIdentity.class);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(new RotationResult(RotationType.CREATE_RENAME, newIdentity));

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(filesRotated).increment();
        assertThat(reader.getLastRotationType(), equalTo(RotationType.CREATE_RENAME));
    }

    @Test
    void run_resumes_from_checkpoint_offset() throws Exception {
        Path testFile = tempDir.resolve("resume.log");
        Files.writeString(testFile, "line1\nline2\nline3\n");
        long offsetAfterFirstLine = "line1\n".getBytes(StandardCharsets.UTF_8).length;
        checkpointEntry.setReadOffset(offsetAfterFirstLine);

        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(buffer, times(2)).write(any(Record.class), eq(5000));
    }

    @Test
    void run_increments_read_errors_on_io_exception() throws Exception {
        Path testFile = tempDir.resolve("ioerror.log");
        Files.writeString(testFile, "data\n");

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        when(fileOps.openReadChannel(testFile)).thenThrow(new IOException("disk error"));
        Counter readErrors = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        when(metrics.getReadErrors()).thenReturn(readErrors);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(readErrors).increment();
        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void run_includes_file_metadata_when_enabled() throws Exception {
        Path testFile = tempDir.resolve("meta.log");
        Files.writeString(testFile, "data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();

        ArgumentCaptor<Map> dataCaptor = ArgumentCaptor.forClass(Map.class);
        EventBuilder mockBuilder = mock(EventBuilder.class);
        Event mockEvent = mock(Event.class);
        when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(mockBuilder);
        when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
        when(mockBuilder.withData(dataCaptor.capture())).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockEvent);

        final TailFileReader reader = createReader(testFile, 4096, 1048576, true);
        reader.run();

        Map<String, Object> capturedData = dataCaptor.getValue();
        assertThat(capturedData.containsKey("file_path"), equalTo(true));
        assertThat(capturedData.containsKey("file_identity"), equalTo(true));
    }

    @Test
    void getFileIdentity_returns_identity_passed_in_constructor() throws Exception {
        Path testFile = tempDir.resolve("identity.log");
        Files.writeString(testFile, "");

        final TailFileReader reader = createReader(testFile);
        assertThat(reader.getFileIdentity(), equalTo(fileIdentity));
    }

    @Test
    void getLastRotationType_defaults_to_no_rotation() throws Exception {
        Path testFile = tempDir.resolve("default.log");
        Files.writeString(testFile, "");

        final TailFileReader reader = createReader(testFile);
        assertThat(reader.getLastRotationType(), equalTo(RotationType.NO_ROTATION));
    }

    @Test
    void start_position_end_seeks_to_end_of_file_for_new_files() throws Exception {
        Path testFile = tempDir.resolve("startend.log");
        Files.writeString(testFile, "existing-line1\nexisting-line2\n");
        long fileSize = Files.size(testFile);
        when(fileOps.size(testFile)).thenReturn(fileSize);

        final TailFileReader reader = createReader(testFile, 4096, 1048576, false, StartPosition.END);

        assertThat(reader.getReadOffset(), equalTo(fileSize));
    }

    @Test
    void start_position_beginning_starts_from_offset_zero_for_new_files() throws Exception {
        Path testFile = tempDir.resolve("startbegin.log");
        Files.writeString(testFile, "existing-line1\nexisting-line2\n");

        final TailFileReader reader = createReader(testFile, 4096, 1048576, false, StartPosition.BEGINNING);

        assertThat(reader.getReadOffset(), equalTo(0L));
    }

    @Test
    void start_position_end_does_not_seek_when_checkpoint_exists() throws Exception {
        Path testFile = tempDir.resolve("checkpoint-end.log");
        Files.writeString(testFile, "existing-line1\nexisting-line2\n");
        checkpointEntry.setReadOffset(10);

        final TailFileReader reader = createReader(testFile, 4096, 1048576, false, StartPosition.END);

        assertThat(reader.getReadOffset(), equalTo(10L));
    }

    @Test
    void run_increments_events_emitted_counter() throws Exception {
        Path testFile = tempDir.resolve("emitted.log");
        Files.writeString(testFile, "line1\nline2\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        Counter eventsEmitted = mock(Counter.class);
        when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(eventsEmitted, times(2)).increment();
    }

    private TailFileReaderContext createContextWithCodec(final InputCodec codec) {
        return new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofSeconds(30),
                Duration.ofSeconds(30), StartPosition.BEGINNING, false,
                Duration.ofSeconds(30), 1000,
                Duration.ofSeconds(5), 3, codec);
    }

    private TailFileReaderContext createContextWithAcknowledgements(final int batchSize, final Duration batchTimeout, final int maxRetries) {
        return new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, true, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofSeconds(30),
                Duration.ofSeconds(30), StartPosition.BEGINNING, false,
                Duration.ofSeconds(30), batchSize,
                batchTimeout, maxRetries, null);
    }

    private TailFileReader createReaderWithContext(final Path path, final TailFileReaderContext context) {
        fileIdentity = mock(FileIdentity.class);
        return new TailFileReader(path, fileIdentity, checkpointEntry, context,
                () -> onCompleteCalled.set(true));
    }

    @Test
    void start_position_end_falls_back_to_zero_on_io_exception() throws Exception {
        Path testFile = tempDir.resolve("startend-error.log");
        Files.writeString(testFile, "existing data\n");
        when(fileOps.size(testFile)).thenThrow(new IOException("disk error"));

        final TailFileReader reader = createReader(testFile, 4096, 1048576, false, StartPosition.END);

        assertThat(reader.getReadOffset(), equalTo(0L));
    }

    @Test
    void run_increments_read_errors_on_runtime_exception() throws Exception {
        Path testFile = tempDir.resolve("runtime-err.log");
        Files.writeString(testFile, "data\n");

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenThrow(new RuntimeException("unexpected"));
        Counter readErrors = mock(Counter.class);
        when(metrics.getReadErrors()).thenReturn(readErrors);

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(readErrors).increment();
        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void run_drains_file_on_create_rename_and_handles_no_such_file() throws Exception {
        Path testFile = tempDir.resolve("drain-nosuch.log");
        Files.writeString(testFile, "data\n");

        FileIdentity newIdentity = mock(FileIdentity.class);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(new RotationResult(RotationType.CREATE_RENAME, newIdentity));
        when(fileOps.openReadChannel(testFile)).thenThrow(new NoSuchFileException(testFile.toString()));
        Counter filesRotated = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        when(metrics.getFilesRotated()).thenReturn(filesRotated);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(filesRotated).increment();
        assertThat(reader.getLastRotationType(), equalTo(RotationType.CREATE_RENAME));
    }

    @Test
    void run_drains_file_on_create_rename_and_handles_io_exception() throws Exception {
        Path testFile = tempDir.resolve("drain-ioerr.log");
        Files.writeString(testFile, "data\n");

        FileIdentity newIdentity = mock(FileIdentity.class);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(new RotationResult(RotationType.CREATE_RENAME, newIdentity));
        when(fileOps.openReadChannel(testFile)).thenThrow(new IOException("disk error"));
        Counter filesRotated = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter readErrors = mock(Counter.class);
        when(metrics.getFilesRotated()).thenReturn(filesRotated);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getReadErrors()).thenReturn(readErrors);

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(readErrors).increment();
    }

    @Test
    void run_drain_timeout_logs_data_loss_when_unread_data() throws Exception {
        Path testFile = tempDir.resolve("drain-timeout.log");
        Files.writeString(testFile, "A".repeat(10000) + "\n");

        FileIdentity newIdentity = mock(FileIdentity.class);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(new RotationResult(RotationType.CREATE_RENAME, newIdentity));

        FileChannel mockChannel = mock(FileChannel.class);
        when(fileOps.openReadChannel(testFile)).thenReturn(mockChannel);
        when(mockChannel.position(anyLong())).thenReturn(mockChannel);
        lenient().when(mockChannel.read(any(ByteBuffer.class))).thenAnswer(inv -> {
            Thread.sleep(5);
            ByteBuffer buf = inv.getArgument(0);
            byte[] data = "A".repeat(buf.remaining()).getBytes();
            buf.put(data, 0, Math.min(data.length, buf.remaining()));
            return buf.position();
        });
        when(mockChannel.size()).thenReturn(100000L);

        Counter filesRotated = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        Counter dataLossEvents = mock(Counter.class);
        when(metrics.getFilesRotated()).thenReturn(filesRotated);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getFilesOpened()).thenReturn(mock(Counter.class));
        lenient().when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getDataLossEvents()).thenReturn(dataLossEvents);

        TailFileReaderContext context = new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofSeconds(30),
                Duration.ofMillis(1), StartPosition.BEGINNING, false,
                Duration.ofSeconds(30), 1000,
                Duration.ofSeconds(5), 3, null);

        lenientStubEventFactory();
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);

        fileIdentity = mock(FileIdentity.class);
        final TailFileReader reader = new TailFileReader(testFile, fileIdentity, checkpointEntry, context,
                () -> onCompleteCalled.set(true));
        reader.run();

        verify(dataLossEvents).increment();
    }

    @Test
    void run_max_read_time_reached_breaks_without_data_loss() throws Exception {
        Path testFile = tempDir.resolve("maxread.log");
        Files.writeString(testFile, "A".repeat(10000) + "\n");

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        FileChannel mockChannel = mock(FileChannel.class);
        when(fileOps.openReadChannel(testFile)).thenReturn(mockChannel);
        when(mockChannel.position(anyLong())).thenReturn(mockChannel);
        when(mockChannel.read(any(ByteBuffer.class))).thenAnswer(inv -> {
            Thread.sleep(5);
            ByteBuffer buf = inv.getArgument(0);
            byte[] data = "A".repeat(buf.remaining()).getBytes();
            buf.put(data, 0, Math.min(data.length, buf.remaining()));
            return buf.position();
        });

        Counter filesClosed = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getFilesOpened()).thenReturn(mock(Counter.class));
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        stubEventFactory();
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);

        TailFileReaderContext context = new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofMillis(1),
                Duration.ofSeconds(30), StartPosition.BEGINNING, false,
                Duration.ofSeconds(30), 1000,
                Duration.ofSeconds(5), 3, null);

        fileIdentity = mock(FileIdentity.class);
        final TailFileReader reader = new TailFileReader(testFile, fileIdentity, checkpointEntry, context,
                () -> onCompleteCalled.set(true));
        reader.run();

        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void run_with_codec_parses_bytes_and_emits_records() throws Exception {
        Path testFile = tempDir.resolve("codec.log");
        Files.writeString(testFile, "codec-data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getLinesRead()).thenReturn(linesRead);
        when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        InputCodec mockCodec = mock(InputCodec.class);
        doAnswer(inv -> {
            Consumer<Record<Event>> consumer = inv.getArgument(1);
            Event mockEvent = mock(Event.class);
            consumer.accept(new Record<>(mockEvent));
            return null;
        }).when(mockCodec).parse(any(), any());

        TailFileReaderContext context = createContextWithCodec(mockCodec);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(buffer, atLeastOnce()).write(any(Record.class), eq(5000));
        verify(linesRead, atLeastOnce()).increment();
    }

    @Test
    void run_with_codec_handles_parse_io_exception() throws Exception {
        Path testFile = tempDir.resolve("codec-error.log");
        Files.writeString(testFile, "bad-data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter readErrors = mock(Counter.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getReadErrors()).thenReturn(readErrors);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        InputCodec mockCodec = mock(InputCodec.class);
        doThrow(new IOException("parse error")).when(mockCodec).parse(any(), any());

        TailFileReaderContext context = createContextWithCodec(mockCodec);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(readErrors).increment();
    }

    @Test
    void run_codec_record_retries_on_backpressure_and_records_timer() throws Exception {
        Path testFile = tempDir.resolve("codec-backpressure.log");
        Files.writeString(testFile, "data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Counter writeTimeouts = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getLinesRead()).thenReturn(linesRead);
        when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);
        when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        doThrow(new TimeoutException("buffer full"))
                .doNothing()
                .when(buffer).write(any(Record.class), anyInt());

        InputCodec mockCodec = mock(InputCodec.class);
        doAnswer(inv -> {
            Consumer<Record<Event>> consumer = inv.getArgument(1);
            Event mockEvent = mock(Event.class);
            consumer.accept(new Record<>(mockEvent));
            return null;
        }).when(mockCodec).parse(any(), any());

        TailFileReaderContext context = createContextWithCodec(mockCodec);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(writeTimeouts).increment();
        verify(backpressureTimer).record(anyLong(), any(TimeUnit.class));
    }

    @Test
    void run_with_acknowledgements_creates_ack_set_and_completes_on_batch_full() throws Exception {
        Path testFile = tempDir.resolve("ack.log");
        Files.writeString(testFile, "line1\nline2\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(ackSet);

        TailFileReaderContext context = createContextWithAcknowledgements(1, Duration.ofSeconds(5), 3);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(acknowledgementSetManager, atLeastOnce()).create(any(), any(Duration.class));
        verify(ackSet, atLeastOnce()).add(any(Event.class));
        verify(ackSet, atLeastOnce()).complete();
    }

    @Test
    void run_with_acknowledgements_batch_timeout_triggers_complete() throws Exception {
        Path testFile = tempDir.resolve("ack-timeout.log");
        Files.writeString(testFile, "line1\nline2\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(ackSet);

        TailFileReaderContext context = createContextWithAcknowledgements(10000, Duration.ofMillis(0), 3);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(ackSet, atLeastOnce()).complete();
    }

    @SuppressWarnings("unchecked")
    @Test
    void handleAcknowledgement_positive_resets_retry_and_updates_checkpoint() throws Exception {
        Path testFile = tempDir.resolve("ack-pos.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);
        ArgumentCaptor<Consumer> handlerCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(acknowledgementSetManager.create(handlerCaptor.capture(), any(Duration.class))).thenReturn(ackSet);

        TailFileReaderContext context = createContextWithAcknowledgements(1000, Duration.ofSeconds(5), 3);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        Consumer<Boolean> handler = handlerCaptor.getValue();
        handler.accept(true);

        assertThat(checkpointEntry.getCommittedOffset(), equalTo(reader.getReadOffset()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void handleAcknowledgement_negative_retries_and_eventually_advances() throws Exception {
        Path testFile = tempDir.resolve("ack-neg.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        Counter ackFailures = mock(Counter.class);
        when(metrics.getAcknowledgmentFailures()).thenReturn(ackFailures);

        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);
        ArgumentCaptor<Consumer> handlerCaptor = ArgumentCaptor.forClass(Consumer.class);
        when(acknowledgementSetManager.create(handlerCaptor.capture(), any(Duration.class))).thenReturn(ackSet);

        TailFileReaderContext context = createContextWithAcknowledgements(1000, Duration.ofSeconds(5), 2);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        Consumer<Boolean> handler = handlerCaptor.getValue();
        handler.accept(false);
        verify(ackFailures, times(1)).increment();

        handler.accept(false);
        verify(ackFailures, times(2)).increment();

        handler.accept(false);
        verify(ackFailures, times(3)).increment();
        assertThat(checkpointEntry.getCommittedOffset(), equalTo(reader.getReadOffset()));
    }

    @Test
    void run_with_codec_acknowledgements_creates_ack_set() throws Exception {
        Path testFile = tempDir.resolve("codec-ack.log");
        Files.writeString(testFile, "data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getLinesRead()).thenReturn(linesRead);
        when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(ackSet);

        InputCodec mockCodec = mock(InputCodec.class);
        doAnswer(inv -> {
            Consumer<Record<Event>> consumer = inv.getArgument(1);
            Event mockEvent = mock(Event.class);
            consumer.accept(new Record<>(mockEvent));
            return null;
        }).when(mockCodec).parse(any(), any());

        TailFileReaderContext context = new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, true, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofSeconds(30),
                Duration.ofSeconds(30), StartPosition.BEGINNING, false,
                Duration.ofSeconds(30), 1, Duration.ofSeconds(5), 3, mockCodec);

        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(ackSet, atLeastOnce()).add(any(Event.class));
        verify(ackSet, atLeastOnce()).complete();
    }

    @Test
    void run_update_file_lag_handles_io_exception() throws Exception {
        Path testFile = tempDir.resolve("lag-error.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenThrow(new IOException("disk error"));

        final TailFileReader reader = createReader(testFile);
        reader.run();

        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void getLastActivityMillis_returns_initial_value() throws Exception {
        Path testFile = tempDir.resolve("activity.log");
        Files.writeString(testFile, "");
        long before = System.currentTimeMillis();
        final TailFileReader reader = createReader(testFile);
        long after = System.currentTimeMillis();

        assertThat(reader.getLastActivityMillis() >= before, equalTo(true));
        assertThat(reader.getLastActivityMillis() <= after, equalTo(true));
    }

    @Test
    void run_emitLine_backpressure_records_timer_after_recovery() throws Exception {
        Path testFile = tempDir.resolve("bp-timer.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        Timer backpressureTimer = mock(Timer.class);
        when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        Counter writeTimeouts = mock(Counter.class);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);

        doThrow(new TimeoutException("buffer full"))
                .doNothing()
                .when(buffer).write(any(Record.class), anyInt());

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(backpressureTimer).record(anyLong(), any(TimeUnit.class));
    }

    @Test
    void run_codec_backpressure_interrupt_stops_reader() throws Exception {
        Path testFile = tempDir.resolve("codec-bp-interrupt.log");
        Files.writeString(testFile, "data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter writeTimeouts = mock(Counter.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        doThrow(new TimeoutException("buffer full"))
                .when(buffer).write(any(Record.class), anyInt());

        InputCodec mockCodec = mock(InputCodec.class);
        doAnswer(inv -> {
            Consumer<Record<Event>> consumer = inv.getArgument(1);
            Event mockEvent = mock(Event.class);
            consumer.accept(new Record<>(mockEvent));
            return null;
        }).when(mockCodec).parse(any(), any());

        TailFileReaderContext context = createContextWithCodec(mockCodec);

        Thread readerThread = new Thread(() -> {
            final TailFileReader reader = createReaderWithContext(testFile, context);
            reader.run();
        });
        readerThread.start();
        Thread.sleep(300);
        readerThread.interrupt();
        readerThread.join(5000);

        assertThat(readerThread.isAlive(), equalTo(false));
    }

    @Test
    void run_emitLine_backpressure_interrupt_stops_reader() throws Exception {
        Path testFile = tempDir.resolve("bp-interrupt.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Counter writeTimeouts = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));
        stubEventFactory();

        doThrow(new TimeoutException("buffer full"))
                .when(buffer).write(any(Record.class), anyInt());

        Thread readerThread = new Thread(() -> {
            final TailFileReader reader = createReader(testFile);
            reader.run();
        });
        readerThread.start();
        Thread.sleep(200);
        readerThread.interrupt();
        readerThread.join(5000);

        assertThat(readerThread.isAlive(), equalTo(false));
    }

    @Test
    void run_drain_timeout_handles_io_exception_on_channel_size() throws Exception {
        Path testFile = tempDir.resolve("drain-size-err.log");
        Files.writeString(testFile, "A".repeat(10000) + "\n");

        FileIdentity newIdentity = mock(FileIdentity.class);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(new RotationResult(RotationType.CREATE_RENAME, newIdentity));

        FileChannel mockChannel = mock(FileChannel.class);
        when(fileOps.openReadChannel(testFile)).thenReturn(mockChannel);
        when(mockChannel.position(anyLong())).thenReturn(mockChannel);
        when(mockChannel.read(any(ByteBuffer.class))).thenAnswer(inv -> {
            Thread.sleep(5);
            ByteBuffer buf = inv.getArgument(0);
            byte[] data = "A".repeat(buf.remaining()).getBytes();
            buf.put(data, 0, Math.min(data.length, buf.remaining()));
            return buf.position();
        });
        when(mockChannel.size()).thenThrow(new IOException("channel closed"));

        Counter filesRotated = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        Counter dataLossEvents = mock(Counter.class);
        when(metrics.getFilesRotated()).thenReturn(filesRotated);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getFilesOpened()).thenReturn(mock(Counter.class));
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        lenient().when(metrics.getDataLossEvents()).thenReturn(dataLossEvents);

        TailFileReaderContext context = new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofSeconds(30),
                Duration.ofMillis(1), StartPosition.BEGINNING, false,
                Duration.ofSeconds(30), 1000,
                Duration.ofSeconds(5), 3, null);

        stubEventFactory();
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);

        fileIdentity = mock(FileIdentity.class);
        final TailFileReader reader = new TailFileReader(testFile, fileIdentity, checkpointEntry, context,
                () -> onCompleteCalled.set(true));
        reader.run();
    }

    @Test
    void run_updateFileLagBytes_handles_io_exception_on_file_size() throws Exception {
        Path testFile = tempDir.resolve("lag-err.log");
        Files.writeString(testFile, "line1\n");

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(fileOps.size(testFile)).thenThrow(new IOException("disk error"));

        stubEventFactory();
        stubReadMetrics();

        final TailFileReader reader = createReader(testFile);
        reader.run();

        assertThat(onCompleteCalled.get(), equalTo(true));
    }

    @Test
    void run_with_acks_enabled_batch_not_full_does_not_complete_ack_set() throws Exception {
        Path testFile = tempDir.resolve("ack-notfull.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(ackSet);

        TailFileReaderContext context = createContextWithAcknowledgements(10000, Duration.ofHours(1), 3);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(ackSet, atLeastOnce()).add(any(Event.class));
        verify(ackSet, atLeastOnce()).complete();
    }

    @Test
    void run_without_acks_does_not_create_ack_set() throws Exception {
        Path testFile = tempDir.resolve("no-ack.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();
        when(fileOps.size(testFile)).thenReturn(Files.size(testFile));

        final TailFileReader reader = createReader(testFile);
        reader.run();

        verify(acknowledgementSetManager, never()).create(any(), any(Duration.class));
    }

    @Test
    void run_with_acks_enabled_zero_batch_count_does_not_trigger_batch_timeout() throws Exception {
        Path testFile = tempDir.resolve("ack-zero-batch.log");
        Files.writeString(testFile, "");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        lenient().when(metrics.getBytesRead()).thenReturn(bytesRead);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        TailFileReaderContext context = createContextWithAcknowledgements(1000, Duration.ofMillis(0), 3);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(acknowledgementSetManager, never()).create(any(), any(Duration.class));
    }

    @Test
    void run_with_codec_acks_disabled_does_not_create_ack_set() throws Exception {
        Path testFile = tempDir.resolve("codec-no-ack.log");
        Files.writeString(testFile, "data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getLinesRead()).thenReturn(linesRead);
        when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        InputCodec mockCodec = mock(InputCodec.class);
        doAnswer(inv -> {
            Consumer<Record<Event>> consumer = inv.getArgument(1);
            Event mockEvent = mock(Event.class);
            consumer.accept(new Record<>(mockEvent));
            return null;
        }).when(mockCodec).parse(any(), any());

        TailFileReaderContext context = createContextWithCodec(mockCodec);
        final TailFileReader reader = createReaderWithContext(testFile, context);
        reader.run();

        verify(acknowledgementSetManager, never()).create(any(), any(Duration.class));
    }

    @Test
    void run_emitLine_thread_interrupted_during_backpressure_exits() throws Exception {
        Path testFile = tempDir.resolve("bp-interrupt-line.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter writeTimeouts = mock(Counter.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));
        stubEventFactory();

        doThrow(new TimeoutException("buffer full"))
                .when(buffer).write(any(Record.class), anyInt());

        Thread readerThread = new Thread(() -> {
            final TailFileReader reader = createReader(testFile);
            reader.run();
        });
        readerThread.start();
        Thread.sleep(300);
        readerThread.interrupt();
        readerThread.join(5000);

        assertThat(readerThread.isAlive(), equalTo(false));
    }

    @Test
    void run_codec_thread_interrupted_during_backpressure_exits() throws Exception {
        Path testFile = tempDir.resolve("codec-bp-int.log");
        Files.writeString(testFile, "data\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter writeTimeouts = mock(Counter.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        doThrow(new TimeoutException("buffer full"))
                .when(buffer).write(any(Record.class), anyInt());

        InputCodec mockCodec = mock(InputCodec.class);
        doAnswer(inv -> {
            Consumer<Record<Event>> consumer = inv.getArgument(1);
            Event mockEvent = mock(Event.class);
            consumer.accept(new Record<>(mockEvent));
            return null;
        }).when(mockCodec).parse(any(), any());

        TailFileReaderContext context = createContextWithCodec(mockCodec);

        Thread readerThread = new Thread(() -> {
            final TailFileReader reader = createReaderWithContext(testFile, context);
            reader.run();
        });
        readerThread.start();
        Thread.sleep(300);
        readerThread.interrupt();
        readerThread.join(5000);

        assertThat(readerThread.isAlive(), equalTo(false));
    }

    @Test
    void run_multi_byte_character_split_across_reads_triggers_decoder_carryover() throws Exception {
        Path testFile = tempDir.resolve("multibyte.log");
        String multiByteContent = "\u00E9\u00E9\u00E9\u00E9\n";
        Files.writeString(testFile, multiByteContent);
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        stubEventFactory();

        final TailFileReader reader = createReader(testFile, 3, 1048576, false);
        reader.run();

        verify(buffer, atLeastOnce()).write(any(Record.class), eq(5000));
    }

    @Test
    void run_skippingToNewline_skips_remainder_after_max_line_truncation() throws Exception {
        Path testFile = tempDir.resolve("skip-newline.log");
        String longLine = "A".repeat(60) + "\nsecond\n";
        Files.writeString(testFile, longLine);
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);
        stubReadMetrics();
        Counter linesTruncated = mock(Counter.class);
        when(metrics.getLinesTruncated()).thenReturn(linesTruncated);
        stubEventFactory();

        final TailFileReader reader = createReader(testFile, 20, 10, false);
        reader.run();

        verify(linesTruncated, atLeastOnce()).increment();
        verify(buffer, atLeastOnce()).write(any(Record.class), eq(5000));
    }

    @Test
    void run_backpressure_retry_timeout_exceeded_logs_data_loss() throws Exception {
        Path testFile = tempDir.resolve("bp-timeout.log");
        Files.writeString(testFile, "line1\n");
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        Counter bytesRead = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter writeTimeouts = mock(Counter.class);
        Counter dataLossEvents = mock(Counter.class);
        when(metrics.getBytesRead()).thenReturn(bytesRead);
        when(metrics.getFilesOpened()).thenReturn(filesOpened);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getWriteTimeouts()).thenReturn(writeTimeouts);
        when(metrics.getDataLossEvents()).thenReturn(dataLossEvents);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));
        stubEventFactory();

        doThrow(new TimeoutException("buffer full"))
                .when(buffer).write(any(Record.class), anyInt());

        TailFileReaderContext context = new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofMillis(1),
                Duration.ofSeconds(30), StartPosition.BEGINNING, false,
                Duration.ofMillis(1), 1000,
                Duration.ofSeconds(5), 3, null);

        fileIdentity = mock(FileIdentity.class);
        final TailFileReader reader = new TailFileReader(testFile, fileIdentity, checkpointEntry, context,
                () -> onCompleteCalled.set(true));
        reader.run();

        verify(dataLossEvents).increment();
        assertThat(onCompleteCalled.get(), equalTo(true));
    }
}
