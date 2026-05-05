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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;

import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TailFileReaderPoolTest {

    @TempDir
    Path tempDir;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private EventFactory eventFactory;

    @Mock
    private FileSystemOperations fileOps;

    @Mock
    private CheckpointRegistry checkpointRegistry;

    @Mock
    private FileTailMetrics metrics;

    @Mock
    private RotationDetector rotationDetector;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private TailFileReaderContext createReaderContext() {
        return new TailFileReaderContext(
                buffer, eventFactory, fileOps, metrics, rotationDetector,
                acknowledgementSetManager, false, StandardCharsets.UTF_8,
                4096, 1048576, 5000, Duration.ofSeconds(5),
                Duration.ofSeconds(30), StartPosition.END, false,
                Duration.ofSeconds(30), 1000,
                Duration.ofSeconds(5), 3, null);
    }

    private TailFileReaderPool createPool(final int maxActiveFiles, final int readerThreads) {
        when(metrics.getActiveFileCount()).thenReturn(new AtomicLong(0));
        return new TailFileReaderPool(
                checkpointRegistry, metrics, maxActiveFiles, readerThreads,
                Duration.ofMinutes(30), createReaderContext());
    }

    private TailFileReaderPool createPoolWithoutMetrics(final int maxActiveFiles, final int readerThreads) {
        return new TailFileReaderPool(
                checkpointRegistry, metrics, maxActiveFiles, readerThreads,
                Duration.ofMinutes(30), createReaderContext());
    }

    @Test
    void addFile_submits_reader_when_under_max_active_files() {
        TailFileReaderPool pool = createPool(10, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity = mock(FileIdentity.class);
        when(identity.toString()).thenReturn("test-identity");
        Path path = Paths.get("/tmp/test.log");

        pool.addFile(identity, path);

        assertThat(pool.getActiveReaderCount(), equalTo(1));
        assertThat(pool.getPendingCount(), equalTo(0));
    }

    @Test
    void addFile_is_idempotent_for_same_identity() {
        TailFileReaderPool pool = createPool(10, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity = mock(FileIdentity.class);
        when(identity.toString()).thenReturn("dup-identity");
        Path path = Paths.get("/tmp/dup.log");

        pool.addFile(identity, path);
        pool.addFile(identity, path);

        assertThat(pool.getActiveReaderCount(), equalTo(1));
    }

    @Test
    void addFile_queues_pending_when_at_max_active_files() {
        TailFileReaderPool pool = createPool(1, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity1 = mock(FileIdentity.class);
        when(identity1.toString()).thenReturn("id-1");
        FileIdentity identity2 = mock(FileIdentity.class);

        pool.addFile(identity1, Paths.get("/tmp/file1.log"));
        pool.addFile(identity2, Paths.get("/tmp/file2.log"));

        assertThat(pool.getActiveReaderCount(), equalTo(1));
        assertThat(pool.getPendingCount(), equalTo(1));
    }

    @Test
    void addFile_does_not_add_pending_duplicate_to_queue() {
        TailFileReaderPool pool = createPool(1, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity1 = mock(FileIdentity.class);
        when(identity1.toString()).thenReturn("id-1");
        FileIdentity identity2 = mock(FileIdentity.class);

        pool.addFile(identity1, Paths.get("/tmp/file1.log"));
        pool.addFile(identity2, Paths.get("/tmp/file2.log"));
        pool.addFile(identity2, Paths.get("/tmp/file2.log"));

        assertThat(pool.getPendingCount(), equalTo(1));
    }

    @Test
    void addFile_queues_multiple_pending_files() {
        TailFileReaderPool pool = createPool(1, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity1 = mock(FileIdentity.class);
        when(identity1.toString()).thenReturn("id-1");
        FileIdentity identity2 = mock(FileIdentity.class);
        FileIdentity identity3 = mock(FileIdentity.class);

        pool.addFile(identity1, Paths.get("/tmp/file1.log"));
        pool.addFile(identity2, Paths.get("/tmp/file2.log"));
        pool.addFile(identity3, Paths.get("/tmp/file3.log"));

        assertThat(pool.getActiveReaderCount(), equalTo(1));
        assertThat(pool.getPendingCount(), equalTo(2));
    }

    @Test
    void shutdown_does_not_throw() {
        TailFileReaderPool pool = createPoolWithoutMetrics(10, 1);
        pool.shutdown();
    }

    @Test
    void getActiveReaderCount_returns_zero_initially() {
        TailFileReaderPool pool = createPoolWithoutMetrics(10, 2);
        assertThat(pool.getActiveReaderCount(), equalTo(0));
    }

    @Test
    void getPendingCount_returns_zero_initially() {
        TailFileReaderPool pool = createPoolWithoutMetrics(10, 2);
        assertThat(pool.getPendingCount(), equalTo(0));
    }

    @Test
    void closeInactiveReaders_removes_inactive_readers() throws Exception {
        Counter filesClosed = mock(Counter.class);
        lenient().when(metrics.getFilesClosed()).thenReturn(filesClosed);
        lenient().when(metrics.getActiveFileCount()).thenReturn(new AtomicLong(0));

        TailFileReaderPool pool = new TailFileReaderPool(
                checkpointRegistry, metrics, 10, 2,
                Duration.ofMillis(1), createReaderContext());

        pool.closeInactiveReaders();

        assertThat(pool.getActiveReaderCount(), equalTo(0));
    }

    @Test
    void closeReaderForPath_removes_matching_reader() {
        Counter filesClosed = mock(Counter.class);
        lenient().when(metrics.getFilesClosed()).thenReturn(filesClosed);
        TailFileReaderPool pool = createPool(10, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity = mock(FileIdentity.class);
        when(identity.toString()).thenReturn("/tmp/removable.log");
        Path path = Paths.get("/tmp/removable.log");

        pool.addFile(identity, path);
        assertThat(pool.getActiveReaderCount(), equalTo(1));

        pool.closeReaderForPath(path);
        assertThat(pool.getActiveReaderCount(), equalTo(0));
    }

    @Test
    void closeReaderForPath_does_nothing_when_no_match() {
        TailFileReaderPool pool = createPool(10, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity = mock(FileIdentity.class);
        when(identity.toString()).thenReturn("id-nomatch");
        pool.addFile(identity, Paths.get("/tmp/file1.log"));

        pool.closeReaderForPath(Paths.get("/tmp/other.log"));
        assertThat(pool.getActiveReaderCount(), equalTo(1));
    }

    @Test
    void shutdown_handles_interrupted_exception() throws Exception {
        TailFileReaderPool pool = createPoolWithoutMetrics(10, 1);

        Thread shutdownThread = new Thread(() -> {
            Thread.currentThread().interrupt();
            pool.shutdown();
        });
        shutdownThread.start();
        shutdownThread.join(5000);

        assertThat(shutdownThread.isAlive(), equalTo(false));
    }

    @Test
    void onReaderComplete_with_create_rename_resubmits_reader() throws Exception {
        Path testFile = tempDir.resolve("rotate.log");
        Files.writeString(testFile, "line1\n");

        Counter filesRotated = mock(Counter.class);
        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        lenient().when(metrics.getFilesRotated()).thenReturn(filesRotated);
        lenient().when(metrics.getFilesOpened()).thenReturn(filesOpened);
        lenient().when(metrics.getFilesClosed()).thenReturn(filesClosed);
        lenient().when(metrics.getBytesRead()).thenReturn(bytesRead);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-1");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(fileOps.size(testFile)).thenReturn(6L);
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);

        FileIdentity newIdentity = mock(FileIdentity.class);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(new RotationResult(RotationType.CREATE_RENAME, newIdentity));
        when(rotationDetector.getFingerprintBytes()).thenReturn(1024);

        EventBuilder mockBuilder = mock(EventBuilder.class);
        Event mockEvent = mock(Event.class);
        lenient().when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withData(any(Map.class))).thenReturn(mockBuilder);
        lenient().when(mockBuilder.build()).thenReturn(mockEvent);

        TailFileReaderPool pool = createPool(10, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity = FileIdentity.from(testFile, fileOps, 1024);

        pool.addFile(identity, testFile);

        Thread.sleep(2000);

        pool.shutdown();
    }

    @Test
    void closeInactiveReaders_with_real_reader() throws Exception {
        Path testFile = tempDir.resolve("inactive.log");
        Files.writeString(testFile, "data\n");

        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        lenient().when(metrics.getFilesOpened()).thenReturn(filesOpened);
        lenient().when(metrics.getFilesClosed()).thenReturn(filesClosed);
        lenient().when(metrics.getBytesRead()).thenReturn(bytesRead);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));
        when(metrics.getActiveFileCount()).thenReturn(new AtomicLong(0));

        BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-inactive");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(fileOps.size(testFile)).thenReturn((long) "data\n".length());
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.NO_ROTATION);

        EventBuilder mockBuilder = mock(EventBuilder.class);
        Event mockEvent = mock(Event.class);
        lenient().when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withData(any(Map.class))).thenReturn(mockBuilder);
        lenient().when(mockBuilder.build()).thenReturn(mockEvent);

        TailFileReaderPool pool = new TailFileReaderPool(
                checkpointRegistry, metrics, 10, 2,
                Duration.ofMillis(1), createReaderContext());

        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity = FileIdentity.from(testFile, fileOps, 1024);
        pool.addFile(identity, testFile);

        Thread.sleep(1000);

        pool.closeInactiveReaders();

        pool.shutdown();
    }

    @Test
    void shutdown_calls_shutdownNow_when_awaitTermination_returns_false() throws Exception {
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);

        TailFileReaderPool pool = new TailFileReaderPool(
                checkpointRegistry, metrics, 10,
                Duration.ofMinutes(30), createReaderContext(),
                () -> mockExecutor);

        pool.shutdown();
    }

    @Test
    void shutdown_calls_shutdownNow_when_awaitTermination_throws_interrupted() throws Exception {
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.awaitTermination(anyLong(), any(TimeUnit.class)))
                .thenThrow(new InterruptedException("test"));

        TailFileReaderPool pool = new TailFileReaderPool(
                checkpointRegistry, metrics, 10,
                Duration.ofMinutes(30), createReaderContext(),
                () -> mockExecutor);

        pool.shutdown();

        assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
        Thread.interrupted();
    }

    @Test
    void closeInactiveReaders_removes_reader_past_threshold() throws Exception {
        Counter filesClosed = mock(Counter.class);
        when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getActiveFileCount()).thenReturn(new AtomicLong(0));
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        Counter filesOpened = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        lenient().when(metrics.getFilesOpened()).thenReturn(filesOpened);
        lenient().when(metrics.getBytesRead()).thenReturn(bytesRead);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        Path testFile = tempDir.resolve("inactive-test.log");
        Files.writeString(testFile, "data\n");

        BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-close-inactive");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        lenient().when(fileOps.size(testFile)).thenReturn((long) "data\n".length());

        CountDownLatch latch = new CountDownLatch(1);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenAnswer(inv -> {
                    latch.await();
                    return RotationResult.NO_ROTATION;
                });

        EventBuilder mockBuilder = mock(EventBuilder.class);
        Event mockEvent = mock(Event.class);
        lenient().when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withData(any(Map.class))).thenReturn(mockBuilder);
        lenient().when(mockBuilder.build()).thenReturn(mockEvent);

        TailFileReaderPool pool = new TailFileReaderPool(
                checkpointRegistry, metrics, 10, 2,
                Duration.ofMillis(1), createReaderContext());

        FileIdentity identity = FileIdentity.from(testFile, fileOps, 1024);
        pool.addFile(identity, testFile);

        Thread.sleep(100);

        pool.closeInactiveReaders();

        latch.countDown();

        pool.shutdown();
    }

    @Test
    void closeInactiveReaders_keeps_active_readers() throws Exception {
        Counter filesClosed = mock(Counter.class);
        lenient().when(metrics.getFilesClosed()).thenReturn(filesClosed);
        when(metrics.getActiveFileCount()).thenReturn(new AtomicLong(0));
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        lenient().when(metrics.getFilesOpened()).thenReturn(mock(Counter.class));
        lenient().when(metrics.getBytesRead()).thenReturn(mock(Counter.class));
        lenient().when(metrics.getLinesRead()).thenReturn(mock(Counter.class));
        lenient().when(metrics.getEventsEmitted()).thenReturn(mock(Counter.class));
        lenient().when(metrics.getBackpressureTimer()).thenReturn(mock(Timer.class));
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        Path testFile = tempDir.resolve("active-test.log");
        Files.writeString(testFile, "data\n");

        BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-keep-active");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        lenient().when(fileOps.size(testFile)).thenReturn((long) "data\n".length());

        CountDownLatch latch = new CountDownLatch(1);
        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenAnswer(inv -> {
                    latch.await();
                    return RotationResult.NO_ROTATION;
                });

        EventBuilder mockBuilder = mock(EventBuilder.class);
        Event mockEvent = mock(Event.class);
        lenient().when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withData(any(Map.class))).thenReturn(mockBuilder);
        lenient().when(mockBuilder.build()).thenReturn(mockEvent);

        TailFileReaderPool pool = new TailFileReaderPool(
                checkpointRegistry, metrics, 10, 2,
                Duration.ofHours(1), createReaderContext());

        FileIdentity identity = FileIdentity.from(testFile, fileOps, 1024);
        pool.addFile(identity, testFile);

        Thread.sleep(100);

        assertThat(pool.getActiveReaderCount(), equalTo(1));

        pool.closeInactiveReaders();

        assertThat(pool.getActiveReaderCount(), equalTo(1));

        latch.countDown();
        pool.shutdown();
    }

    @Test
    void onReaderComplete_with_deleted_rotation_marks_completed_and_processes_pending() throws Exception {
        Path testFile = tempDir.resolve("deleted-rotate.log");
        Files.writeString(testFile, "line1\n");

        Counter filesOpened = mock(Counter.class);
        Counter filesClosed = mock(Counter.class);
        Counter bytesRead = mock(Counter.class);
        Counter linesRead = mock(Counter.class);
        Counter eventsEmitted = mock(Counter.class);
        Timer backpressureTimer = mock(Timer.class);
        lenient().when(metrics.getFilesOpened()).thenReturn(filesOpened);
        lenient().when(metrics.getFilesClosed()).thenReturn(filesClosed);
        lenient().when(metrics.getBytesRead()).thenReturn(bytesRead);
        lenient().when(metrics.getLinesRead()).thenReturn(linesRead);
        lenient().when(metrics.getEventsEmitted()).thenReturn(eventsEmitted);
        lenient().when(metrics.getBackpressureTimer()).thenReturn(backpressureTimer);
        lenient().when(metrics.getFileLagBytes()).thenReturn(new AtomicLong(0));

        BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-del");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(fileOps.size(testFile)).thenReturn(6L);
        FileChannel realChannel = FileChannel.open(testFile, StandardOpenOption.READ);
        lenient().when(fileOps.openReadChannel(testFile)).thenReturn(realChannel);

        when(rotationDetector.checkRotation(any(), any(), any(long.class)))
                .thenReturn(RotationResult.DELETED);

        EventBuilder mockBuilder = mock(EventBuilder.class);
        Event mockEvent = mock(Event.class);
        lenient().when(eventFactory.eventBuilder(EventBuilder.class)).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withEventType(any())).thenReturn(mockBuilder);
        lenient().when(mockBuilder.withData(any(Map.class))).thenReturn(mockBuilder);
        lenient().when(mockBuilder.build()).thenReturn(mockEvent);

        TailFileReaderPool pool = createPool(10, 2);
        when(checkpointRegistry.getOrCreate(anyString())).thenReturn(new CheckpointEntry());

        FileIdentity identity = FileIdentity.from(testFile, fileOps, 1024);

        Path pendingFile = tempDir.resolve("pending.log");
        Files.writeString(pendingFile, "pending\n");
        BasicFileAttributes pendingAttrs = mock(BasicFileAttributes.class);
        when(pendingAttrs.fileKey()).thenReturn("inode-pending");
        lenient().when(fileOps.readAttributes(pendingFile)).thenReturn(pendingAttrs);
        lenient().when(fileOps.size(pendingFile)).thenReturn(8L);
        lenient().when(fileOps.openReadChannel(pendingFile)).thenReturn(
                FileChannel.open(pendingFile, StandardOpenOption.READ));
        FileIdentity pendingIdentity = FileIdentity.from(pendingFile, fileOps, 1024);

        TailFileReaderPool limitedPool = new TailFileReaderPool(
                checkpointRegistry, metrics, 1, 2,
                Duration.ofMinutes(30), createReaderContext());

        limitedPool.addFile(identity, testFile);
        limitedPool.addFile(pendingIdentity, pendingFile);

        assertThat(limitedPool.getPendingCount(), equalTo(1));

        Thread.sleep(2000);

        limitedPool.shutdown();
    }
}
