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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.FileSystems;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DirectoryWatcherTest {

    @TempDir
    Path tempDir;

    @Mock
    private TailFileReaderPool readerPool;

    @Mock
    private CheckpointRegistry checkpointRegistry;

    @Mock
    private FileSourceConfig config;

    @Mock
    private FileTailMetrics metrics;

    private FileSystemOperations realFileOps;
    private GlobPathResolver globPathResolver;

    @BeforeEach
    void setUp() throws IOException {
        realFileOps = new DefaultFileSystemOperations();
    }

    private DirectoryWatcher createWatcher() {
        globPathResolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());
        return new DirectoryWatcher(globPathResolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true);
    }

    @Test
    void start_discovers_existing_files_and_adds_to_pool() throws IOException {
        Files.writeString(tempDir.resolve("initial.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        final DirectoryWatcher watcher = createWatcher();
        try {
            watcher.start();
            verify(readerPool, atLeastOnce()).addFile(any(FileIdentity.class), any(Path.class));
        } finally {
            watcher.stop();
        }
    }

    @Test
    void start_then_pollScan_detects_new_files() throws IOException {
        Files.writeString(tempDir.resolve("initial.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        final DirectoryWatcher watcher = createWatcher();
        try {
            watcher.start();

            Files.writeString(tempDir.resolve("new-file.log"), "new content");
            watcher.pollScan();

            verify(readerPool, atLeastOnce()).addFile(any(FileIdentity.class), any(Path.class));
        } finally {
            watcher.stop();
        }
    }

    @Test
    void pollScan_does_nothing_when_not_running() {
        final DirectoryWatcher watcher = createWatcher();
        watcher.stop();
        watcher.pollScan();
    }

    @Test
    void stop_completes_without_error_before_start() {
        final DirectoryWatcher watcher = createWatcher();
        watcher.stop();
    }

    @Test
    void stop_completes_without_error_after_start() throws IOException {
        Files.writeString(tempDir.resolve("initial.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        final DirectoryWatcher watcher = createWatcher();
        watcher.start();
        watcher.stop();
    }

    @Test
    void isNetworkFilesystem_returns_false_for_local_directory() {
        assertThat(DirectoryWatcher.isNetworkFilesystem(tempDir), equalTo(false));
    }

    @Test
    void isNetworkFilesystem_returns_false_on_IOException() {
        Path nonexistent = Path.of("/nonexistent-path-" + System.nanoTime());
        assertThat(DirectoryWatcher.isNetworkFilesystem(nonexistent), equalTo(false));
    }

    @Test
    void pollScan_detects_vanished_files_and_closes_when_close_removed_true() throws Exception {
        Files.writeString(tempDir.resolve("vanish.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.resolve("*.log").toString()), Collections.emptyList());
        final DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofMillis(100), true);
        try {
            watcher.start();

            Files.delete(tempDir.resolve("vanish.log"));
            watcher.pollScan();

            Thread.sleep(500);
            verify(readerPool, atLeastOnce()).closeReaderForPath(any(Path.class));
        } finally {
            watcher.stop();
        }
    }

    @Test
    void pollScan_handles_runtime_exception_from_glob_resolver() throws IOException {
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        GlobPathResolver badResolver = mock(GlobPathResolver.class);
        when(badResolver.resolve()).thenReturn(Set.of());
        when(badResolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(badResolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true);
        try {
            watcher.start();

            when(badResolver.resolve()).thenThrow(new RuntimeException("glob error"));
            watcher.pollScan();
        } finally {
            watcher.stop();
        }
    }

    @Test
    void addFileToPool_handles_runtime_exception() throws IOException {
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        doThrow(new RuntimeException("pool error")).when(readerPool).addFile(any(), any());

        Files.writeString(tempDir.resolve("error.log"), "content");
        final DirectoryWatcher watcher = createWatcher();
        try {
            watcher.start();
        } finally {
            watcher.stop();
        }
    }

    @Test
    void pollScan_does_not_close_when_close_removed_false() throws IOException {
        Files.writeString(tempDir.resolve("keep.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        globPathResolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());
        DirectoryWatcher watcher = new DirectoryWatcher(globPathResolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), false);
        try {
            watcher.start();

            Files.delete(tempDir.resolve("keep.log"));
            watcher.pollScan();

            verify(readerPool, never()).closeReaderForPath(any(Path.class));
        } finally {
            watcher.stop();
        }
    }

    @Test
    void stop_handles_interrupted_exception_on_poll_scheduler() throws Exception {
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        final DirectoryWatcher watcher = createWatcher();
        watcher.start();

        Thread stopThread = new Thread(() -> {
            Thread.currentThread().interrupt();
            watcher.stop();
        });
        stopThread.start();
        stopThread.join(5000);
    }

    @Test
    void start_uses_supplementary_poll_interval_with_watch_service() throws IOException {
        Files.writeString(tempDir.resolve("initial.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(1));

        final DirectoryWatcher watcher = createWatcher();
        try {
            watcher.start();
        } finally {
            watcher.stop();
        }
    }

    @Test
    void watch_loop_exits_on_closed_watch_service() throws Exception {
        Files.writeString(tempDir.resolve("watch.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        final DirectoryWatcher watcher = createWatcher();
        watcher.start();

        Thread.sleep(500);

        watcher.stop();
    }

    @Test
    void watch_loop_detects_new_file_created() throws Exception {
        Files.writeString(tempDir.resolve("existing.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        final DirectoryWatcher watcher = createWatcher();
        try {
            watcher.start();

            Thread.sleep(500);

            Files.writeString(tempDir.resolve("new-detected.log"), "new content");

            Thread.sleep(2000);
        } finally {
            watcher.stop();
        }
    }

    @Test
    void watch_loop_handles_file_deletion_with_close_removed_true() throws Exception {
        Files.writeString(tempDir.resolve("delete-me.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        globPathResolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());
        DirectoryWatcher watcher = new DirectoryWatcher(globPathResolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofMillis(100), true);
        try {
            watcher.start();

            Thread.sleep(500);

            Files.delete(tempDir.resolve("delete-me.log"));

            Thread.sleep(2000);
        } finally {
            watcher.stop();
        }
    }

    @Test
    void watch_loop_handles_file_deletion_with_close_removed_false() throws Exception {
        Files.writeString(tempDir.resolve("keep-me.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        globPathResolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());
        DirectoryWatcher watcher = new DirectoryWatcher(globPathResolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofMillis(100), false);
        try {
            watcher.start();

            Thread.sleep(500);

            Files.delete(tempDir.resolve("keep-me.log"));

            Thread.sleep(2000);
        } finally {
            watcher.stop();
        }
    }

    @Test
    void watch_loop_handles_file_reappearing_during_rotate_wait() throws Exception {
        Files.writeString(tempDir.resolve("rotate-reappear.log"), "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        globPathResolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());
        DirectoryWatcher watcher = new DirectoryWatcher(globPathResolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofMillis(500), true);
        try {
            watcher.start();

            Thread.sleep(500);

            Files.delete(tempDir.resolve("rotate-reappear.log"));
            Thread.sleep(100);
            Files.writeString(tempDir.resolve("rotate-reappear.log"), "new content");

            Thread.sleep(2000);
        } finally {
            watcher.stop();
        }
    }

    @Test
    void startWatchService_falls_back_on_exception() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> { throw new RuntimeException("cannot create WatchService"); });
        try {
            watcher.start();
            Thread.sleep(200);
        } finally {
            watcher.stop();
        }
    }

    @Test
    void startWatchService_falls_back_on_io_exception() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> { throw new IOException("cannot create WatchService"); });
        try {
            watcher.start();
            Thread.sleep(200);
        } finally {
            watcher.stop();
        }
    }

    @Test
    void stop_handles_pollScheduler_not_terminating() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true);
        watcher.start();
        watcher.stop();
    }

    @Test
    void shouldUseWatchService_returns_false_for_network_filesystem() throws Exception {
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true);
        watcher.start();
        watcher.stop();
    }

    @Test
    void stop_handles_IOException_closing_WatchService() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        WatchService realWatchService = FileSystems.getDefault().newWatchService();

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> realWatchService);
        watcher.start();
        Thread.sleep(200);

        realWatchService.close();
        Thread.sleep(200);

        watcher.stop();
    }

    @Test
    void registerDirectory_handles_IOException() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        Path watchDir = tempDir.resolve("unreadable-watch");
        Files.createDirectory(watchDir);
        watchDir.toFile().setReadable(false);
        watchDir.toFile().setExecutable(false);
        when(resolver.getWatchDirectories()).thenReturn(Set.of(watchDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true);
        try {
            watcher.start();
            Thread.sleep(200);
        } finally {
            watchDir.toFile().setReadable(true);
            watchDir.toFile().setExecutable(true);
            watcher.stop();
        }
    }

    @Test
    void supplementary_poll_interval_when_not_macOS_and_watch_active() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(1));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> FileSystems.getDefault().newWatchService(),
                false);
        try {
            watcher.start();
            Thread.sleep(200);
        } finally {
            watcher.stop();
        }
    }

    @Test
    void watchLoop_handles_invalid_WatchKey() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        WatchKey mockKey = mock(WatchKey.class);
        when(mockKey.pollEvents()).thenReturn(Collections.emptyList());
        when(mockKey.reset()).thenReturn(false);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();
    }

    @Test
    void handleWatchEvent_handles_OVERFLOW() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        WatchEvent<?> overflowEvent = mock(WatchEvent.class);
        when(overflowEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.OVERFLOW);

        WatchKey mockKey = mock(WatchKey.class);
        when(mockKey.pollEvents()).thenReturn(List.of(overflowEvent));
        when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();
    }

    @Test
    void triggerFullRescan_handles_runtime_exception() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        WatchEvent<?> overflowEvent = mock(WatchEvent.class);
        when(overflowEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.OVERFLOW);

        WatchKey mockKey = mock(WatchKey.class);
        when(mockKey.pollEvents()).thenReturn(List.of(overflowEvent));
        when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve())
                .thenReturn(Set.of())
                .thenThrow(new RuntimeException("rescan error"));
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();
    }

    @Test
    void handleDeletion_file_reappears_during_rotateWait() throws Exception {
        Path testFile = tempDir.resolve("reappear.log");
        Files.writeString(testFile, "content");
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        WatchEvent<Path> deleteEvent = mock(WatchEvent.class);
        lenient().when(deleteEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.ENTRY_DELETE);
        lenient().when(deleteEvent.context()).thenReturn(testFile.getFileName());

        WatchEvent<Path> createEvent = mock(WatchEvent.class);
        lenient().when(createEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.ENTRY_CREATE);
        lenient().when(createEvent.context()).thenReturn(testFile.getFileName());

        WatchKey mockKey = mock(WatchKey.class);
        lenient().when(mockKey.watchable()).thenReturn(tempDir);
        lenient().when(mockKey.pollEvents())
                .thenReturn(List.of(createEvent))
                .thenReturn(List.of(deleteEvent))
                .thenReturn(Collections.emptyList());
        lenient().when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        lenient().when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of(testFile.toAbsolutePath().normalize()));
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));
        when(resolver.matches(testFile.toAbsolutePath().normalize())).thenReturn(true);

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofMillis(500), true,
                () -> mockWatchService);
        watcher.start();

        Thread.sleep(2000);
        watcher.stop();
    }

    @Test
    void stop_pollScheduler_shutdownNow_on_timeout() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> { throw new RuntimeException("no watch service"); },
                false);
        watcher.start();
        watcher.stop();
    }

    @Test
    void stop_handles_IOException_when_watchService_close_fails() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take()).thenThrow(new ClosedWatchServiceException());
        doThrow(new IOException("close error")).when(mockWatchService).close();

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-ws-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService);
        watcher.start();
        Thread.sleep(200);
        watcher.stop();
    }

    @Test
    void stop_handles_pollScheduler_awaitTermination_returning_false() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-ps-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> { throw new RuntimeException("no watch service"); },
                false,
                () -> mockScheduler);
        watcher.start();
        watcher.stop();
    }

    @Test
    void stop_handles_pollScheduler_awaitTermination_throws_interrupted() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class)))
                .thenThrow(new InterruptedException("test"));

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-psi-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> { throw new RuntimeException("no watch service"); },
                false,
                () -> mockScheduler);
        watcher.start();
        watcher.stop();

        assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
        Thread.interrupted();
    }

    @Test
    void stop_handles_watchThread_join_interrupted() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        CountDownLatch watchStarted = new CountDownLatch(1);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take()).thenAnswer(inv -> {
            watchStarted.countDown();
            while (!Thread.currentThread().isInterrupted()) {
                LockSupport.parkNanos(100_000_000L);
            }
            Thread.interrupted();
            Thread.sleep(2000);
            throw new ClosedWatchServiceException();
        });

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-wtj-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService,
                false,
                () -> mockScheduler);
        watcher.start();
        watchStarted.await();

        Thread stopThread = new Thread(() -> {
            Thread.currentThread().interrupt();
            watcher.stop();
        });
        stopThread.start();
        stopThread.join(10000);

        assertThat(stopThread.isAlive(), equalTo(false));
    }

    @Test
    void watch_loop_exits_when_running_becomes_false() throws Exception {
        lenient().when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        CountDownLatch firstIterDone = new CountDownLatch(1);

        WatchKey mockKey = mock(WatchKey.class);
        lenient().when(mockKey.pollEvents()).thenReturn(Collections.emptyList());
        lenient().when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take()).thenAnswer(inv -> {
            firstIterDone.countDown();
            return mockKey;
        }).thenAnswer(inv -> {
            Thread.sleep(60000);
            return mockKey;
        });

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        lenient().when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService,
                false,
                () -> mockScheduler);
        watcher.start();
        firstIterDone.await();
        Thread.sleep(50);
        watcher.stop();
    }

    @Test
    void handleWatchEvent_ignores_file_not_matching_glob() throws Exception {
        Path txtFile = tempDir.resolve("ignored.txt");
        Files.writeString(txtFile, "content");
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        lenient().when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        WatchEvent<Path> createEvent = mock(WatchEvent.class);
        lenient().when(createEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.ENTRY_CREATE);
        lenient().when(createEvent.context()).thenReturn(txtFile.getFileName());

        WatchKey mockKey = mock(WatchKey.class);
        lenient().when(mockKey.watchable()).thenReturn(tempDir);
        lenient().when(mockKey.pollEvents()).thenReturn(List.of(createEvent));
        lenient().when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        lenient().when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));
        when(resolver.matches(any())).thenReturn(false);

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        lenient().when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService,
                false,
                () -> mockScheduler);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();

        verify(readerPool, never()).addFile(any(), any());
    }

    @Test
    void handleWatchEvent_ignores_ENTRY_MODIFY_event() throws Exception {
        Path logFile = tempDir.resolve("modify.log");
        Files.writeString(logFile, "content");
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        lenient().when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        WatchEvent<Path> modifyEvent = mock(WatchEvent.class);
        lenient().when(modifyEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.ENTRY_MODIFY);
        lenient().when(modifyEvent.context()).thenReturn(logFile.getFileName());

        WatchKey mockKey = mock(WatchKey.class);
        lenient().when(mockKey.watchable()).thenReturn(tempDir);
        lenient().when(mockKey.pollEvents()).thenReturn(List.of(modifyEvent));
        lenient().when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        lenient().when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService,
                false,
                () -> mockScheduler);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();

        verify(readerPool, never()).addFile(any(), any());
        verify(readerPool, never()).closeReaderForPath(any());
    }

    @Test
    void handleWatchEvent_DELETE_for_unknown_file_does_nothing() throws Exception {
        Path unknownFile = tempDir.resolve("unknown.log");
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        lenient().when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        WatchEvent<Path> deleteEvent = mock(WatchEvent.class);
        lenient().when(deleteEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.ENTRY_DELETE);
        lenient().when(deleteEvent.context()).thenReturn(unknownFile.getFileName());

        WatchKey mockKey = mock(WatchKey.class);
        lenient().when(mockKey.watchable()).thenReturn(tempDir);
        lenient().when(mockKey.pollEvents()).thenReturn(List.of(deleteEvent));
        lenient().when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        lenient().when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService,
                false,
                () -> mockScheduler);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();

        verify(readerPool, never()).closeReaderForPath(any());
    }

    @Test
    void shouldUseWatchService_returns_false_when_network_fs_detected() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> FileSystems.getDefault().newWatchService(),
                false,
                DirectoryWatcher::createDefaultPollScheduler,
                path -> true);
        watcher.start();
        Thread.sleep(200);
        watcher.stop();
    }

    @Test
    void handleWatchEvent_DELETE_rejectedExecutionException_on_schedule() throws Exception {
        Path logFile = tempDir.resolve("rej-delete.log");
        Files.writeString(logFile, "content");
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);
        lenient().when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        WatchEvent<Path> createEvent = mock(WatchEvent.class);
        lenient().when(createEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.ENTRY_CREATE);
        lenient().when(createEvent.context()).thenReturn(logFile.getFileName());

        WatchEvent<Path> deleteEvent = mock(WatchEvent.class);
        lenient().when(deleteEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.ENTRY_DELETE);
        lenient().when(deleteEvent.context()).thenReturn(logFile.getFileName());

        WatchKey mockKey = mock(WatchKey.class);
        lenient().when(mockKey.watchable()).thenReturn(tempDir);
        lenient().when(mockKey.pollEvents())
                .thenReturn(List.of(createEvent))
                .thenReturn(List.of(deleteEvent));
        lenient().when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        lenient().when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of(logFile.toAbsolutePath().normalize()));
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));
        when(resolver.matches(any())).thenReturn(true);

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(mockScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenThrow(new RejectedExecutionException("shutting down"));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofMillis(100), true,
                () -> mockWatchService,
                false,
                () -> mockScheduler);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();
    }

    @Test
    void triggerFullRescan_handles_rejectedExecutionException() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        WatchEvent<?> overflowEvent = mock(WatchEvent.class);
        when(overflowEvent.kind()).thenReturn((WatchEvent.Kind) StandardWatchEventKinds.OVERFLOW);

        WatchKey mockKey = mock(WatchKey.class);
        when(mockKey.pollEvents()).thenReturn(List.of(overflowEvent));
        when(mockKey.reset()).thenReturn(true);

        WatchService mockWatchService = mock(WatchService.class);
        when(mockWatchService.take())
                .thenReturn(mockKey)
                .thenThrow(new ClosedWatchServiceException());

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        lenient().when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        doThrow(new RejectedExecutionException("shutting down"))
                .when(mockScheduler).execute(any(Runnable.class));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> mockWatchService,
                false,
                () -> mockScheduler);
        watcher.start();
        Thread.sleep(500);
        watcher.stop();
    }

    @Test
    void pollScan_vanished_file_rejectedExecutionException_on_schedule() throws Exception {
        Path vanishFile = tempDir.resolve("vanish-rej.log");
        Files.writeString(vanishFile, "content");
        when(config.getFingerprintBytes()).thenReturn(1024);
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        lenient().when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        lenient().when(mockScheduler.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));
        when(mockScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenThrow(new RejectedExecutionException("shutting down"));

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve())
                .thenReturn(Set.of(vanishFile.toAbsolutePath().normalize()))
                .thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(Path.of("/nonexistent-dir-" + System.nanoTime())));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofMillis(100), true,
                () -> { throw new RuntimeException("no watch service"); },
                false,
                () -> mockScheduler);
        watcher.start();
        watcher.pollScan();
        watcher.stop();
    }

    @Test
    void startWatchService_falls_back_to_polling_when_registration_fails() throws Exception {
        when(config.getPollInterval()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getFingerprintBytes()).thenReturn(1024);

        WatchService closedWatchService = FileSystems.getDefault().newWatchService();
        closedWatchService.close();

        GlobPathResolver resolver = mock(GlobPathResolver.class);
        when(resolver.resolve()).thenReturn(Set.of());
        when(resolver.getWatchDirectories()).thenReturn(Set.of(tempDir));

        DirectoryWatcher watcher = new DirectoryWatcher(resolver, readerPool, checkpointRegistry, config, realFileOps, metrics,
                Duration.ofSeconds(5), true,
                () -> closedWatchService,
                false,
                () -> Executors.newSingleThreadScheduledExecutor(),
                path -> false);
        watcher.start();
        Thread.sleep(200);
        watcher.stop();
    }
}
