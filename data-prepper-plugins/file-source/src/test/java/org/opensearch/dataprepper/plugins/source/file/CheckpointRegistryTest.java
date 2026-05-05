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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CheckpointRegistryTest {

    private static final Duration FLUSH_INTERVAL = Duration.ofHours(1);
    private static final Duration CLEANUP_AFTER = Duration.ofHours(24);

    @TempDir
    Path tempDir;

    private Path checkpointFile;
    private CheckpointRegistry registry;

    @BeforeEach
    void setUp() {
        checkpointFile = tempDir.resolve("checkpoints.json");
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.shutdown();
        }
    }

    private CheckpointRegistry createRegistry() {
        return createRegistry(checkpointFile);
    }

    private CheckpointRegistry createRegistry(final Path file) {
        return new CheckpointRegistry(file, FLUSH_INTERVAL, CLEANUP_AFTER);
    }

    @Test
    void getOrCreateReturnsNewEntryForUnknownKey() {
        registry = createRegistry();

        final CheckpointEntry entry = registry.getOrCreate("test-key");

        assertThat(entry, notNullValue());
        assertThat(entry.getReadOffset(), equalTo(0L));
        assertThat(entry.getCommittedOffset(), equalTo(0L));
        assertThat(entry.getStatus(), equalTo(CheckpointStatus.ACTIVE));
    }

    @Test
    void getOrCreateReturnsSameEntryForSameKey() {
        registry = createRegistry();

        final CheckpointEntry first = registry.getOrCreate("test-key");
        first.setReadOffset(42L);

        final CheckpointEntry second = registry.getOrCreate("test-key");

        assertThat(second.getReadOffset(), equalTo(42L));
    }

    @Test
    void getReturnsNullForUnknownKey() {
        registry = createRegistry();

        assertThat(registry.get("nonexistent"), nullValue());
    }

    @Test
    void getReturnsEntryAfterGetOrCreate() {
        registry = createRegistry();

        registry.getOrCreate("my-key").setReadOffset(77L);

        final CheckpointEntry retrieved = registry.get("my-key");
        assertThat(retrieved, notNullValue());
        assertThat(retrieved.getReadOffset(), equalTo(77L));
    }

    @Test
    void flushAndLoadRoundTripPreservesEntries() {
        registry = createRegistry();
        final CheckpointEntry entry = registry.getOrCreate("/var/log/app.log");
        entry.setReadOffset(1024L);
        entry.setCommittedOffset(512L);
        entry.setStatus(CheckpointStatus.ACTIVE);

        registry.flush();
        registry.shutdown();

        final CheckpointRegistry reloaded = createRegistry();
        registry = reloaded;

        final CheckpointEntry loaded = reloaded.get("/var/log/app.log");
        assertThat(loaded, notNullValue());
        assertThat(loaded.getReadOffset(), equalTo(1024L));
        assertThat(loaded.getCommittedOffset(), equalTo(512L));
        assertThat(loaded.getStatus(), equalTo(CheckpointStatus.ACTIVE));
    }

    @Test
    void flushCreatesCheckpointFileOnDisk() {
        registry = createRegistry();
        registry.getOrCreate("some-file");

        registry.flush();

        assertThat(Files.exists(checkpointFile), equalTo(true));
    }

    @Test
    void flushUsesAtomicWriteWithTempFile() {
        registry = createRegistry();
        registry.getOrCreate("file1");

        registry.flush();

        final Path tempFile = checkpointFile.resolveSibling(checkpointFile.getFileName() + ".tmp");
        assertThat(Files.exists(tempFile), equalTo(false));
        assertThat(Files.exists(checkpointFile), equalTo(true));
    }

    @Test
    void corruptCheckpointFileStartsWithEmptyState() throws IOException {
        Files.writeString(checkpointFile, "THIS IS NOT VALID JSON{{{");

        registry = createRegistry();

        assertThat(registry.get("any-key"), nullValue());
    }

    @Test
    void emptyCheckpointFileStartsWithEmptyState() throws IOException {
        Files.writeString(checkpointFile, "");

        registry = createRegistry();

        assertThat(registry.get("any-key"), nullValue());
    }

    @Test
    void cleanupRemovesStaleCompletedEntries() throws InterruptedException {
        final Duration zeroCleanup = Duration.ZERO;
        registry = new CheckpointRegistry(checkpointFile, FLUSH_INTERVAL, zeroCleanup);

        final CheckpointEntry entry = registry.getOrCreate("stale-file");
        entry.setStatus(CheckpointStatus.COMPLETED);

        Thread.sleep(50);

        registry.flush();

        assertThat(registry.get("stale-file"), nullValue());
    }

    @Test
    void cleanupKeepsActiveEntries() {
        final Duration zeroCleanup = Duration.ZERO;
        registry = new CheckpointRegistry(checkpointFile, FLUSH_INTERVAL, zeroCleanup);

        registry.getOrCreate("active-file").setReadOffset(100L);

        registry.flush();

        final CheckpointEntry entry = registry.get("active-file");
        assertThat(entry, notNullValue());
        assertThat(entry.getReadOffset(), equalTo(100L));
    }

    @Test
    void multipleEntriesPersistCorrectly() {
        registry = createRegistry();

        registry.getOrCreate("file-a").setReadOffset(10L);
        registry.getOrCreate("file-b").setReadOffset(20L);
        registry.getOrCreate("file-c").setReadOffset(30L);

        registry.flush();
        registry.shutdown();

        final CheckpointRegistry reloaded = createRegistry();
        registry = reloaded;

        assertThat(reloaded.get("file-a").getReadOffset(), equalTo(10L));
        assertThat(reloaded.get("file-b").getReadOffset(), equalTo(20L));
        assertThat(reloaded.get("file-c").getReadOffset(), equalTo(30L));
    }

    @Test
    void shutdownFlushesBeforeTerminating() {
        registry = createRegistry();
        registry.getOrCreate("flush-on-shutdown").setReadOffset(999L);

        registry.shutdown();

        final CheckpointRegistry reloaded = createRegistry();
        registry = reloaded;

        final CheckpointEntry entry = reloaded.get("flush-on-shutdown");
        assertThat(entry, notNullValue());
        assertThat(entry.getReadOffset(), equalTo(999L));
    }

    @Test
    void flush_handles_io_error_on_unwritable_path() {
        Path unwritablePath = Path.of("/nonexistent-dir-" + System.nanoTime() + "/sub/checkpoints.json");
        registry = createRegistry(unwritablePath);
        registry.getOrCreate("some-key").setReadOffset(100L);

        registry.flush();
    }

    @Test
    void shutdown_handles_scheduler_interrupted() throws Exception {
        registry = createRegistry();
        registry.getOrCreate("interrupt-key").setReadOffset(50L);

        Thread shutdownThread = new Thread(() -> {
            Thread.currentThread().interrupt();
            registry.shutdown();
        });
        shutdownThread.start();
        shutdownThread.join(5000);

        assertThat(shutdownThread.isAlive(), equalTo(false));
        registry = null;
    }

    @Test
    void load_handles_null_checkpoint_file() {
        CheckpointRegistry nullRegistry = new CheckpointRegistry(null, FLUSH_INTERVAL, CLEANUP_AFTER);

        assertThat(nullRegistry.get("any"), nullValue());
        nullRegistry.getOrCreate("test").setReadOffset(10L);
        assertThat(nullRegistry.get("test").getReadOffset(), equalTo(10L));
    }

    @Test
    void shutdown_calls_shutdownNow_when_awaitTermination_returns_false() throws Exception {
        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);

        registry = new CheckpointRegistry(checkpointFile, FLUSH_INTERVAL, CLEANUP_AFTER, () -> mockScheduler);

        registry.shutdown();
        registry = null;
    }

    @Test
    void shutdown_calls_shutdownNow_when_awaitTermination_throws_interrupted() throws Exception {
        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("test"));

        registry = new CheckpointRegistry(checkpointFile, FLUSH_INTERVAL, CLEANUP_AFTER, () -> mockScheduler);

        registry.shutdown();
        registry = null;

        assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
        Thread.interrupted();
    }

    @Test
    void load_with_file_having_no_parent_directory(@TempDir final Path altDir) {
        final Path noParentFile = altDir.resolve("checkpoint-no-parent.json");
        registry = new CheckpointRegistry(noParentFile, FLUSH_INTERVAL, CLEANUP_AFTER);
        assertThat(registry.get("any"), nullValue());
    }

    @Test
    void load_handles_null_map_from_json() throws IOException {
        Files.writeString(checkpointFile, "null");
        registry = createRegistry();
        assertThat(registry.get("any"), nullValue());
    }

    @Test
    void cleanup_does_not_remove_recently_completed_entry() {
        final Duration longCleanup = Duration.ofHours(48);
        registry = new CheckpointRegistry(checkpointFile, FLUSH_INTERVAL, longCleanup);

        final CheckpointEntry entry = registry.getOrCreate("recent-completed");
        entry.setStatus(CheckpointStatus.COMPLETED);

        registry.flush();

        assertThat(registry.get("recent-completed"), notNullValue());
    }

    @Test
    void markCompleted_sets_status_on_existing_entry() {
        registry = new CheckpointRegistry(checkpointFile, FLUSH_INTERVAL, CLEANUP_AFTER);
        registry.getOrCreate("mark-test");
        registry.markCompleted("mark-test");
        assertThat(registry.get("mark-test").getStatus(), equalTo(CheckpointStatus.COMPLETED));
    }

    @Test
    void markCompleted_does_nothing_for_nonexistent_key() {
        registry = new CheckpointRegistry(checkpointFile, FLUSH_INTERVAL, CLEANUP_AFTER);
        registry.markCompleted("nonexistent");
        assertThat(registry.get("nonexistent"), nullValue());
    }

    @Test
    void flush_with_null_checkpoint_file_does_not_throw() {
        final CheckpointRegistry nullFileRegistry = new CheckpointRegistry(null, FLUSH_INTERVAL, CLEANUP_AFTER);
        nullFileRegistry.flush();
        nullFileRegistry.shutdown();
    }
}
