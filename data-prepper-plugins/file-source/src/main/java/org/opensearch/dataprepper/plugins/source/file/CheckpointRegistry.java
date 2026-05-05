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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class CheckpointRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointRegistry.class);
    private static final TypeReference<Map<String, CheckpointEntry>> MAP_TYPE = new TypeReference<>() { };

    private final ConcurrentHashMap<String, CheckpointEntry> storage;
    private final ObjectMapper objectMapper;
    private final Path checkpointFile;
    private final Duration cleanupAfter;
    private final ScheduledExecutorService scheduler;

    public CheckpointRegistry(final Path checkpointFile, final Duration flushInterval, final Duration cleanupAfter) {
        this(checkpointFile, flushInterval, cleanupAfter, () -> Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "checkpoint-flush");
            thread.setDaemon(true);
            return thread;
        }));
    }

    CheckpointRegistry(final Path checkpointFile, final Duration flushInterval, final Duration cleanupAfter,
                        final Supplier<ScheduledExecutorService> schedulerSupplier) {
        this.checkpointFile = checkpointFile;
        this.cleanupAfter = cleanupAfter;
        this.objectMapper = new ObjectMapper();
        this.storage = new ConcurrentHashMap<>();
        this.scheduler = schedulerSupplier.get();

        load();
        scheduler.scheduleAtFixedRate(this::flush, flushInterval.toMillis(), flushInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public CheckpointEntry getOrCreate(final String key) {
        return storage.computeIfAbsent(key, k -> new CheckpointEntry());
    }

    public CheckpointEntry get(final String key) {
        return storage.get(key);
    }

    public void markCompleted(final String key) {
        final CheckpointEntry entry = storage.get(key);
        if (entry != null) {
            entry.setStatus(CheckpointStatus.COMPLETED);
        }
    }

    public synchronized void flush() {
        if (checkpointFile == null) {
            return;
        }
        try {
            final Map<String, CheckpointEntry> snapshot = new HashMap<>();
            for (final Map.Entry<String, CheckpointEntry> entry : storage.entrySet()) {
                snapshot.put(entry.getKey(), entry.getValue().snapshot());
            }
            final Path tempFile = checkpointFile.resolveSibling(checkpointFile.getFileName() + ".tmp");
            objectMapper.writeValue(tempFile.toFile(), snapshot);
            Files.move(tempFile, checkpointFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            cleanupStaleEntries();
        } catch (final IOException e) {
            LOG.error("Failed to flush checkpoint file", e);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        boolean interrupted = false;
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (final InterruptedException e) {
            scheduler.shutdownNow();
            interrupted = true;
        }
        flush();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void load() {
        if (checkpointFile == null) {
            return;
        }
        try {
            if (checkpointFile.getParent() != null) {
                Files.createDirectories(checkpointFile.getParent());
            }
            final Map<String, CheckpointEntry> loaded = objectMapper.readValue(checkpointFile.toFile(), MAP_TYPE);
            if (loaded != null) {
                storage.putAll(loaded);
            }
            LOG.info("Loaded {} checkpoint entries from {}", storage.size(), checkpointFile);
        } catch (final FileNotFoundException | NoSuchFileException e) {
            LOG.debug("No existing checkpoint file at {}. Starting with empty state.", checkpointFile);
        } catch (final IOException e) {
            LOG.warn("Corrupt or unreadable checkpoint file at {}. Starting with empty state.", checkpointFile, e);
        }
    }

    private void cleanupStaleEntries() {
        final long now = System.currentTimeMillis();
        final long cleanupThreshold = cleanupAfter.toMillis();
        final Iterator<Map.Entry<String, CheckpointEntry>> iterator = storage.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, CheckpointEntry> entry = iterator.next();
            final CheckpointEntry checkpoint = entry.getValue();
            if (CheckpointStatus.COMPLETED == checkpoint.getStatus() &&
                    (now - checkpoint.getLastUpdatedMillis()) > cleanupThreshold) {
                iterator.remove();
                LOG.debug("Removed stale checkpoint entry: {}", entry.getKey());
            }
        }
    }
}
