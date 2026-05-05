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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class TailFileReaderPool {

    private static final Logger LOG = LoggerFactory.getLogger(TailFileReaderPool.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;
    private static final long RE_POLL_DELAY_MILLIS = 500;

    private final ConcurrentHashMap<FileIdentity, TailFileReader> activeReaders;
    private final Set<FileIdentity> pendingIdentities;
    private final ConcurrentLinkedQueue<PendingFile> pendingQueue;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler;
    private final CheckpointRegistry checkpointRegistry;
    private final FileTailMetrics metrics;
    private final int maxActiveFiles;
    private final Duration closeInactive;
    private final TailFileReaderContext readerContext;

    public TailFileReaderPool(final CheckpointRegistry checkpointRegistry,
                              final FileTailMetrics metrics,
                              final int maxActiveFiles,
                              final int readerThreads,
                              final Duration closeInactive,
                              final TailFileReaderContext readerContext) {
        this(checkpointRegistry, metrics, maxActiveFiles, closeInactive, readerContext,
                () -> Executors.newFixedThreadPool(readerThreads, r -> {
                    final Thread thread = new Thread(r, "tail-reader");
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    TailFileReaderPool(final CheckpointRegistry checkpointRegistry,
                       final FileTailMetrics metrics,
                       final int maxActiveFiles,
                       final Duration closeInactive,
                       final TailFileReaderContext readerContext,
                       final Supplier<ExecutorService> executorServiceSupplier) {
        this.checkpointRegistry = Objects.requireNonNull(checkpointRegistry, "checkpointRegistry must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.maxActiveFiles = maxActiveFiles;
        this.closeInactive = Objects.requireNonNull(closeInactive, "closeInactive must not be null");
        this.readerContext = Objects.requireNonNull(readerContext, "readerContext must not be null");
        this.activeReaders = new ConcurrentHashMap<>();
        this.pendingIdentities = ConcurrentHashMap.newKeySet();
        this.pendingQueue = new ConcurrentLinkedQueue<>();
        this.executorService = executorServiceSupplier.get();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "tail-reader-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    public synchronized void addFile(final FileIdentity fileIdentity, final Path path) {
        if (activeReaders.containsKey(fileIdentity) || pendingIdentities.contains(fileIdentity)) {
            return;
        }

        if (activeReaders.size() < maxActiveFiles) {
            submitReader(fileIdentity, path);
        } else {
            pendingIdentities.add(fileIdentity);
            pendingQueue.add(new PendingFile(fileIdentity, path));
            LOG.debug("File queued as pending: {}", path);
        }
    }

    private synchronized void submitReader(final FileIdentity fileIdentity, final Path path) {
        if (executorService.isShutdown() || activeReaders.containsKey(fileIdentity)) {
            return;
        }
        final CheckpointEntry checkpoint = checkpointRegistry.getOrCreate(fileIdentity.toString());
        final TailFileReader reader = new TailFileReader(
                path, fileIdentity, checkpoint, readerContext,
                () -> onReaderComplete(fileIdentity, path));
        activeReaders.put(fileIdentity, reader);
        metrics.getActiveFileCount().incrementAndGet();
        executorService.submit(reader);
    }

    private synchronized void onReaderComplete(final FileIdentity fileIdentity, final Path path) {
        final TailFileReader completedReader = activeReaders.remove(fileIdentity);
        if (completedReader == null) {
            return;
        }
        metrics.getActiveFileCount().decrementAndGet();

        if (completedReader != null && completedReader.getLastRotationType() == RotationType.CREATE_RENAME) {
            LOG.info("Re-adding path {} after create/rename rotation", path);
            final FileIdentity newIdentity = FileIdentity.from(path, readerContext.getFileOps(),
                    readerContext.getRotationDetector().getFingerprintBytes());
            submitReader(newIdentity, path);
        } else if (completedReader != null && completedReader.getLastRotationType() != RotationType.DELETED) {
            final PendingFile next = pendingQueue.poll();
            if (next != null) {
                pendingIdentities.remove(next.getFileIdentity());
                submitReader(next.getFileIdentity(), next.getPath());
                pendingQueue.add(new PendingFile(fileIdentity, path));
                pendingIdentities.add(fileIdentity);
            } else {
                scheduler.schedule(() -> submitReader(fileIdentity, path), RE_POLL_DELAY_MILLIS, TimeUnit.MILLISECONDS);
            }
        } else {
            checkpointRegistry.markCompleted(fileIdentity.toString());
            final PendingFile next = pendingQueue.poll();
            if (next != null) {
                pendingIdentities.remove(next.getFileIdentity());
                submitReader(next.getFileIdentity(), next.getPath());
            }
        }
    }

    public void shutdown() {
        scheduler.shutdownNow();
        executorService.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn("Reader pool did not terminate within the allowed time");
                }
            }
        } catch (final InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public int getActiveReaderCount() {
        return activeReaders.size();
    }

    public int getPendingCount() {
        return pendingQueue.size();
    }

    public synchronized void closeInactiveReaders() {
        final long now = System.currentTimeMillis();
        final long inactiveThresholdMillis = closeInactive.toMillis();
        activeReaders.entrySet().removeIf(entry -> {
            final TailFileReader reader = entry.getValue();
            if ((now - reader.getLastActivityMillis()) >= inactiveThresholdMillis) {
                LOG.info("Closing inactive reader for file identity {}", entry.getKey());
                metrics.getActiveFileCount().decrementAndGet();
                metrics.getFilesClosed().increment();
                return true;
            }
            return false;
        });
    }

    public synchronized void closeReaderForPath(final Path path) {
        final Path absolutePath = path.toAbsolutePath().normalize();
        activeReaders.entrySet().removeIf(entry -> {
            final TailFileReader reader = entry.getValue();
            if (reader.getPath().toAbsolutePath().normalize().equals(absolutePath)) {
                LOG.info("Closing reader for removed file: {}", path);
                metrics.getActiveFileCount().decrementAndGet();
                metrics.getFilesClosed().increment();
                return true;
            }
            return false;
        });
    }
}
