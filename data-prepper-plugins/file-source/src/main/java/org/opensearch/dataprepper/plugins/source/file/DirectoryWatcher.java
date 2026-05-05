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

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class DirectoryWatcher {

    @FunctionalInterface
    interface WatchServiceFactory {
        WatchService create() throws IOException;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryWatcher.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;
    private static final Set<String> NETWORK_FS_TYPES = Set.of(
            "nfs", "nfs4", "cifs", "smb", "smb2", "fuse.sshfs", "afs", "9p"
    );

    private final GlobPathResolver globPathResolver;
    private final TailFileReaderPool readerPool;
    private final CheckpointRegistry checkpointRegistry;
    private final FileSourceConfig config;
    private final FileSystemOperations fileOps;
    private final FileTailMetrics metrics;
    private final Duration rotateWait;
    private final boolean closeRemoved;
    private final Set<Path> knownFiles;
    private final WatchServiceFactory watchServiceFactory;
    private final Supplier<ScheduledExecutorService> pollSchedulerSupplier;
    private final Predicate<Path> networkFsCheck;
    private final boolean macOS;

    private volatile WatchService watchService;
    private volatile Thread watchThread;
    private volatile ScheduledExecutorService pollScheduler;
    private volatile boolean running;

    public DirectoryWatcher(final GlobPathResolver globPathResolver,
                            final TailFileReaderPool readerPool,
                            final CheckpointRegistry checkpointRegistry,
                            final FileSourceConfig config,
                            final FileSystemOperations fileOps,
                            final FileTailMetrics metrics,
                            final Duration rotateWait,
                            final boolean closeRemoved) {
        this(globPathResolver, readerPool, checkpointRegistry, config, fileOps, metrics, rotateWait, closeRemoved,
                () -> FileSystems.getDefault().newWatchService(), isMacOS());
    }

    DirectoryWatcher(final GlobPathResolver globPathResolver,
                            final TailFileReaderPool readerPool,
                            final CheckpointRegistry checkpointRegistry,
                            final FileSourceConfig config,
                            final FileSystemOperations fileOps,
                            final FileTailMetrics metrics,
                            final Duration rotateWait,
                            final boolean closeRemoved,
                            final WatchServiceFactory watchServiceFactory) {
        this(globPathResolver, readerPool, checkpointRegistry, config, fileOps, metrics, rotateWait, closeRemoved,
                watchServiceFactory, isMacOS());
    }

    DirectoryWatcher(final GlobPathResolver globPathResolver,
                            final TailFileReaderPool readerPool,
                            final CheckpointRegistry checkpointRegistry,
                            final FileSourceConfig config,
                            final FileSystemOperations fileOps,
                            final FileTailMetrics metrics,
                            final Duration rotateWait,
                            final boolean closeRemoved,
                            final WatchServiceFactory watchServiceFactory,
                            final boolean macOS) {
        this(globPathResolver, readerPool, checkpointRegistry, config, fileOps, metrics, rotateWait, closeRemoved,
                watchServiceFactory, macOS, DirectoryWatcher::createDefaultPollScheduler,
                DirectoryWatcher::isNetworkFilesystem);
    }

    DirectoryWatcher(final GlobPathResolver globPathResolver,
                            final TailFileReaderPool readerPool,
                            final CheckpointRegistry checkpointRegistry,
                            final FileSourceConfig config,
                            final FileSystemOperations fileOps,
                            final FileTailMetrics metrics,
                            final Duration rotateWait,
                            final boolean closeRemoved,
                            final WatchServiceFactory watchServiceFactory,
                            final boolean macOS,
                            final Supplier<ScheduledExecutorService> pollSchedulerSupplier) {
        this(globPathResolver, readerPool, checkpointRegistry, config, fileOps, metrics, rotateWait, closeRemoved,
                watchServiceFactory, macOS, pollSchedulerSupplier,
                DirectoryWatcher::isNetworkFilesystem);
    }

    DirectoryWatcher(final GlobPathResolver globPathResolver,
                            final TailFileReaderPool readerPool,
                            final CheckpointRegistry checkpointRegistry,
                            final FileSourceConfig config,
                            final FileSystemOperations fileOps,
                            final FileTailMetrics metrics,
                            final Duration rotateWait,
                            final boolean closeRemoved,
                            final WatchServiceFactory watchServiceFactory,
                            final boolean macOS,
                            final Supplier<ScheduledExecutorService> pollSchedulerSupplier,
                            final Predicate<Path> networkFsCheck) {
        this.globPathResolver = Objects.requireNonNull(globPathResolver, "globPathResolver must not be null");
        this.readerPool = Objects.requireNonNull(readerPool, "readerPool must not be null");
        this.checkpointRegistry = Objects.requireNonNull(checkpointRegistry, "checkpointRegistry must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.fileOps = Objects.requireNonNull(fileOps, "fileOps must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.rotateWait = Objects.requireNonNull(rotateWait, "rotateWait must not be null");
        this.closeRemoved = closeRemoved;
        this.knownFiles = ConcurrentHashMap.newKeySet();
        this.watchServiceFactory = Objects.requireNonNull(watchServiceFactory, "watchServiceFactory must not be null");
        this.macOS = macOS;
        this.pollSchedulerSupplier = Objects.requireNonNull(pollSchedulerSupplier, "pollSchedulerSupplier must not be null");
        this.networkFsCheck = Objects.requireNonNull(networkFsCheck, "networkFsCheck must not be null");
    }

    public void start() {
        running = true;

        final Set<Path> initialFiles = globPathResolver.resolve();
        knownFiles.addAll(initialFiles);
        for (final Path file : initialFiles) {
            addFileToPool(file);
        }

        final boolean useWatchService = shouldUseWatchService();
        if (useWatchService) {
            startWatchService();
        }

        startPollScheduler(useWatchService);
    }

    public void stop() {
        running = false;

        if (watchThread != null) {
            watchThread.interrupt();
        }

        if (watchService != null) {
            try {
                watchService.close();
            } catch (final IOException e) {
                LOG.warn("Error closing WatchService", e);
            }
        }

        if (pollScheduler != null) {
            pollScheduler.shutdown();
            try {
                if (!pollScheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    pollScheduler.shutdownNow();
                }
            } catch (final InterruptedException e) {
                pollScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (watchThread != null) {
            try {
                watchThread.join(TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean shouldUseWatchService() {
        final Set<Path> watchDirs = globPathResolver.getWatchDirectories();
        for (final Path dir : watchDirs) {
            if (Files.isDirectory(dir) && networkFsCheck.test(dir)) {
                LOG.info("Network filesystem detected at {}. Using polling only.", dir);
                return false;
            }
        }
        return true;
    }

    static boolean isNetworkFilesystem(final Path path) {
        try {
            final FileStore fileStore = Files.getFileStore(path);
            final String fsType = fileStore.type().toLowerCase();
            return NETWORK_FS_TYPES.contains(fsType);
        } catch (final IOException e) {
            LOG.warn("Unable to determine filesystem type for {}. Assuming local.", path);
            return false;
        }
    }

    private static boolean isMacOS() {
        return System.getProperty("os.name").toLowerCase().contains("mac");
    }

    private void startWatchService() {
        try {
            watchService = watchServiceFactory.create();
            final Set<Path> watchDirs = globPathResolver.getWatchDirectories();
            for (final Path dir : watchDirs) {
                if (Files.isDirectory(dir)) {
                    registerDirectory(dir);
                }
            }

            watchThread = new Thread(this::watchLoop, "file-tail-watcher");
            watchThread.setDaemon(true);
            watchThread.start();
            LOG.info("WatchService started for {} directories", watchDirs.size());
        } catch (final IOException | RuntimeException e) {
            LOG.error("Failed to create WatchService. Falling back to polling only.", e);
            watchService = null;
        }
    }

    private void registerDirectory(final Path dir) {
        try {
            dir.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.OVERFLOW);
            LOG.debug("Registered WatchService for directory: {}", dir);
        } catch (final IOException e) {
            LOG.warn("Failed to register WatchService for directory: {}", dir, e);
        }
    }

    static ScheduledExecutorService createDefaultPollScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "file-tail-poll");
            thread.setDaemon(true);
            return thread;
        });
    }

    private void startPollScheduler(final boolean watchServiceActive) {
        pollScheduler = pollSchedulerSupplier.get();

        final long intervalMillis;
        if (!watchServiceActive || macOS) {
            intervalMillis = config.getPollInterval().toMillis();
            LOG.info("Poll scheduler started with interval {}ms (primary mode)", intervalMillis);
        } else {
            final int supplementaryPollMultiplier = 10;
            intervalMillis = config.getPollInterval().toMillis() * supplementaryPollMultiplier;
            LOG.info("Poll scheduler started with interval {}ms (supplementary mode)", intervalMillis);
        }

        pollScheduler.scheduleAtFixedRate(this::pollScan, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void watchLoop() {
        while (running) {
            try {
                final WatchKey key = watchService.take();
                for (final WatchEvent<?> event : key.pollEvents()) {
                    handleWatchEvent(key, event);
                }
                if (!key.reset()) {
                    LOG.warn("WatchKey no longer valid. Directory may have been deleted.");
                }
            } catch (final ClosedWatchServiceException e) {
                LOG.debug("WatchService closed, exiting watch loop");
                break;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleWatchEvent(final WatchKey key, final WatchEvent<?> event) {
        final WatchEvent.Kind<?> kind = event.kind();

        if (kind == StandardWatchEventKinds.OVERFLOW) {
            LOG.warn("WatchService OVERFLOW detected. Triggering full rescan.");
            triggerFullRescan();
            return;
        }

        final Path watchedDir = (Path) key.watchable();
        final WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
        final Path child = watchedDir.resolve(pathEvent.context()).toAbsolutePath().normalize();

        if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
            if (Files.isRegularFile(child) && globPathResolver.matches(child)) {
                LOG.debug("New file detected via WatchService: {}", child);
                knownFiles.add(child);
                addFileToPool(child);
            }
        } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
            if (knownFiles.contains(child)) {
                LOG.debug("File deletion detected via WatchService: {}. Waiting {} before treating as removed.", child, rotateWait);
                try {
                    pollScheduler.schedule(() -> handleDeletion(child), rotateWait.toMillis(), TimeUnit.MILLISECONDS);
                } catch (final RejectedExecutionException e) {
                    LOG.debug("Poll scheduler rejected deletion handling (shutting down)");
                }
            }
        }
    }

    private void triggerFullRescan() {
        try {
            pollScheduler.execute(this::pollScan);
        } catch (final RejectedExecutionException e) {
            LOG.debug("Poll scheduler rejected rescan (shutting down)");
        }
    }

    private void handleDeletion(final Path file) {
        if (!Files.exists(file)) {
            knownFiles.remove(file);
            if (closeRemoved) {
                LOG.info("File confirmed removed after rotate_wait. Closing reader for: {}", file);
                readerPool.closeReaderForPath(file);
            } else {
                LOG.info("File confirmed removed after rotate_wait. closeRemoved=false, keeping reader open for: {}", file);
            }
        } else {
            LOG.debug("File reappeared during rotate_wait period (likely rotation): {}", file);
        }
    }

    void pollScan() {
        if (!running) {
            return;
        }

        try {
            readerPool.closeInactiveReaders();

            final Set<Path> currentFiles = globPathResolver.resolve();

            final Set<Path> newFiles = new HashSet<>(currentFiles);
            newFiles.removeAll(knownFiles);
            for (final Path file : newFiles) {
                LOG.debug("New file detected via poll scan: {}", file);
                addFileToPool(file);
            }

            final Set<Path> vanishedFiles = new HashSet<>(knownFiles);
            vanishedFiles.removeAll(currentFiles);
            for (final Path file : vanishedFiles) {
                LOG.debug("File vanished detected via poll scan: {}. Deferring by rotateWait.", file);
                if (closeRemoved) {
                    try {
                        pollScheduler.schedule(() -> handleDeletion(file), rotateWait.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (final RejectedExecutionException e) {
                        LOG.debug("Poll scheduler rejected vanished file handling (shutting down)");
                    }
                }
            }

            knownFiles.removeAll(vanishedFiles);
            knownFiles.addAll(currentFiles);
        } catch (final RuntimeException e) {
            LOG.error("Error during poll scan", e);
        }
    }

    private void addFileToPool(final Path file) {
        try {
            final FileIdentity identity = FileIdentity.from(file, fileOps, config.getFingerprintBytes());
            readerPool.addFile(identity, file);
        } catch (final RuntimeException e) {
            LOG.warn("Failed to add file to reader pool: {}", file, e);
        }
    }
}
