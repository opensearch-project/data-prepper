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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Objects;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

@DataPrepperPlugin(name = "file", pluginType = Source.class, pluginConfigurationType = FileSourceConfig.class)
public class FileSource implements Source<Record<Object>> {
    private static final String MESSAGE_KEY = "message";
    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() { };

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long STOP_WAIT_MILLIS = 200;
    private static final int MAX_FILES_PER_THREAD_WARNING_THRESHOLD = 250;
    private final FileSourceConfig fileSourceConfig;
    private final FileStrategy fileStrategy;
    private final EventFactory eventFactory;
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;
    private final DecompressionEngine decompressionEngine;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final boolean acknowledgementsEnabled;

    private Thread readThread;
    private FileReaderPool readerPool;
    private CheckpointRegistry checkpointRegistry;
    private DirectoryWatcher directoryWatcher;

    private volatile boolean isStopRequested;
    private final int writeTimeout;

    @DataPrepperPluginConstructor
    public FileSource(
            final FileSourceConfig fileSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
            final EventFactory eventFactory, final AcknowledgementSetManager acknowledgementSetManager) {
        Objects.requireNonNull(fileSourceConfig, "fileSourceConfig must not be null");
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory must not be null");
        this.pluginMetrics = Objects.requireNonNull(pluginMetrics, "pluginMetrics must not be null");
        this.pluginFactory = Objects.requireNonNull(pluginFactory, "pluginFactory must not be null");
        this.acknowledgementsEnabled = fileSourceConfig.isAcknowledgments();
        if (acknowledgementsEnabled) {
            Objects.requireNonNull(acknowledgementSetManager, "AcknowledgementSetManager is required when acknowledgments is enabled");
        }
        this.acknowledgementSetManager = acknowledgementSetManager;
        fileSourceConfig.validate();
        this.fileSourceConfig = fileSourceConfig;
        this.isStopRequested = false;
        this.writeTimeout = FileSourceConfig.DEFAULT_TIMEOUT;
        this.decompressionEngine = fileSourceConfig.getCompression().getDecompressionEngine();

        if (fileSourceConfig.isLegacyConfig()) {
            fileStrategy = new ClassicFileStrategy();
        } else {
            fileStrategy = null;
        }
    }

    @Override
    public void start(final Buffer<Record<Object>> buffer) {
        Objects.requireNonNull(buffer, "Buffer cannot be null for file source to start");

        if (fileSourceConfig.isLegacyConfig()) {
            LOG.info("Starting file source in legacy mode with path: {}", fileSourceConfig.getFilePathToRead());
            readThread = new Thread(() -> {
                fileStrategy.start(buffer);
                LOG.info("Completed reading file.");
            }, "file-source");
            readThread.setDaemon(false);
            readThread.start();
            return;
        }

        startModernPath(buffer);
    }

    private void startModernPath(final Buffer<Record<Object>> buffer) {
        LOG.info("Starting file source with paths: {}", fileSourceConfig.getAllPaths());

        final int maxActiveFiles = fileSourceConfig.getMaxActiveFiles();
        final int readerThreads = fileSourceConfig.getEffectiveReaderThreads();
        if (readerThreads > 0 && maxActiveFiles / readerThreads > MAX_FILES_PER_THREAD_WARNING_THRESHOLD) {
            LOG.warn("max_active_files ({}) is {} times reader_threads ({}). Files with pending data may experience high latency.",
                    maxActiveFiles, maxActiveFiles / readerThreads, readerThreads);
        }

        try {
            final FileMetrics fileMetrics = new FileMetrics(pluginMetrics);
            final FileSystemOperations fileOps = new DefaultFileSystemOperations();

            final String checkpointPath = fileSourceConfig.getCheckpointFile();
            final Path cpFile;
            if (checkpointPath != null) {
                cpFile = Paths.get(checkpointPath);
            } else {
                LOG.warn("No checkpoint_file configured. Checkpoint state will not be persisted across restarts.");
                cpFile = null;
            }

            checkpointRegistry = new CheckpointRegistry(
                    cpFile,
                    fileSourceConfig.getCheckpointInterval(),
                    fileSourceConfig.getCheckpointCleanupAfter());

            final Charset encoding = Charset.forName(fileSourceConfig.getEncoding());

            final RotationDetector rotationDetector = new RotationDetector(fileOps, fileSourceConfig.getFingerprintBytes());

            final InputCodec fileCodec = createCodec();

            final FileReaderContext readerContext = new FileReaderContext(
                    buffer, eventFactory, fileOps, fileMetrics, rotationDetector,
                    acknowledgementSetManager, acknowledgementsEnabled,
                    encoding,
                    fileSourceConfig.getReadBufferSize(),
                    fileSourceConfig.getMaxLineLength(),
                    writeTimeout,
                    fileSourceConfig.getMaxReadTimePerFile(),
                    fileSourceConfig.getRotationDrainTimeout(),
                    fileSourceConfig.getStartPosition(),
                    fileSourceConfig.isIncludeFileMetadata(),
                    fileSourceConfig.getAcknowledgmentTimeout(),
                    fileSourceConfig.getBatchSize(),
                    fileSourceConfig.getBatchTimeout(),
                    fileSourceConfig.getMaxAcknowledgmentRetries(),
                    fileCodec,
                    fileSourceConfig.isTail(),
                    decompressionEngine);

            readerPool = new FileReaderPool(
                    checkpointRegistry, fileMetrics,
                    maxActiveFiles,
                    readerThreads,
                    fileSourceConfig.getCloseInactive(),
                    readerContext);

            final GlobPathResolver globPathResolver = new GlobPathResolver(
                    fileSourceConfig.getAllPaths(),
                    fileSourceConfig.getExcludePaths());

            if (fileSourceConfig.isTail()) {
                directoryWatcher = new DirectoryWatcher(
                        globPathResolver, readerPool, checkpointRegistry,
                        fileSourceConfig, fileOps, fileMetrics,
                        fileSourceConfig.getRotateWait(),
                        fileSourceConfig.isCloseRemoved());
                directoryWatcher.start();
            } else {
                final Set<Path> resolvedPaths = globPathResolver.resolve();
                for (final Path path : resolvedPaths) {
                    final FileIdentity fileIdentity = FileIdentity.from(path, fileOps, fileSourceConfig.getFingerprintBytes());
                    readerPool.addFile(fileIdentity, path);
                }
            }
        } catch (final RuntimeException e) {
            shutdownTailingResources();
            throw e;
        }
    }

    private void shutdownTailingResources() {
        if (directoryWatcher != null) {
            directoryWatcher.stop();
        }
        if (readerPool != null) {
            readerPool.shutdown();
        }
        if (checkpointRegistry != null) {
            checkpointRegistry.shutdown();
        }
    }

    private InputCodec createCodec() {
        if (fileSourceConfig.getCodec() == null) {
            return null;
        }
        final PluginModel codecConfiguration = fileSourceConfig.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(
                codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
    }

    @Override
    public void stop() {
        isStopRequested = true;

        shutdownTailingResources();

        if (readThread != null) {
            try {
                readThread.join(STOP_WAIT_MILLIS);
            } catch (final InterruptedException e) {
                readThread.interrupt();
            }
        }
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    private interface FileStrategy {
        void start(final Buffer<Record<Object>> buffer);
    }

    private class ClassicFileStrategy implements FileStrategy {
        @Override
        public void start(Buffer<Record<Object>> buffer) {
            final GlobPathResolver resolver = new GlobPathResolver(
                    fileSourceConfig.getAllPaths(), fileSourceConfig.getExcludePaths());
            final Set<Path> resolvedPaths = resolver.resolve();
            if (resolvedPaths.isEmpty() && fileSourceConfig.getFilePathToRead() != null) {
                resolvedPaths.add(Paths.get(fileSourceConfig.getFilePathToRead()).toAbsolutePath().normalize());
            }
            for (final Path filePath : resolvedPaths) {
                if (isStopRequested) {
                    break;
                }
                readFile(filePath, buffer);
            }
        }

        private void readFile(final Path filePath, final Buffer<Record<Object>> buffer) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(decompressionEngine.createInputStream(Files.newInputStream(filePath)), Charset.forName(fileSourceConfig.getEncoding())))) {
                String line;
                while ((line = reader.readLine()) != null && !isStopRequested) {
                    writeLineAsEventOrString(line, buffer);
                }
            } catch (IOException | TimeoutException | IllegalArgumentException ex) {
                LOG.error("Error processing the input file path [{}]", filePath, ex);
                throw new RuntimeException(format("Error processing the input file %s", filePath), ex);
            }
        }

        private Record<Object> getEventRecordFromLine(final String line) {
            Map<String, Object> structuredLine = new HashMap<>();

            switch(fileSourceConfig.getFormat()) {
                case JSON:
                    structuredLine = parseJson(line);
                    break;
                case PLAIN:
                    structuredLine.put(MESSAGE_KEY, line);
                    break;
            }

            return new Record<>(
            eventFactory.eventBuilder(EventBuilder.class)
                    .withEventType(fileSourceConfig.getRecordType().toString())
                    .withData(structuredLine)
                    .build());
        }

        private Map<String, Object> parseJson(final String jsonString) {
            try {
                return OBJECT_MAPPER.readValue(jsonString, MAP_TYPE_REFERENCE);
            } catch (JsonProcessingException e) {
                LOG.error(SENSITIVE, "Unable to parse json data [{}], assuming plain text", jsonString, e);
                final Map<String, Object> plainMap = new HashMap<>();
                plainMap.put(MESSAGE_KEY, jsonString);
                return plainMap;
            }
        }

        private void writeLineAsEventOrString(final String line, final Buffer<Record<Object>> buffer) throws TimeoutException, IllegalArgumentException {
            if (fileSourceConfig.getRecordType() == RecordType.EVENT) {
                buffer.write(getEventRecordFromLine(line), writeTimeout);
            } else if (fileSourceConfig.getRecordType() == RecordType.STRING) {
                buffer.write(new Record<>(line), writeTimeout);
            }
        }
    }

}
