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

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Objects;

public final class TailFileReaderContext {

    private final Buffer<Record<Object>> buffer;
    private final EventFactory eventFactory;
    private final FileSystemOperations fileOps;
    private final FileTailMetrics metrics;
    private final RotationDetector rotationDetector;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final boolean acknowledgementsEnabled;
    private final Charset encoding;
    private final int readBufferSize;
    private final int maxLineLength;
    private final int writeTimeout;
    private final Duration maxReadTimePerFile;
    private final Duration rotationDrainTimeout;
    private final StartPosition startPosition;
    private final boolean includeFileMetadata;
    private final Duration acknowledgmentTimeout;
    private final int batchSize;
    private final Duration batchTimeout;
    private final int maxAcknowledgmentRetries;
    private final InputCodec codec;

    public TailFileReaderContext(final Buffer<Record<Object>> buffer,
                                final EventFactory eventFactory,
                                final FileSystemOperations fileOps,
                                final FileTailMetrics metrics,
                                final RotationDetector rotationDetector,
                                final AcknowledgementSetManager acknowledgementSetManager,
                                final boolean acknowledgementsEnabled,
                                final Charset encoding,
                                final int readBufferSize,
                                final int maxLineLength,
                                final int writeTimeout,
                                final Duration maxReadTimePerFile,
                                final Duration rotationDrainTimeout,
                                final StartPosition startPosition,
                                final boolean includeFileMetadata,
                                final Duration acknowledgmentTimeout,
                                final int batchSize,
                                final Duration batchTimeout,
                                final int maxAcknowledgmentRetries,
                                final InputCodec codec) {
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory must not be null");
        this.fileOps = Objects.requireNonNull(fileOps, "fileOps must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.rotationDetector = Objects.requireNonNull(rotationDetector, "rotationDetector must not be null");
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.acknowledgementsEnabled = acknowledgementsEnabled;
        this.encoding = Objects.requireNonNull(encoding, "encoding must not be null");
        this.readBufferSize = readBufferSize;
        this.maxLineLength = maxLineLength;
        this.writeTimeout = writeTimeout;
        this.maxReadTimePerFile = Objects.requireNonNull(maxReadTimePerFile, "maxReadTimePerFile must not be null");
        this.rotationDrainTimeout = Objects.requireNonNull(rotationDrainTimeout, "rotationDrainTimeout must not be null");
        this.startPosition = Objects.requireNonNull(startPosition, "startPosition must not be null");
        this.includeFileMetadata = includeFileMetadata;
        this.acknowledgmentTimeout = Objects.requireNonNull(acknowledgmentTimeout, "acknowledgmentTimeout must not be null");
        this.batchSize = batchSize;
        this.batchTimeout = Objects.requireNonNull(batchTimeout, "batchTimeout must not be null");
        this.maxAcknowledgmentRetries = maxAcknowledgmentRetries;
        this.codec = codec;
    }

    public Buffer<Record<Object>> getBuffer() {
        return buffer;
    }

    public EventFactory getEventFactory() {
        return eventFactory;
    }

    public FileSystemOperations getFileOps() {
        return fileOps;
    }

    public FileTailMetrics getMetrics() {
        return metrics;
    }

    public RotationDetector getRotationDetector() {
        return rotationDetector;
    }

    public AcknowledgementSetManager getAcknowledgementSetManager() {
        return acknowledgementSetManager;
    }

    public boolean isAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    public Charset getEncoding() {
        return encoding;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public int getMaxLineLength() {
        return maxLineLength;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public Duration getMaxReadTimePerFile() {
        return maxReadTimePerFile;
    }

    public Duration getRotationDrainTimeout() {
        return rotationDrainTimeout;
    }

    public StartPosition getStartPosition() {
        return startPosition;
    }

    public boolean isIncludeFileMetadata() {
        return includeFileMetadata;
    }

    public Duration getAcknowledgmentTimeout() {
        return acknowledgmentTimeout;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Duration getBatchTimeout() {
        return batchTimeout;
    }

    public int getMaxAcknowledgmentRetries() {
        return maxAcknowledgmentRetries;
    }

    public InputCodec getCodec() {
        return codec;
    }
}
