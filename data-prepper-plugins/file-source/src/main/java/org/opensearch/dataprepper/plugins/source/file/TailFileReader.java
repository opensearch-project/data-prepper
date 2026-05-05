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

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class TailFileReader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TailFileReader.class);
    private static final long BACK_PRESSURE_SLEEP_MILLIS = 100;
    private static final String MESSAGE_KEY = "message";
    private static final String EVENT_TYPE = "event";
    private static final String FILE_PATH_KEY = "file_path";
    private static final String FILE_IDENTITY_KEY = "file_identity";

    private final FileIdentity fileIdentity;
    private final Path path;
    private final Buffer<Record<Object>> buffer;
    private final EventFactory eventFactory;
    private final FileSystemOperations fileOps;
    private final CheckpointEntry checkpointEntry;
    private final FileTailMetrics metrics;
    private final Charset encoding;
    private final int readBufferSize;
    private final int maxLineLength;
    private final int writeTimeout;
    private final Duration maxReadTimePerFile;
    private final boolean includeFileMetadata;
    private final Runnable onComplete;
    private final RotationDetector rotationDetector;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final boolean acknowledgementsEnabled;
    private final Duration acknowledgmentTimeout;
    private final int batchSize;
    private final StartPosition startPosition;
    private final Duration rotationDrainTimeout;
    private final Duration batchTimeout;
    private final int maxAcknowledgmentRetries;
    private final InputCodec codec;

    private final AtomicLong readOffset;
    private final StringBuilder partialLine;
    private final String cachedAbsolutePath;
    private final String cachedFileIdentityString;
    private volatile long lastActivityMillis;
    private boolean skippingToNewline;

    private AcknowledgementSet currentAckSet;
    private int currentBatchCount;
    private long batchStartOffset;
    private long batchOpenedAtMillis;
    private final AtomicInteger acknowledgmentRetryCount = new AtomicInteger(0);
    private volatile RotationType lastRotationType;

    public TailFileReader(final Path path,
                          final FileIdentity fileIdentity,
                          final CheckpointEntry checkpointEntry,
                          final TailFileReaderContext context,
                          final Runnable onComplete) {
        this.path = Objects.requireNonNull(path, "path must not be null");
        this.fileIdentity = Objects.requireNonNull(fileIdentity, "fileIdentity must not be null");
        this.checkpointEntry = Objects.requireNonNull(checkpointEntry, "checkpointEntry must not be null");
        Objects.requireNonNull(context, "context must not be null");
        this.onComplete = Objects.requireNonNull(onComplete, "onComplete must not be null");

        this.buffer = context.getBuffer();
        this.eventFactory = context.getEventFactory();
        this.fileOps = context.getFileOps();
        this.metrics = context.getMetrics();
        this.encoding = context.getEncoding();
        this.readBufferSize = context.getReadBufferSize();
        this.maxLineLength = context.getMaxLineLength();
        this.writeTimeout = context.getWriteTimeout();
        this.maxReadTimePerFile = context.getMaxReadTimePerFile();
        this.includeFileMetadata = context.isIncludeFileMetadata();
        this.rotationDetector = context.getRotationDetector();
        this.acknowledgementSetManager = context.getAcknowledgementSetManager();
        this.acknowledgementsEnabled = context.isAcknowledgementsEnabled();
        this.acknowledgmentTimeout = context.getAcknowledgmentTimeout();
        this.batchSize = context.getBatchSize();
        this.startPosition = context.getStartPosition();
        this.rotationDrainTimeout = context.getRotationDrainTimeout();
        this.batchTimeout = context.getBatchTimeout();
        this.maxAcknowledgmentRetries = context.getMaxAcknowledgmentRetries();
        this.codec = context.getCodec();

        this.readOffset = new AtomicLong(checkpointEntry.getReadOffset());
        if (checkpointEntry.getReadOffset() == 0 && startPosition == StartPosition.END) {
            try {
                final long fileSize = fileOps.size(path);
                this.readOffset.set(fileSize);
                checkpointEntry.setReadOffset(fileSize);
            } catch (final IOException e) {
                LOG.warn("Unable to determine file size for start_position=end on {}. Starting from offset 0.", path);
            }
        }
        this.partialLine = new StringBuilder();
        this.cachedAbsolutePath = path.toAbsolutePath().toString();
        this.cachedFileIdentityString = fileIdentity.toString();
        this.currentBatchCount = 0;
        this.batchStartOffset = readOffset.get();
        this.batchOpenedAtMillis = System.currentTimeMillis();
        this.lastRotationType = RotationType.NO_ROTATION;
        this.lastActivityMillis = System.currentTimeMillis();
    }

    @Override
    public void run() {
        try {
            final RotationResult rotation = rotationDetector.checkRotation(path, fileIdentity, readOffset.get());
            lastRotationType = rotation.getRotationType();

            switch (rotation.getRotationType()) {
                case COPYTRUNCATE:
                    LOG.info("Copytruncate detected for {}. Resetting offset to 0.", path);
                    metrics.getFilesRotated().increment();
                    completePendingAckSet();
                    readOffset.set(0);
                    checkpointEntry.setReadOffset(0);
                    batchStartOffset = 0;
                    readFile();
                    break;
                case DELETED:
                    LOG.info("File deleted: {}. Closing reader.", path);
                    completePendingAckSet();
                    break;
                case CREATE_RENAME:
                    LOG.info("Create/rename rotation detected for {}. Draining current file.", path);
                    metrics.getFilesRotated().increment();
                    drainCurrentFile();
                    completePendingAckSet();
                    break;
                case NO_ROTATION:
                default:
                    readFile();
                    break;
            }
        } catch (final RuntimeException e) {
            LOG.error("Error reading file {}", path, e);
            metrics.getReadErrors().increment();
        } finally {
            flushPartialLine();
            completePendingAckSet();
            onComplete.run();
        }
    }

    private void drainCurrentFile() {
        try (final FileChannel channel = fileOps.openReadChannel(path)) {
            metrics.getFilesOpened().increment();
            channel.position(readOffset.get());
            readLoop(channel, rotationDrainTimeout.toMillis(), true);
        } catch (final NoSuchFileException e) {
            LOG.warn("File already removed during drain: {}", path);
        } catch (final IOException e) {
            LOG.error("IO error draining file {}", path, e);
            metrics.getReadErrors().increment();
        } finally {
            metrics.getFilesClosed().increment();
        }
    }

    private void readFile() {
        try (final FileChannel channel = fileOps.openReadChannel(path)) {
            metrics.getFilesOpened().increment();
            channel.position(readOffset.get());
            readLoop(channel, maxReadTimePerFile.toMillis(), false);
            updateFileLagBytes();
        } catch (final NoSuchFileException e) {
            LOG.warn("File not found: {}", path);
        } catch (final IOException e) {
            LOG.error("IO error reading file {}", path, e);
            metrics.getReadErrors().increment();
        } finally {
            metrics.getFilesClosed().increment();
        }
    }

    private void readLoop(final FileChannel channel, final long timeoutMillis, final boolean isDraining) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(readBufferSize);
        final long loopStart = System.currentTimeMillis();
        final ByteArrayOutputStream codecAccumulator = codec != null ? new ByteArrayOutputStream() : null;
        long codecBytesAccumulated = 0;
        final CharsetDecoder decoder = codec == null ? encoding.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE) : null;
        final CharBuffer charBuffer = codec == null ? CharBuffer.allocate(readBufferSize) : null;
        final ByteBuffer decoderCarryover = codec == null ? ByteBuffer.allocate(8) : null;

        while (!Thread.currentThread().isInterrupted()) {
            final long elapsed = System.currentTimeMillis() - loopStart;
            if (elapsed >= timeoutMillis) {
                if (isDraining) {
                    long currentFileSize = 0;
                    try {
                        currentFileSize = channel.size();
                    } catch (final IOException ignored) {
                    }
                    if (readOffset.get() < currentFileSize) {
                        LOG.warn("Rotation drain timeout expired with unread data for {}. Potential data loss.", path);
                        metrics.getDataLossEvents().increment();
                    }
                } else {
                    LOG.debug("Max read time reached for file {}", path);
                }
                break;
            }

            byteBuffer.clear();
            if (decoderCarryover != null && decoderCarryover.position() > 0) {
                decoderCarryover.flip();
                byteBuffer.put(decoderCarryover);
                decoderCarryover.clear();
            }
            final int bytesRead = channel.read(byteBuffer);
            if (bytesRead <= 0 && byteBuffer.position() == 0) {
                break;
            }

            final int totalBytes = byteBuffer.position();
            metrics.getBytesRead().increment(Math.max(0, bytesRead));
            byteBuffer.flip();

            if (codec != null) {
                final byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                codecAccumulator.write(bytes);
                codecBytesAccumulated += bytes.length;
            } else {
                charBuffer.clear();
                final CoderResult result = decoder.decode(byteBuffer, charBuffer, false);
                if (result.isUnderflow() && byteBuffer.hasRemaining()) {
                    decoderCarryover.put(byteBuffer);
                }
                charBuffer.flip();
                if (charBuffer.hasRemaining()) {
                    processChunk(charBuffer.toString());
                }
            }

            readOffset.addAndGet(Math.max(0, bytesRead));
            if (codec == null) {
                checkpointEntry.setReadOffset(readOffset.get());
            }
            lastActivityMillis = System.currentTimeMillis();

            if (acknowledgementsEnabled && currentAckSet != null && currentBatchCount > 0) {
                final long batchAge = System.currentTimeMillis() - batchOpenedAtMillis;
                if (batchAge >= batchTimeout.toMillis()) {
                    completePendingAckSet();
                }
            }
        }

        if (codec != null && codecAccumulator.size() > 0) {
            parseWithCodec(codecAccumulator.toByteArray());
            checkpointEntry.setReadOffset(readOffset.get());
        }
    }

    private void updateFileLagBytes() {
        try {
            final long currentFileSize = fileOps.size(path);
            final long lag = currentFileSize - readOffset.get();
            metrics.getFileLagBytes().set(Math.max(0, lag));
        } catch (final IOException e) {
            LOG.debug("Unable to determine file size for lag calculation on {}", path);
        }
    }

    private void processChunk(final String chunk) {
        int start = 0;
        for (int i = 0; i < chunk.length(); i++) {
            if (chunk.charAt(i) == '\n') {
                if (skippingToNewline) {
                    skippingToNewline = false;
                    start = i + 1;
                    continue;
                }
                final String segment = chunk.substring(start, i);
                partialLine.append(segment);
                final String line = partialLine.length() > maxLineLength
                        ? partialLine.substring(0, maxLineLength)
                        : partialLine.toString();
                if (partialLine.length() > maxLineLength) {
                    metrics.getLinesTruncated().increment();
                }
                emitLine(line);
                partialLine.setLength(0);
                start = i + 1;
            }
        }
        if (!skippingToNewline && start < chunk.length()) {
            partialLine.append(chunk, start, chunk.length());
        }

        if (partialLine.length() > maxLineLength) {
            emitLine(partialLine.substring(0, maxLineLength));
            partialLine.setLength(0);
            skippingToNewline = true;
            metrics.getLinesTruncated().increment();
        }
    }

    private void flushPartialLine() {
        if (partialLine.length() > 0) {
            emitLine(partialLine.toString());
            partialLine.setLength(0);
        }
    }

    private void parseWithCodec(final byte[] bytes) {
        try {
            codec.parse(new ByteArrayInputStream(bytes), record -> {
                emitCodecRecord(record);
            });
        } catch (final IOException e) {
            LOG.error("Codec parse error for file {}", path, e);
            metrics.getReadErrors().increment();
        }
    }

    @SuppressWarnings("unchecked")
    private void emitCodecRecord(final Record<Event> record) {
        final Record<Object> objectRecord = (Record<Object>) (Record<?>) record;
        final Event event = record.getData();
        writeRecordWithRetry(objectRecord, event);
    }

    private void emitLine(final String line) {
        final Map<String, Object> data = new HashMap<>();
        data.put(MESSAGE_KEY, line);
        if (includeFileMetadata) {
            data.put(FILE_PATH_KEY, cachedAbsolutePath);
            data.put(FILE_IDENTITY_KEY, cachedFileIdentityString);
        }

        final Event event = eventFactory.eventBuilder(EventBuilder.class)
                .withEventType(EVENT_TYPE)
                .withData(data)
                .build();

        final Record<Object> record = new Record<>(event);
        writeRecordWithRetry(record, event);
    }

    private void writeRecordWithRetry(final Record<Object> record, final Event event) {
        boolean written = false;
        long backpressureStartNanos = 0;
        boolean backpressureActive = false;
        final long maxRetryMillis = maxReadTimePerFile.toMillis();
        final long retryStart = System.currentTimeMillis();
        while (!written && !Thread.currentThread().isInterrupted()) {
            if (System.currentTimeMillis() - retryStart > maxRetryMillis) {
                LOG.warn("Backpressure retry timeout exceeded for file {}. Event may be lost.", path);
                metrics.getDataLossEvents().increment();
                break;
            }
            try {
                buffer.write(record, writeTimeout);
                written = true;
                metrics.getLinesRead().increment();
                metrics.getEventsEmitted().increment();

                if (backpressureActive) {
                    final long backpressureElapsedNanos = System.nanoTime() - backpressureStartNanos;
                    metrics.getBackpressureTimer().record(backpressureElapsedNanos, TimeUnit.NANOSECONDS);
                }

                if (acknowledgementsEnabled && acknowledgementSetManager != null) {
                    ensureAckSet();
                    currentAckSet.add(event);
                    currentBatchCount++;
                    if (currentBatchCount >= batchSize) {
                        completePendingAckSet();
                    }
                }
            } catch (final TimeoutException e) {
                if (!backpressureActive) {
                    backpressureStartNanos = System.nanoTime();
                    backpressureActive = true;
                }
                metrics.getWriteTimeouts().increment();
                LOG.debug("Back pressure from buffer, retrying for file {}", path);
                try {
                    Thread.sleep(BACK_PRESSURE_SLEEP_MILLIS);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void ensureAckSet() {
        if (currentAckSet == null) {
            final long capturedBatchStart = readOffset.get();
            batchStartOffset = capturedBatchStart;
            batchOpenedAtMillis = System.currentTimeMillis();
            currentAckSet = acknowledgementSetManager.create(
                    result -> handleAcknowledgement(result, capturedBatchStart, readOffset.get()),
                    acknowledgmentTimeout);
        }
    }

    private void completePendingAckSet() {
        if (currentAckSet != null) {
            currentAckSet.complete();
            currentAckSet = null;
            currentBatchCount = 0;
        }
    }

    private void handleAcknowledgement(final boolean result, final long batchStart, final long batchEnd) {
        if (result) {
            acknowledgmentRetryCount.set(0);
            checkpointEntry.setCommittedOffset(batchEnd);
            LOG.debug("Positive acknowledgement for file {} offset range [{}, {}]", path, batchStart, batchEnd);
        } else {
            final int retryCount = acknowledgmentRetryCount.incrementAndGet();
            metrics.getAcknowledgmentFailures().increment();
            if (retryCount > maxAcknowledgmentRetries) {
                LOG.error("Exceeded max acknowledgment retries ({}) for file {} offset range [{}, {}]. Advancing offset to avoid infinite retry.",
                        maxAcknowledgmentRetries, path, batchStart, batchEnd);
                checkpointEntry.setCommittedOffset(batchEnd);
                acknowledgmentRetryCount.set(0);
            } else {
                LOG.warn("Negative acknowledgement for file {} offset range [{}, {}]. Retry {}/{}.",
                        path, batchStart, batchEnd, retryCount, maxAcknowledgmentRetries);
            }
        }
    }

    public FileIdentity getFileIdentity() {
        return fileIdentity;
    }

    public Path getPath() {
        return path;
    }

    public long getReadOffset() {
        return readOffset.get();
    }

    public RotationType getLastRotationType() {
        return lastRotationType;
    }

    public long getLastActivityMillis() {
        return lastActivityMillis;
    }
}
