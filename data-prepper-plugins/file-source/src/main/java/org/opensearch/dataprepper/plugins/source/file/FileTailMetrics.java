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
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.concurrent.atomic.AtomicLong;

public final class FileTailMetrics {

    private final Counter linesRead;
    private final Counter bytesRead;
    private final Counter linesTruncated;
    private final Counter filesOpened;
    private final Counter filesClosed;
    private final Counter filesRotated;
    private final Counter readErrors;
    private final Counter writeTimeouts;
    private final Counter checkpointFlushes;
    private final Counter checkpointErrors;
    private final Counter eventsEmitted;
    private final Counter dataLossEvents;
    private final Counter acknowledgmentFailures;
    private final Timer backpressureTimer;
    private final AtomicLong activeFileCount;
    private final AtomicLong fileLagBytes;

    public FileTailMetrics(final PluginMetrics pluginMetrics) {
        this.linesRead = pluginMetrics.counter("tailLinesRead");
        this.bytesRead = pluginMetrics.counter("tailBytesRead");
        this.linesTruncated = pluginMetrics.counter("tailLinesTruncated");
        this.filesOpened = pluginMetrics.counter("tailFilesOpened");
        this.filesClosed = pluginMetrics.counter("tailFilesClosed");
        this.filesRotated = pluginMetrics.counter("tailFilesRotated");
        this.readErrors = pluginMetrics.counter("tailReadErrors");
        this.writeTimeouts = pluginMetrics.counter("tailWriteTimeouts");
        this.checkpointFlushes = pluginMetrics.counter("tailCheckpointFlushes");
        this.checkpointErrors = pluginMetrics.counter("tailCheckpointErrors");
        this.eventsEmitted = pluginMetrics.counter("tailEventsEmitted");
        this.dataLossEvents = pluginMetrics.counter("tailDataLossEvents");
        this.acknowledgmentFailures = pluginMetrics.counter("tailAcknowledgmentFailures");
        this.backpressureTimer = pluginMetrics.timer("tailBackpressureTime");
        this.activeFileCount = new AtomicLong(0);
        pluginMetrics.gauge("tailActiveFiles", activeFileCount);
        this.fileLagBytes = new AtomicLong(0);
        pluginMetrics.gauge("tailFileLagBytes", fileLagBytes);
    }

    public Counter getLinesRead() {
        return linesRead;
    }

    public Counter getBytesRead() {
        return bytesRead;
    }

    public Counter getLinesTruncated() {
        return linesTruncated;
    }

    public Counter getFilesOpened() {
        return filesOpened;
    }

    public Counter getFilesClosed() {
        return filesClosed;
    }

    public Counter getFilesRotated() {
        return filesRotated;
    }

    public Counter getReadErrors() {
        return readErrors;
    }

    public Counter getWriteTimeouts() {
        return writeTimeouts;
    }

    public Counter getCheckpointFlushes() {
        return checkpointFlushes;
    }

    public Counter getCheckpointErrors() {
        return checkpointErrors;
    }

    public AtomicLong getActiveFileCount() {
        return activeFileCount;
    }

    public Counter getEventsEmitted() {
        return eventsEmitted;
    }

    public Timer getBackpressureTimer() {
        return backpressureTimer;
    }

    public AtomicLong getFileLagBytes() {
        return fileLagBytes;
    }

    public Counter getDataLossEvents() {
        return dataLossEvents;
    }

    public Counter getAcknowledgmentFailures() {
        return acknowledgmentFailures;
    }
}
