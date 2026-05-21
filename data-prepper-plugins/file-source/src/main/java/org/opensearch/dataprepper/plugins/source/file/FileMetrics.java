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

public final class FileMetrics {

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
    private final Counter truncationEvents;
    private final Timer backpressureTimer;
    private final AtomicLong activeFileCount;
    private final AtomicLong fileLagBytes;

    public FileMetrics(final PluginMetrics pluginMetrics) {
        this.linesRead = pluginMetrics.counter("linesRead");
        this.bytesRead = pluginMetrics.counter("bytesRead");
        this.linesTruncated = pluginMetrics.counter("linesTruncated");
        this.filesOpened = pluginMetrics.counter("filesOpened");
        this.filesClosed = pluginMetrics.counter("filesClosed");
        this.filesRotated = pluginMetrics.counter("filesRotated");
        this.readErrors = pluginMetrics.counter("readErrors");
        this.writeTimeouts = pluginMetrics.counter("writeTimeouts");
        this.checkpointFlushes = pluginMetrics.counter("checkpointFlushes");
        this.checkpointErrors = pluginMetrics.counter("checkpointErrors");
        this.eventsEmitted = pluginMetrics.counter("eventsEmitted");
        this.dataLossEvents = pluginMetrics.counter("dataLossEvents");
        this.acknowledgmentFailures = pluginMetrics.counter("acknowledgmentFailures");
        this.truncationEvents = pluginMetrics.counter("truncationEvents");
        this.backpressureTimer = pluginMetrics.timer("backpressureTime");
        this.activeFileCount = new AtomicLong(0);
        pluginMetrics.gauge("activeFiles", activeFileCount);
        this.fileLagBytes = new AtomicLong(0);
        pluginMetrics.gauge("fileLagBytes", fileLagBytes);
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

    public Counter getTruncationEvents() {
        return truncationEvents;
    }
}
