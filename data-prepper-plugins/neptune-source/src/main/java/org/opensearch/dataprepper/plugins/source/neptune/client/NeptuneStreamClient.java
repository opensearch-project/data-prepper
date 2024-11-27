/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.neptune.client;

import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.neptunedata.model.StreamRecordsNotFoundException;

import java.time.Duration;
import java.util.List;

public class NeptuneStreamClient {
    private static final Logger LOG = LoggerFactory.getLogger(NeptuneStreamClient.class);
    private static final long MAX_BACKOFF_TIME = 60;
    private final NeptuneDataClientWrapper dataClient;
    private final NeptuneStreamEventListener listener;

    @Getter
    private StreamPosition streamPositionInfo;
    private long retryCount;

    public NeptuneStreamClient(final NeptuneSourceConfig config, final int batchSize, final NeptuneStreamEventListener listener) {
        this.dataClient = new NeptuneDataClientWrapper(config, batchSize);
        this.listener = listener;
        this.streamPositionInfo = StreamPosition.empty();
        this.retryCount = 0;
    }


    public void setStreamPosition(final long commitNum, final long opNum) {
        streamPositionInfo = new StreamPosition(commitNum, opNum);
    }

    public void start() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted() && !listener.shouldStopNeptuneStream(streamPositionInfo)) {
            try {
                final List<NeptuneStreamRecord> streamRecords =
                        this.dataClient.getStreamRecords(streamPositionInfo.getCommitNum(), streamPositionInfo.getOpNum());
                retryCount = 0;
                if (!streamRecords.isEmpty()) {
                    final NeptuneStreamRecord<?> lastRecord = streamRecords.get(streamRecords.size() - 1);
                    setStreamPosition(lastRecord.getCommitNum(), lastRecord.getOpNum());
                }
                listener.onNeptuneStreamEvents(streamRecords, streamPositionInfo);
            } catch (final StreamRecordsNotFoundException exception) {
                final long nextBackoff = getNextBackoff();
                LOG.info("Stream is up-to-date, Sleeping for {} seconds before retrying again.", nextBackoff);
                Thread.sleep(Duration.ofSeconds(nextBackoff).toMillis());
            } catch (final Exception exception) {
                if (!listener.onNeptuneStreamException(exception, streamPositionInfo)) {
                    break;
                }
            }
        }
    }

    private long getNextBackoff() {
        final long nextBackoff = (long) Math.pow(2.0f, retryCount++);
        return Math.min(MAX_BACKOFF_TIME, nextBackoff);
    }
}
