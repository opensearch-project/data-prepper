package org.opensearch.dataprepper.plugins.source.neptune.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;

import org.opensearch.dataprepper.plugins.source.neptune.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.source.neptune.client.NeptuneConnection;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.neptune.model.S3PartitionStatus;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StreamWorker {
    public static final String STREAM_PREFIX = "STREAM-";
    private static final Logger LOG = LoggerFactory.getLogger(StreamWorker.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static final String SUCCESS_ITEM_COUNTER_NAME = "changeEventsProcessed";
    static final String FAILURE_ITEM_COUNTER_NAME = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";
    private static final int BUFFER_WRITE_TIMEOUT_MILLIS = 15_000;
    private static final int WAIT_MILLIS = 10_000;

    private static final int STREAM_RECORDS_BATCH_SIZE = 10_000;

    private final NeptuneSourceConfig sourceConfig;
    private final RecordBufferWriter recordBufferWriter;
    private final StreamRecordConverter streamRecordConverter;
    private final DataStreamPartitionCheckpoint partitionCheckpoint;

    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    private final StreamAcknowledgementManager streamAcknowledgementManager;
    private final PluginMetrics pluginMetrics;
    private final int recordFlushBatchSize;
    private final int checkPointIntervalInMs;
    private final int bufferWriteIntervalInMs;
    private final int streamBatchSize;
    private boolean stopWorker = false;
    private final ExecutorService executorService;

    Optional<S3PartitionStatus> s3PartitionStatus = Optional.empty();

    final List<Event> records = new ArrayList<>();
    final List<Long> recordBytes = new ArrayList<>();
    long lastBufferWriteTime = System.currentTimeMillis();

    private Long checkPointCommitNum = null;
    private Long checkPointOpNum = null;
    private Long recordCount = null;
    private long lastLocalCheckpointCommitNum = 0;
    private long lastLocalCheckpointOpNum = 0;
    private long lastLocalRecordCount = 0;

    private final Lock lock;

    public static StreamWorker create(final RecordBufferWriter recordBufferWriter,
                                      final StreamRecordConverter recordConverter,
                                      final NeptuneSourceConfig sourceConfig,
                                      final StreamAcknowledgementManager streamAcknowledgementManager,
                                      final DataStreamPartitionCheckpoint partitionCheckpoint,
                                      final PluginMetrics pluginMetrics,
                                      final int recordFlushBatchSize,
                                      final int checkPointIntervalInMs,
                                      final int bufferWriteIntervalInMs,
                                      final int streamBatchSize
                                      // final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics
    ) {
        return new StreamWorker(recordBufferWriter, recordConverter, sourceConfig, streamAcknowledgementManager, partitionCheckpoint,
                pluginMetrics, recordFlushBatchSize, checkPointIntervalInMs, bufferWriteIntervalInMs, streamBatchSize);
    }

    public StreamWorker(final RecordBufferWriter recordBufferWriter,
                        final StreamRecordConverter streamRecordConverter,
                        final NeptuneSourceConfig sourceConfig,
                        final StreamAcknowledgementManager streamAcknowledgementManager,
                        final DataStreamPartitionCheckpoint partitionCheckpoint,
                        final PluginMetrics pluginMetrics,
                        final int recordFlushBatchSize,
                        final int checkPointIntervalInMs,
                        final int bufferWriteIntervalInMs,
                        final int streamBatchSize
    ) {
        this.recordBufferWriter = recordBufferWriter;
        this.streamRecordConverter = streamRecordConverter;
        this.sourceConfig = sourceConfig;
        this.streamAcknowledgementManager = streamAcknowledgementManager;
        this.partitionCheckpoint = partitionCheckpoint;
        this.pluginMetrics = pluginMetrics;
        this.recordFlushBatchSize = recordFlushBatchSize;
        this.checkPointIntervalInMs = checkPointIntervalInMs;
        this.bufferWriteIntervalInMs = bufferWriteIntervalInMs;
        this.streamBatchSize = streamBatchSize;
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
        this.bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        this.bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        this.lock = new ReentrantLock();
        // this.documentDBAggregateMetrics = documentDBAggregateMetrics;

        if (sourceConfig.isAcknowledgmentsEnabled()) {
            // starts acknowledgement monitoring thread
            streamAcknowledgementManager.init((Void) -> stop());
        }
        // buffer write and checkpoint in separate thread on timeout
        this.executorService = Executors.newSingleThreadExecutor(BackgroundThreadFactory.defaultExecutorThreadFactory("neptune-stream-checkpoint"));
        this.executorService.submit(this::bufferWriteAndCheckpointStream);
    }

    private boolean shouldWaitForS3Partition() {
        s3PartitionStatus = partitionCheckpoint.getGlobalS3FolderCreationStatus();
        return s3PartitionStatus.isEmpty();
    }

    public void processStream(final StreamPartition streamPartition) {
        // documentDBAggregateMetrics.getStreamApiInvocations().increment();

        while (shouldWaitForS3Partition() && !Thread.currentThread().isInterrupted()) {
            LOG.info("S3 partitions are not ready, waiting for them to be complete before resuming streams.");
            try {
                Thread.sleep(WAIT_MILLIS);
            } catch (final InterruptedException ex) {
                LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                Thread.currentThread().interrupt();
                break;
            }
        }

        final List<String> s3Partitions = s3PartitionStatus.get().getPartitions();
        if (s3Partitions.isEmpty()) {
            // This should not happen unless the S3 partition creator failed.
            // documentDBAggregateMetrics.getStream5xxErrors().increment();
            throw new IllegalStateException("S3 partitions are not created. Please check the S3 partition creator thread.");
        }
        streamRecordConverter.initializePartitions(s3Partitions);
        LOG.info("Starting to watch streams for change events.");
        setCheckpointInformation(streamPartition);

        final HttpClient httpClient = NeptuneConnection.getHttpClient(sourceConfig);
        HttpResponse response;
        while (!Thread.currentThread().isInterrupted() && !stopWorker) {
            try {
                 response = httpClient.execute(new HttpGet(getStreamEndpoint(sourceConfig, checkPointCommitNum, checkPointOpNum)));
            } catch (Exception e) {
                if (e.getMessage().contains("Connection refused")) {
                    // Due to local SSH tunnel disconnection, try again
                    // FIXME: clean up, this is for local testing only
                    continue;
                } else {
                    LOG.info("Error fetching stream data, stopping processing");
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            final Map<String, Object> responseData = parseResponse(response);
            if (responseData == null) {
                LOG.warn("The change stream cursor didn't return any document. Stopping the change stream. New thread should restart the stream.");
                stop();
                partitionCheckpoint.resetCheckpoint();
            }
            if (response.getStatusLine().getStatusCode() != 200) {
                final String errorCode = responseData.get("code").toString();
                switch (errorCode) {
                    case "ExpiredStreamException":
                        // Reset checkpoint and start from TRIM_HORIZON iteration
                        checkPointCommitNum = 0L;
                        checkPointOpNum = 0L;
                        continue;
                    case "InvalidParameterException":
                    case "StreamRecordsNotFoundException":
                        LOG.warn("The change stream cursor didn't return any document. Stopping the change stream. New thread should restart the stream.");
                        stop();
                        partitionCheckpoint.resetCheckpoint();
                        continue;
                }
            }

            // TODO: handle stream response size limit
            // There is also a size limit of 10 MB on the response that can't be modified and that takes precedence over the number of
            // records specified in the limit parameter. The response does include a threshold-breaching record if the 10 MB limit was reached.

            final List<Object> recordList = (List<Object>) responseData.get("records");
            for (int i = 0; i < recordList.size(); i++) {
                final StreamRecord record = parseStreamRecord((Map<String, Object>) recordList.get(i));
                final Event event = streamRecordConverter.convert(record);
                records.add(event);
                // recordBytes.add(bytes);

                lock.lock();
                try {
                    recordCount += 1;
                    checkPointCommitNum = record.getEventId().getCommitNum();
                    checkPointOpNum = record.getEventId().getOpNum();
                    LOG.info("Process stream record - commitNum {}, opNum {}", checkPointCommitNum, checkPointOpNum);

                    if ((recordCount % recordFlushBatchSize == 0) || (System.currentTimeMillis() - lastBufferWriteTime >= bufferWriteIntervalInMs)) {
                        writeToBuffer();
                    }
                } catch (Exception e) {
                    // this will only happen if writing to buffer gets interrupted from shutdown,
                    // otherwise it's infinite backoff and retry
                    LOG.error("Failed to add records to buffer with error", e);
                    failureItemsCounter.increment(records.size());
                } finally {
                    lock.unlock();
                }
            }
        }

        if (!records.isEmpty()) {
            LOG.info("Flushing and checkpointing last processed record batch from the stream before terminating");
            writeToBuffer(records, checkPointCommitNum, checkPointOpNum, recordCount);
        }
        // Do final checkpoint.
        if (!sourceConfig.isAcknowledgmentsEnabled()) {
            partitionCheckpoint.checkpoint(checkPointCommitNum, checkPointOpNum, recordCount);
        }

        // System.clearProperty(STOP_S3_SCAN_PROCESSING_PROPERTY);
        // stop other threads for this worker
        stop();

        partitionCheckpoint.giveUpPartition();

        // shutdown acknowledgement monitoring thread
        if (streamAcknowledgementManager != null) {
            streamAcknowledgementManager.shutdown();
        }
    }

    private Map<String, Object> parseResponse(final HttpResponse response) {
        final BufferedReader bufferedReader;
        StringBuffer responseBuffer = new StringBuffer();
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                responseBuffer.append(line);
            }
        } catch (IOException e) {
            LOG.error("Error reading response from stream", e);
            return null;
        }

        try {
            return objectMapper.readValue(responseBuffer.toString(), Map.class);
        } catch (final JsonProcessingException e) {
            LOG.error("Error converting json data into map");
            return null;
        }
    }

    private StreamRecord parseStreamRecord(final Map<String, Object> rawRecord) {
        return objectMapper.convertValue(rawRecord, StreamRecord.class);
    }

    private void setCheckpointInformation(final StreamPartition streamPartition) {
        Optional<Long> commitNum = streamPartition.getProgressState().map(StreamProgressState::getCommitNum);
        commitNum.ifPresent(num -> checkPointCommitNum = num);
        Optional<Long> opNum = streamPartition.getProgressState().map(StreamProgressState::getOpNum);
        opNum.ifPresent(num -> checkPointOpNum = num);
        Optional<Long> loadedRecords = streamPartition.getProgressState().map(StreamProgressState::getLoadedRecords);
        loadedRecords.ifPresent(count -> recordCount = count);
    }

    private String getStreamEndpoint(final NeptuneSourceConfig sourceConfig, final long commitNum, final long opNum) {
        final String baseUri = String.format("https://%s:%s/%s/stream", sourceConfig.getHost(), sourceConfig.getPort(), sourceConfig.getStreamType());
        String iteratorType = "AFTER_SEQUENCE_NUMBER"; // DEFAULT
        if (commitNum == 0L && opNum == 0L) {
            iteratorType = "TRIM_HORIZON";
            return baseUri + String.format("?iteratorType=%s&limit=%s", iteratorType, STREAM_RECORDS_BATCH_SIZE);
        }
        return baseUri + String.format("?iteratorType=%s&commitNum=%s&opNum=%s&limit=%s", iteratorType, commitNum, opNum, STREAM_RECORDS_BATCH_SIZE);
    }

    private void writeToBuffer(final List<Event> records, final long commitNum, final long opNum, final long recordCount) {
        final AcknowledgementSet acknowledgementSet = streamAcknowledgementManager
                .createAcknowledgementSet(commitNum, opNum, recordCount).orElse(null);
        recordBufferWriter.writeToBuffer(acknowledgementSet, records);
        successItemsCounter.increment(records.size());
        if (acknowledgementSet != null) {
            acknowledgementSet.complete();
        }
    }

    private void writeToBuffer() {
        LOG.debug("Write to buffer for line {} to {}", lastLocalRecordCount, recordCount);
        writeToBuffer(records, checkPointCommitNum, checkPointOpNum, recordCount);
        lastLocalCheckpointCommitNum = checkPointCommitNum;
        lastLocalCheckpointOpNum = checkPointOpNum;
        lastLocalRecordCount = recordCount;
        lastBufferWriteTime = System.currentTimeMillis();
        bytesProcessedSummary.record(recordBytes.stream().mapToLong(Long::longValue).sum());
        records.clear();
        recordBytes.clear();
    }

    private void bufferWriteAndCheckpointStream() {
        long lastCheckpointTime = System.currentTimeMillis();
        while (!Thread.currentThread().isInterrupted() && !stopWorker) {
            if (!records.isEmpty() && lastBufferWriteTime < Instant.now().minusMillis(BUFFER_WRITE_TIMEOUT_MILLIS).toEpochMilli()) {
                lock.lock();
                LOG.debug("Writing to buffer due to buffer write delay");
                try {
                    writeToBuffer();
                } catch(Exception e){
                    // this will only happen if writing to buffer gets interrupted from shutdown,
                    // otherwise it's infinite backoff and retry
                    LOG.error("Failed to add records to buffer with error", e);
                    failureItemsCounter.increment(records.size());
                } finally {
                    lock.unlock();
                }
            }

            if (!sourceConfig.isAcknowledgmentsEnabled()) {
                if (System.currentTimeMillis() - lastCheckpointTime >= checkPointIntervalInMs) {
                    try {
                        lock.lock();
                        LOG.debug("Perform regular checkpoint for commitNum {} and opNum {} at record count {}", lastLocalCheckpointCommitNum, lastLocalCheckpointOpNum, lastLocalRecordCount);
                        partitionCheckpoint.checkpoint(lastLocalCheckpointCommitNum, lastLocalCheckpointOpNum, lastLocalRecordCount);
                    } catch (Exception e) {
                        LOG.warn("Exception checkpointing the current state. The stream record processing will start from previous checkpoint.", e);
                        stop();
                    } finally {
                        lock.unlock();;
                    }
                    lastCheckpointTime = System.currentTimeMillis();
                }
            }

            try {
                Thread.sleep(BUFFER_WRITE_TIMEOUT_MILLIS);
            } catch (InterruptedException ex) {
                break;
            }
        }
        LOG.info("Checkpoint monitoring thread interrupted.");
    }

    void stop() {
        stopWorker = true;
    }
}
