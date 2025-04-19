/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.dlq.s3;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

/**
 * S3 Dlq writer. Stores DLQ Objects in an S3 bucket.
 *
 * @since 2.2
 */
public class S3DlqWriter implements DlqWriter {

    static final String S3_DLQ_RECORDS_SUCCESS = "dlqS3RecordsSuccess";
    static final String S3_DLQ_RECORDS_FAILED = "dlqS3RecordsFailed";
    static final String S3_DLQ_REQUEST_SUCCESS = "dlqS3RequestSuccess";
    static final String S3_DLQ_REQUEST_FAILED = "dlqS3RequestFailed";
    static final String S3_DLQ_REQUEST_LATENCY = "dlqS3RequestLatency";
    static final String S3_DLQ_REQUEST_SIZE_BYTES = "dlqS3RequestSizeBytes";
    static final String DLQ_OBJECTS = "dlqObjects";
    private static final String KEY_NAME_FORMAT = "dlq-v%s-%s-%s-%s-%s.json";
    private static final String FULL_KEY_FORMAT = "%s%s";

    private static final Logger LOG = LoggerFactory.getLogger(S3DlqWriter.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String keyPathPrefix;

    private final String bucketOwner;
    private final ObjectMapper objectMapper;

    private final Counter dlqS3RecordsSuccessCounter;
    private final Counter dlqS3RecordsFailedCounter;
    private final Counter dlqS3RequestSuccessCounter;
    private final Counter dlqS3RequestFailedCounter;
    private final Timer dlqS3RequestTimer;
    private final DistributionSummary dlqS3RequestSizeBytesSummary;
    private final KeyPathGenerator keyPathGenerator;

    S3DlqWriter(final S3DlqWriterConfig s3DlqWriterConfig, final ObjectMapper objectMapper, final PluginMetrics pluginMetrics) {
        dlqS3RecordsSuccessCounter = pluginMetrics.counter(S3_DLQ_RECORDS_SUCCESS);
        dlqS3RecordsFailedCounter = pluginMetrics.counter(S3_DLQ_RECORDS_FAILED);
        dlqS3RequestSuccessCounter = pluginMetrics.counter(S3_DLQ_REQUEST_SUCCESS);
        dlqS3RequestFailedCounter = pluginMetrics.counter(S3_DLQ_REQUEST_FAILED);
        dlqS3RequestTimer = pluginMetrics.timer(S3_DLQ_REQUEST_LATENCY);
        dlqS3RequestSizeBytesSummary = pluginMetrics.summary(S3_DLQ_REQUEST_SIZE_BYTES);

        this.s3Client = s3DlqWriterConfig.getS3Client();
        Objects.requireNonNull(s3DlqWriterConfig.getBucket());
        this.bucket = s3DlqWriterConfig.getBucket();
        this.keyPathPrefix = StringUtils.isEmpty(s3DlqWriterConfig.getKeyPathPrefix()) ? s3DlqWriterConfig.getKeyPathPrefix() :
            enforceDefaultDelimiterOnKeyPathPrefix(s3DlqWriterConfig.getKeyPathPrefix());
        this.objectMapper = objectMapper;
        this.keyPathGenerator = new KeyPathGenerator(keyPathPrefix);
        this.bucketOwner = s3DlqWriterConfig.getBucketOwner();
    }

    @Override
    public void write(final List<DlqObject> dlqObjects, final String pipelineName, final String pluginId) throws IOException {
        if(dlqObjects.isEmpty()) {
            return;
        }

        try {
            doWrite(dlqObjects, pipelineName, pluginId);
            dlqS3RequestSuccessCounter.increment();
            dlqS3RecordsSuccessCounter.increment(dlqObjects.size());
        } catch (final Exception e) {
            dlqS3RequestFailedCounter.increment();
            dlqS3RecordsFailedCounter.increment(dlqObjects.size());
            throw e;
        }
    }

    private void doWrite(final List<DlqObject> dlqObjects, final String pipelineName, final String pluginId) throws IOException {
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(bucket)
            .expectedBucketOwner(bucketOwner)
            .key(buildKey(pipelineName, pluginId))
            .build();

        final String content = deserialize(dlqObjects);

        final PutObjectResponse response = timedPutObject(putObjectRequest, content);

        if (!response.sdkHttpResponse().isSuccessful()) {
            LOG.error(SENSITIVE, "Failed to write content [{}] to S3 dlq", content);
            LOG.error("Failed to write to S3 dlq due to status code: [{}]", response.sdkHttpResponse().statusCode());
            throw new IOException(String.format(
                "Failed to write to S3 dlq due to status code: %d", response.sdkHttpResponse().statusCode()));
        }
    }

    private PutObjectResponse timedPutObject(final PutObjectRequest putObjectRequest, final String content) throws IOException {
        try {
            return dlqS3RequestTimer.recordCallable(() -> putObject(putObjectRequest, content));
        } catch (final IOException ioException) {
            throw ioException;
        } catch (final Exception ex) {
            LOG.error(SENSITIVE, "Failed timed write to S3 dlq with content: [{}]", content, ex);
            throw new IOException("Failed timed write to S3 dlq.", ex);
        }
    }

    private PutObjectResponse putObject(final PutObjectRequest request, final String content) throws IOException {
        try {
            return s3Client.putObject(request, RequestBody.fromString(content));
        } catch (Exception ex) {
            LOG.error(SENSITIVE, "Failed to write content [{}] to S3 dlq", content, ex);
            throw new IOException("Failed to write to S3 dlq.", ex);
        }
    }

    private String deserialize(final List<DlqObject> dlqObjects) throws IOException {
        try {
            final Map<String, Object> output = Map.of(DLQ_OBJECTS, dlqObjects);

            final String content = objectMapper.writeValueAsString(output);

            dlqS3RequestSizeBytesSummary.record(content.getBytes(StandardCharsets.UTF_8).length);

            return content;
        } catch (JsonProcessingException e) {
            LOG.error(SENSITIVE, "Failed to build valid S3 request body with dlqObjects: [{}]", dlqObjects, e);
            throw new IOException("Failed to build valid S3 request body", e);
        }
    }

    private String buildKey(final String pipelineName, final String pluginId) {
        final String key = String.format(KEY_NAME_FORMAT, DataPrepperVersion.getCurrentVersion().getMajorVersion(),
            pipelineName, pluginId, Instant.now(), UUID.randomUUID());
        return keyPathPrefix == null ? key : String.format(FULL_KEY_FORMAT, keyPathGenerator.generate(), key);
    }

    private String enforceDefaultDelimiterOnKeyPathPrefix(final String keyPathPrefix) {
        return (keyPathPrefix.charAt(keyPathPrefix.length() - 1) == '/') ? keyPathPrefix : keyPathPrefix.concat("/");
    }

    @Override
    public void close() throws IOException {
        s3Client.close();
    }
}
