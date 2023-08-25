/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.UUID.randomUUID;


/**
 * * An Handler class which helps log failed data to AWS S3 bucket or file based on configuration.
 */

public class DlqPushHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DlqPushHandler.class);

    private static final String BUCKET = "bucket";

    private static final String ROLE_ARN = "sts_role_arn";

    private static final String REGION = "region";

    private static final String S3_PLUGIN_NAME = "s3";

    private static final String KEY_PATH_PREFIX = "key_path_prefix";

    private String dlqFile;

    private String keyPathPrefix;

    private DlqProvider dlqProvider;

    static final String S3_DLQ_RECORDS_SUCCESS = "dlqS3RecordsSuccess";
    static final String S3_DLQ_RECORDS_FAILED = "dlqS3RecordsFailed";
    static final String S3_DLQ_REQUEST_SUCCESS = "dlqS3RequestSuccess";
    static final String S3_DLQ_REQUEST_FAILED = "dlqS3RequestFailed";

    static final String FILE_DLQ_RECORDS_SUCCESS = "dlqFileRecordsSuccess";
    static final String FILE_DLQ_RECORDS_FAILED = "dlqFileRecordsFailed";
    static final String FILE_DLQ_REQUEST_SUCCESS = "dlqFileRequestSuccess";
    static final String FILE_DLQ_REQUEST_FAILED = "dlqFileRequestFailed";

    private final Counter dlqS3RecordsSuccessCounter;
    private final Counter dlqS3RecordsFailedCounter;
    private final Counter dlqS3RequestSuccessCounter;
    private final Counter dlqS3RequestFailedCounter;

    private final Counter dlqFileRecordsSuccessCounter;
    private final Counter dlqFileRecordsFailedCounter;
    private final Counter dlqFileRequestSuccessCounter;
    private final Counter dlqFileRequestFailedCounter;

    public DlqPushHandler(final String dlqFile,
                          final PluginFactory pluginFactory,
                          final String bucket,
                          final String stsRoleArn,
                          final String awsRegion,
                          final String dlqPathPrefix,
                          final PluginMetrics pluginMetrics) {
        this.dlqFile = dlqFile;
        this.dlqProvider = getDlqProvider(pluginFactory,bucket,stsRoleArn,awsRegion,dlqPathPrefix);
        dlqS3RecordsSuccessCounter = pluginMetrics.counter(S3_DLQ_RECORDS_SUCCESS);
        dlqS3RecordsFailedCounter = pluginMetrics.counter(S3_DLQ_RECORDS_FAILED);
        dlqS3RequestSuccessCounter = pluginMetrics.counter(S3_DLQ_REQUEST_SUCCESS);
        dlqS3RequestFailedCounter = pluginMetrics.counter(S3_DLQ_REQUEST_FAILED);
        dlqFileRecordsSuccessCounter = pluginMetrics.counter(FILE_DLQ_RECORDS_SUCCESS);
        dlqFileRecordsFailedCounter = pluginMetrics.counter(FILE_DLQ_RECORDS_FAILED);
        dlqFileRequestSuccessCounter = pluginMetrics.counter(FILE_DLQ_REQUEST_SUCCESS);
        dlqFileRequestFailedCounter = pluginMetrics.counter(FILE_DLQ_REQUEST_FAILED);
    }

    public void perform(final PluginSetting pluginSetting,
                        final Object failedData) {
        if(dlqFile != null)
            writeToFile(failedData);
        else
            pushToS3(pluginSetting, failedData);
    }

    private void writeToFile(Object failedData) {
        try(BufferedWriter dlqFileWriter = Files.newBufferedWriter(Paths.get(dlqFile),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            dlqFileWriter.write(new ObjectMapper().writer().writeValueAsString(failedData)+"\n");
            dlqFileRequestSuccessCounter.increment();
            dlqFileRecordsSuccessCounter.increment();
        } catch (IOException e) {
            LOG.error("Exception while writing failed data to DLQ file Exception: ",e);
            dlqFileRequestFailedCounter.increment();
            dlqFileRecordsFailedCounter.increment();
        }
    }

    private void pushToS3(PluginSetting pluginSetting, Object failedData) {
        DlqWriter dlqWriter = getDlqWriter(pluginSetting.getPipelineName());
        try {
            String pluginId = randomUUID().toString();
            DlqObject dlqObject = DlqObject.builder()
                    .withPluginId(pluginId)
                    .withPluginName(pluginSetting.getName())
                    .withPipelineName(pluginSetting.getPipelineName())
                    .withFailedData(failedData)
                    .build();

            dlqWriter.write(Arrays.asList(dlqObject), pluginSetting.getPipelineName(), pluginId);
            dlqS3RequestSuccessCounter.increment();
            dlqS3RecordsSuccessCounter.increment();
        } catch (final IOException e) {
            LOG.error("Exception while writing failed data to DLQ, Exception : ", e);
            dlqS3RequestFailedCounter.increment();
            dlqS3RecordsFailedCounter.increment();
        }
    }

    private  DlqWriter getDlqWriter(final String writerPipelineName) {
        Optional<DlqWriter> potentialDlq = dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
                .add(writerPipelineName).toString());
        DlqWriter dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
        return dlqWriter;
    }

    private  DlqProvider getDlqProvider(final PluginFactory pluginFactory,
                                        final String bucket,
                                        final String stsRoleArn,
                                        final String awsRegion,
                                        final String dlqPathPrefix) {
        final Map<String, Object> props = new HashMap<>();
        props.put(BUCKET, bucket);
        props.put(ROLE_ARN, stsRoleArn);
        props.put(REGION, awsRegion);
        this.keyPathPrefix = StringUtils.isEmpty(dlqPathPrefix) ? dlqPathPrefix : enforceDefaultDelimiterOnKeyPathPrefix(dlqPathPrefix);
        props.put(KEY_PATH_PREFIX, dlqPathPrefix);
        final PluginSetting dlqPluginSetting = new PluginSetting(S3_PLUGIN_NAME, props);
        DlqProvider dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
        return dlqProvider;
    }

    private String enforceDefaultDelimiterOnKeyPathPrefix(final String keyPathPrefix) {
        return (keyPathPrefix.charAt(keyPathPrefix.length() - 1) == '/') ? keyPathPrefix : keyPathPrefix.concat("/");
    }
}

