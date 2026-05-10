/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.sns.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.dataprepper.metrics.MetricNames;
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
import java.util.List;
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

    private static final String FORCE_PATH_STYLE = "force_path_style";

    private String dlqFile;

    private String keyPathPrefix;

    private DlqProvider dlqProvider;

    private ObjectWriter objectWriter;

    public DlqPushHandler(final String dlqFile,
                          final PluginFactory pluginFactory,
                          final String bucket,
                          final String stsRoleArn,
                          final String awsRegion,
                          final Boolean forcePathStyle,
                          final String dlqPathPrefix) {
        if(dlqFile != null) {
            this.dlqFile = dlqFile;
            this.objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
        }else{
            this.dlqProvider = getDlqProvider(pluginFactory,bucket,stsRoleArn,awsRegion,forcePathStyle,dlqPathPrefix);
        }
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
            dlqFileWriter.write(objectWriter.writeValueAsString(failedData)+"\n");
        } catch (IOException e) {
            LOG.error("Exception while writing failed data to DLQ file Exception: ",e);
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
            final List<DlqObject> dlqObjects = Arrays.asList(dlqObject);
            dlqWriter.write(dlqObjects, pluginSetting.getPipelineName(), pluginId);
            LOG.info("wrote {} events to DLQ",dlqObjects.size());
        } catch (final IOException e) {
            LOG.error("Exception while writing failed data to DLQ, Exception : ", e);
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
                                        final Boolean forcePathStyle,
                                        final String dlqPathPrefix) {
        final Map<String, Object> props = new HashMap<>();
        props.put(BUCKET, bucket);
        props.put(ROLE_ARN, stsRoleArn);
        props.put(REGION, awsRegion);
        props.put(FORCE_PATH_STYLE, forcePathStyle);
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

