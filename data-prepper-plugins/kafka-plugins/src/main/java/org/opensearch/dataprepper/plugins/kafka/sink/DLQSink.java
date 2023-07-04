/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.kafka.sink;

import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;


import static java.util.UUID.randomUUID;


/**
 * *This class which helps log failed data to AWS S3 bucket
 */

public class DLQSink {

    private static final Logger LOG = LoggerFactory.getLogger(DLQSink.class);

    private static final String BUCKET = "bucket";
    private static final String ROLE_ARN = "sts_role_arn";
    private static final String REGION = "region";
    private static final String S3_PLUGIN_NAME = "s3";
    private final DlqProvider dlqProvider;
    final PluginSetting pluginSetting;

    public DLQSink(final PluginFactory pluginFactory, final KafkaSinkConfig kafkaSinkConfig,final PluginSetting pluginSetting) {
         this.dlqProvider = getDlqProvider(pluginFactory, kafkaSinkConfig);
         this.pluginSetting=pluginSetting;

    }

    public  void perform(final Object failedData) {
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
        } catch (final IOException io) {
            LOG.error("Error occured while performing DLQ operation ",io);
        }
    }

    private  DlqWriter getDlqWriter( final String writerPipelineName) {
        Optional<DlqWriter> potentialDlq = dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
                .add(writerPipelineName).toString());
        DlqWriter dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
        return dlqWriter;
    }

    private  DlqProvider getDlqProvider(final PluginFactory pluginFactory, final KafkaSinkConfig kafkaSinkConfig) {
        final Map<String, Object> props = new HashMap<>();
        props.put(BUCKET, kafkaSinkConfig.getDlqConfig().getBucket());
        props.put(ROLE_ARN, kafkaSinkConfig.getDlqConfig().getRoleArn());
        props.put(REGION, kafkaSinkConfig.getDlqConfig().getRegion());
        final PluginSetting dlqPluginSetting = new PluginSetting(S3_PLUGIN_NAME, props);
        DlqProvider dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
        return dlqProvider;
    }
}

