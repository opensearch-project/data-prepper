/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.kafka.sink;

import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.UUID.randomUUID;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;


/**
 * *This class which helps log failed data to AWS S3 bucket
 */

public class DLQSink {

    private static final Logger LOG = LoggerFactory.getLogger(DLQSink.class);

    private final DlqProvider dlqProvider;
    private final PluginSetting pluginSetting;

    public DLQSink(final PluginFactory pluginFactory, final KafkaSinkConfig kafkaSinkConfig, final PluginSetting pluginSetting) {
        this.pluginSetting = pluginSetting;
        this.dlqProvider = getDlqProvider(pluginFactory, kafkaSinkConfig);
    }

    public void perform(final Object failedData, final Exception e) {
        final DlqWriter dlqWriter = getDlqWriter();
        final DlqObject dlqObject = DlqObject.builder()
                .withPluginId(randomUUID().toString())
                .withPluginName(pluginSetting.getName())
                .withPipelineName(pluginSetting.getPipelineName())
                .withFailedData(failedData)
                .build();
        logFailureForDlqObjects(dlqWriter, List.of(dlqObject));
    }

    private DlqWriter getDlqWriter() {
        final Optional<DlqWriter> potentialDlq = dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
                .add(pluginSetting.getPipelineName())
                .add(pluginSetting.getName()).toString());
        final DlqWriter dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
        return dlqWriter;
    }

    private DlqProvider getDlqProvider(final PluginFactory pluginFactory, final KafkaSinkConfig kafkaSinkConfig) {
        kafkaSinkConfig.setDlqConfig(pluginSetting);
        final Optional<PluginModel> dlq = kafkaSinkConfig.getDlq();
        if (dlq.isPresent()) {
            final PluginModel dlqPluginModel = dlq.get();
            final PluginSetting dlqPluginSetting = new PluginSetting(dlqPluginModel.getPluginName(), dlqPluginModel.getPluginSettings());
            dlqPluginSetting.setPipelineName(pluginSetting.getPipelineName());
            final DlqProvider dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
            return dlqProvider;
        }
        return null;
    }

    private void logFailureForDlqObjects(final DlqWriter dlqWriter, final List<DlqObject> dlqObjects) {
        try {
            dlqWriter.write(dlqObjects, pluginSetting.getPipelineName(), pluginSetting.getName());
            dlqObjects.forEach((dlqObject) -> {
                dlqObject.releaseEventHandle(true);
            });
        } catch (final IOException e) {
            dlqObjects.forEach(dlqObject -> {
                LOG.error(SENSITIVE, "DLQ failure for Document[{}]", dlqObject.getFailedData(), e);
                dlqObject.releaseEventHandle(false);
            });
        }
    }
}

