/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.dlq;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;


/**
 * * An Handler class which helps log failed data to AWS S3 bucket or file based on configuration.
 */

public class DlqPushHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DlqPushHandler.class);
    public static final String NUM_DLQ_SUCCESS = "NumDlqSuccess";
    public static final String NUM_DLQ_FAILED = "NumDlqFailed";
    public static final String STS_ROLE_ARN = "sts_role_arn";
    public static final String REGION = "region";

    private final DlqProvider dlqProvider;
    private final PluginSetting dlqPluginSetting;
    private final DlqWriter dlqWriter;
    private final Counter dlqSuccessCounter;
    private final Counter dlqFailedCounter;

    public DlqPushHandler(final PluginFactory pluginFactory, final PluginSetting pluginSetting, final PluginMetrics pluginMetrics,
                          final PluginModel dlqConfig, final String region, final String role, final String metricsPrefix) {
        Map<String, Object> dlqSettings = dlqConfig.getPluginSettings();
        if (!dlqSettings.containsKey(REGION) && region != null) {
            dlqSettings.put(REGION, region);
        }
        if (!dlqSettings.containsKey(STS_ROLE_ARN) && role != null) {
            dlqSettings.put(STS_ROLE_ARN, role);
        }
        dlqPluginSetting = new PluginSetting(dlqConfig.getPluginName(), dlqSettings);
        dlqPluginSetting.setPipelineName(pluginSetting.getPipelineName());
        this.dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
        if (this.dlqProvider != null) {
            Optional<DlqWriter> potentialDlq = this.dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
              .add(pluginSetting.getPipelineName())
              .add(pluginSetting.getName()).toString());
            this.dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
        } else {
            this.dlqWriter = null;
        }
        this.dlqSuccessCounter = pluginMetrics.counter(metricsPrefix+NUM_DLQ_SUCCESS);
        this.dlqFailedCounter = pluginMetrics.counter(metricsPrefix+NUM_DLQ_FAILED);
    }

    public PluginSetting getPluginSetting() {
        return dlqPluginSetting;
    }

    public double getDlqSuccessCounter() {
        return dlqSuccessCounter.count();
    }

    public double getDlqFailedCounter() {
        return dlqFailedCounter.count();
    }

    public boolean perform(final List<DlqObject> dlqObjects) {
        try {
            if (dlqWriter != null && dlqObjects != null && dlqObjects.size() > 0) {
                dlqWriter.write(dlqObjects, dlqPluginSetting.getPipelineName(), dlqPluginSetting.getName());
                dlqSuccessCounter.increment(dlqObjects.size());
                return true;
            }
        } catch (Exception e) {
            LOG.error(NOISY, "failed to write to DLQ", e);
            dlqFailedCounter.increment(dlqObjects.size());
        }
        return false;
    }
}

