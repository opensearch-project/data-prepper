/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.ml_inference.processor.dlq;

import lombok.Getter;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

public class DlqPushHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DlqPushHandler.class);
    public static final String STS_ROLE_ARN = "sts_role_arn";

    public static final String REGION = "region";
    @Getter
    private PluginSetting dlqPluginSetting;
    private DlqProvider dlqProvider;
    private DlqWriter dlqWriter;

    public DlqPushHandler(final PluginFactory pluginFactory, final PluginSetting pluginSetting,
                          final PluginModel dlqConfig, final AwsAuthenticationOptions awsAuthenticationOptions) {
        dlqPluginSetting = new PluginSetting(dlqConfig.getPluginName(), dlqConfig.getPluginSettings());
        dlqPluginSetting.setPipelineName(pluginSetting.getPipelineName());

        Map<String, Object> dlqSettings = dlqPluginSetting.getSettings();
        boolean settingsChanged = false;
        if (!dlqSettings.containsKey(REGION)) {
            if (awsAuthenticationOptions != null) {
                dlqSettings.put(REGION, String.valueOf(awsAuthenticationOptions.getAwsRegion()));
                settingsChanged = true;
            }
        }
        if (!dlqSettings.containsKey(STS_ROLE_ARN)) {
            if (awsAuthenticationOptions != null) {
                dlqSettings.put(STS_ROLE_ARN, String.valueOf(awsAuthenticationOptions.getAwsStsRoleArn()));
                settingsChanged = true;
            }
        }
        if (settingsChanged) {
            LOG.info("Using AWS credentials from aws Auth Config");
            dlqPluginSetting.setSettings(dlqSettings);
        }
        this.dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
        if (this.dlqProvider != null) {
            Optional<DlqWriter> potentialDlq = this.dlqProvider.getDlqWriter(new StringJoiner(MetricNames.DELIMITER)
                    .add(pluginSetting.getPipelineName())
                    .add(pluginSetting.getName()).toString());
            this.dlqWriter = potentialDlq.isPresent() ? potentialDlq.get() : null;
        }
    }

    public void perform(final List<DlqObject> dlqObjects) throws Exception {
        if (dlqWriter != null && dlqObjects != null && dlqObjects.size() > 0) {
            dlqWriter.write(dlqObjects, dlqPluginSetting.getPipelineName(), dlqPluginSetting.getName());
            dlqObjects.forEach(dlqObject -> {
                dlqObject.releaseEventHandle(true);
            });
        }
    }
}
