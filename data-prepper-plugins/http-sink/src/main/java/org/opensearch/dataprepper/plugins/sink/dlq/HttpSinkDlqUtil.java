/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.dlq.s3.KeyPathGenerator;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.UUID.randomUUID;


/**
 * * An util class which helps log failed data to AWS S3 bucket
 */

public class HttpSinkDlqUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkDlqUtil.class);

    private static final String BUCKET = "bucket";
    private static final String ROLE_ARN = "sts_role_arn";
    private static final String REGION = "region";
    private static final String S3_PLUGIN_NAME = "s3";
    private static final String KEY_PATH_PREFIX = "key_path_prefix";

    private static final String KEY_NAME_FORMAT = "dlq-v%s-%s-%s-%s-%s.json";

    private static final String FULL_KEY_FORMAT = "%s%s";

    private String keyPathPrefix;

    private final DlqProvider dlqProvider;

    private KeyPathGenerator keyPathGenerator;

    public HttpSinkDlqUtil(final PluginFactory pluginFactory,
                           final HttpSinkConfiguration httpSinkConfiguration) {
         this.dlqProvider = getDlqProvider(pluginFactory, httpSinkConfiguration);
    }

    public  void perform(final PluginSetting pluginSetting,
                         final Object failedData) {
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
                                        final HttpSinkConfiguration httpSinkConfiguration) {
        final Map<String, Object> props = new HashMap<>();
        props.put(BUCKET, httpSinkConfiguration.getDlq().getPluginSettings().get(BUCKET).toString());
        props.put(ROLE_ARN, httpSinkConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn().toString());
        props.put(REGION, httpSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion().toString());
        keyPathPrefix = StringUtils.isEmpty(httpSinkConfiguration.getDlq().getPluginSettings().get(KEY_PATH_PREFIX).toString()) ? httpSinkConfiguration.getDlq().getPluginSettings().get(KEY_PATH_PREFIX).toString() :
                enforceDefaultDelimiterOnKeyPathPrefix(httpSinkConfiguration.getDlq().getPluginSettings().get(KEY_PATH_PREFIX).toString());
        props.put(KEY_PATH_PREFIX, httpSinkConfiguration.getDlq().getPluginSettings().get(KEY_PATH_PREFIX).toString());
        final PluginSetting dlqPluginSetting = new PluginSetting(S3_PLUGIN_NAME, props);
        DlqProvider dlqProvider = pluginFactory.loadPlugin(DlqProvider.class, dlqPluginSetting);
        return dlqProvider;
    }

    private String enforceDefaultDelimiterOnKeyPathPrefix(final String keyPathPrefix) {
        return (keyPathPrefix.charAt(keyPathPrefix.length() - 1) == '/') ? keyPathPrefix : keyPathPrefix.concat("/");
    }
}

