/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink.dlq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class DlqPushHandlerTest {

    private static final String BUCKET = "bucket";
    private static final String TEST_BUCKET = "test";
    private static final String TEST_ROLE = "arn:aws:iam::524239988122:role/app-test";
    private static final String TEST_REGION = "ap-south-1";
    private static final String S3_PLUGIN_NAME = "s3";
    private static final String KEY_PATH_PREFIX = "key_path_prefix";

    private static final String KEY_PATH_PREFIX_VALUE = "dlq/";

    private static final String PIPELINE_NAME = "log-pipeline";

    private static final String DLQ_FILE = "local_dlq_file";

    private PluginModel pluginModel;

    private DlqPushHandler dlqPushHandler;
    private PluginFactory pluginFactory;

    private AwsAuthenticationOptions awsAuthenticationOptions;

    private DlqProvider dlqProvider;

    private PluginSetting pluginSetting;

    private DlqWriter dlqWriter;
    
    private PluginModel dlqConfig;


    @BeforeEach
    public void setUp(){
        this.pluginFactory = mock(PluginFactory.class);
        this.pluginSetting = mock(PluginSetting.class);
        this.dlqProvider = mock(DlqProvider.class);
        dlqConfig = mock(PluginModel.class);
        dlqWriter = mock(DlqWriter.class);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(pluginSetting.getName()).thenReturn("name");
        when(dlqConfig.getPluginName()).thenReturn("testPlugin");
        when(dlqConfig.getPluginSettings()).thenReturn(Map.of("bucket", TEST_BUCKET, DlqPushHandler.REGION, TEST_REGION, DlqPushHandler.STS_ROLE_ARN, TEST_ROLE));
    
        when(dlqProvider.getDlqWriter(anyString())).thenReturn(Optional.of(dlqWriter));
        this.awsAuthenticationOptions =  mock(AwsAuthenticationOptions.class);
        when(pluginFactory.loadPlugin(any(), any(PluginSetting.class))).thenReturn(dlqProvider);
    }

    private DlqPushHandler createObjectUnderTest(PluginModel dlqConfig) {
        return new DlqPushHandler(pluginFactory, pluginSetting, dlqConfig, awsAuthenticationOptions);
    }

    @Test
    void test_perform_with_valid_dlqConfig() throws Exception {
        DlqPushHandler dlqPushHandler = createObjectUnderTest(dlqConfig);
        DlqObject dlqObject = mock(DlqObject.class);
        dlqPushHandler.perform(List.of(dlqObject));
        verify(dlqWriter).write(anyList(), anyString(), anyString());
    }
    @Test
    void test_perform_with_valid_dlqConfig_with_no_region_no_arn() throws Exception {
        Map<String, Object> settings = new HashMap<>();
        settings.put("bucket", TEST_BUCKET);
        when(dlqConfig.getPluginSettings()).thenReturn(settings);
        String testRegion = TEST_REGION+"_test";
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(testRegion));
        String testRole = TEST_ROLE+"_test";
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(testRole);
        DlqPushHandler dlqPushHandler = createObjectUnderTest(dlqConfig);
        assertThat(settings.get(DlqPushHandler.REGION), equalTo(testRegion));
        assertThat(settings.get(DlqPushHandler.STS_ROLE_ARN), equalTo(testRole));
        DlqObject dlqObject = mock(DlqObject.class);
        dlqPushHandler.perform(List.of(dlqObject));
        verify(dlqWriter).write(anyList(), anyString(), anyString());
    }

    @Test
    void test_perform_with_dlqProvider_returning_null() throws Exception {
        when(dlqProvider.getDlqWriter(anyString())).thenReturn(Optional.empty());
        DlqPushHandler dlqPushHandler = createObjectUnderTest(dlqConfig);
        DlqObject dlqObject = mock(DlqObject.class);
        dlqPushHandler.perform(null);
        verify(dlqWriter, times(0)).write(anyList(), anyString(), anyString());
    }

    @Test
    void test_perform_with_loadPlugin_returning_null() throws Exception {
        when(pluginFactory.loadPlugin(any(), any(PluginSetting.class))).thenReturn(null);
        DlqPushHandler dlqPushHandler = createObjectUnderTest(dlqConfig);
        DlqObject dlqObject = mock(DlqObject.class);
        dlqPushHandler.perform(null);
        verify(dlqWriter, times(0)).write(anyList(), anyString(), anyString());
    }

    @Test
    void test_perform_with_null_dlqObjects() throws Exception {
        DlqPushHandler dlqPushHandler = createObjectUnderTest(dlqConfig);
        DlqObject dlqObject = mock(DlqObject.class);
        dlqPushHandler.perform(null);
        verify(dlqWriter, times(0)).write(anyList(), anyString(), anyString());
    }

    @Test
    void test_construction_with_empty_dlqObjects() throws Exception {
        DlqPushHandler dlqPushHandler = createObjectUnderTest(dlqConfig);
        DlqObject dlqObject = mock(DlqObject.class);
        dlqPushHandler.perform(List.of());
        verify(dlqWriter, times(0)).write(anyList(), anyString(), anyString());
    }

}
