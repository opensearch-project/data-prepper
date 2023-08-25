/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.dlq;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AwsAuthenticationOptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;

public class DlqPushHandlerTest {

    private static final String BUCKET = "bucket";
    private static final String BUCKET_VALUE = "test";
    private static final String ROLE = "arn:aws:iam::524239988944:role/app-test";

    private static final String REGION = "ap-south-1";
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

    private DlqWriter dlqWriter;

    private PluginMetrics pluginMetrics;

    private  Counter dlqS3RecordsSuccessCounter;
    private  Counter dlqS3RecordsFailedCounter;
    private  Counter dlqS3RequestSuccessCounter;
    private  Counter dlqS3RequestFailedCounter;

    private  Counter dlqFileRecordsSuccessCounter;
    private  Counter dlqFileRecordsFailedCounter;
    private  Counter dlqFileRequestSuccessCounter;
    private  Counter dlqFileRequestFailedCounter;

    @BeforeEach
    public void setUp() throws Exception{
        this.pluginFactory = mock(PluginFactory.class);
        this.pluginModel = mock(PluginModel.class);
        this.awsAuthenticationOptions =  mock(AwsAuthenticationOptions.class);
        this.dlqProvider = mock(DlqProvider.class);
        this.dlqWriter = mock(DlqWriter.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.dlqS3RecordsSuccessCounter = mock(Counter.class);
        this.dlqS3RecordsFailedCounter = mock(Counter.class);
        this.dlqS3RequestSuccessCounter = mock(Counter.class);
        this.dlqS3RequestFailedCounter = mock(Counter.class);
        this.dlqFileRecordsSuccessCounter = mock(Counter.class);
        this.dlqFileRecordsFailedCounter = mock(Counter.class);
        this.dlqFileRequestSuccessCounter = mock(Counter.class);
        this.dlqFileRequestFailedCounter = mock(Counter.class);
        when(pluginMetrics.counter(DlqPushHandler.S3_DLQ_RECORDS_SUCCESS)).thenReturn(dlqS3RecordsSuccessCounter);
        when(pluginMetrics.counter(DlqPushHandler.S3_DLQ_RECORDS_FAILED)).thenReturn(dlqS3RecordsFailedCounter);
        when(pluginMetrics.counter(DlqPushHandler.S3_DLQ_REQUEST_SUCCESS)).thenReturn(dlqS3RequestSuccessCounter);
        when(pluginMetrics.counter(DlqPushHandler.S3_DLQ_REQUEST_FAILED)).thenReturn(dlqS3RequestFailedCounter);
        when(pluginMetrics.counter(DlqPushHandler.FILE_DLQ_RECORDS_SUCCESS)).thenReturn(dlqFileRecordsSuccessCounter);
        when(pluginMetrics.counter(DlqPushHandler.FILE_DLQ_RECORDS_FAILED)).thenReturn(dlqFileRecordsFailedCounter);
        when(pluginMetrics.counter(DlqPushHandler.FILE_DLQ_REQUEST_SUCCESS)).thenReturn(dlqFileRequestSuccessCounter);
        when(pluginMetrics.counter(DlqPushHandler.FILE_DLQ_REQUEST_FAILED)).thenReturn(dlqFileRequestFailedCounter);


    }

    @Test
    public void perform_for_dlq_s3_success() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(BUCKET,BUCKET_VALUE);
        props.put(KEY_PATH_PREFIX,KEY_PATH_PREFIX_VALUE);

        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(dlqProvider);

        when(dlqProvider.getDlqWriter(Mockito.anyString())).thenReturn(Optional.of(dlqWriter));
        doNothing().when(dlqWriter).write(anyList(), anyString(), anyString());
        FailedDlqData failedDlqData = FailedDlqData.builder().build();
        dlqPushHandler = new DlqPushHandler(null,pluginFactory, BUCKET_VALUE, ROLE, REGION,KEY_PATH_PREFIX_VALUE, pluginMetrics);

        PluginSetting pluginSetting = new PluginSetting(S3_PLUGIN_NAME, props);
        pluginSetting.setPipelineName(PIPELINE_NAME);
        dlqPushHandler.perform(pluginSetting, failedDlqData);
        verify(dlqWriter).write(anyList(), anyString(), anyString());
    }


    @Test
    public void perform_for_dlq_local_file_success(){

        FailedDlqData failedDlqData = FailedDlqData.builder().build();
        dlqPushHandler = new DlqPushHandler(DLQ_FILE,pluginFactory,null, ROLE, REGION,null, pluginMetrics);

        PluginSetting pluginSetting = new PluginSetting(S3_PLUGIN_NAME, null);
        pluginSetting.setPipelineName(PIPELINE_NAME);
        dlqPushHandler.perform(pluginSetting, failedDlqData);
    }
}
