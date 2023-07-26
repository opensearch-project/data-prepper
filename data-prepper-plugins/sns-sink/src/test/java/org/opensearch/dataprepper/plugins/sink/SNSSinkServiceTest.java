/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

public class SNSSinkServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private SNSSinkConfig snsSinkConfig;

    private SnsClient snsClient;

    private PluginMetrics pluginMetrics;

    private Counter numberOfRecordsSuccessCounter;

    private Counter numberOfRecordsFailedCounter;

    private static final String config = "        topic: arn:aws:sns:ap-south-1:524239988912:my-topic\n" +
            "        message_group_id: test\n" +
            "        aws:\n" +
            "          region: ap-south-1\n" +
            "          sts_role_arn: arn:aws:iam::524239988912:role/app-test\n" +
            "          sts_header_overrides: {\"test\":\"test\"}\n" +
            "        codec:\n" +
            "          ndjson:\n" +
            "        max_retries: 10\n" +
            "        dlq_file: /test/dlq-file.log\n" +
            "        dlq:\n" +
            "          s3:\n" +
            "            bucket: test\n" +
            "            key_path_prefix: test\n";
    private PublishBatchResponse publishBatchResponse;

    private SdkHttpResponse sdkHttpResponse;


    @BeforeEach
    public void setup() throws JsonProcessingException {
        this.snsSinkConfig = objectMapper.readValue(config,SNSSinkConfig.class);
        this.snsClient = mock(SnsClient.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.numberOfRecordsSuccessCounter = mock(Counter.class);
        this.numberOfRecordsFailedCounter = mock(Counter.class);
        this.publishBatchResponse = mock(PublishBatchResponse.class);
        this.sdkHttpResponse = mock(SdkHttpResponse.class);

        when(pluginMetrics.counter(SNSSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_SNS_SUCCESS)).thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(SNSSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_SNS_FAILED)).thenReturn(numberOfRecordsFailedCounter);
        when(snsClient.publishBatch(any(PublishBatchRequest.class))).thenReturn(publishBatchResponse);
        when(publishBatchResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(publishBatchResponse.sdkHttpResponse().statusCode()).thenReturn(new Random().nextInt());
        when(sdkHttpResponse.statusCode()).thenReturn(200);

    }


    private SNSSinkService createObjectUnderTest(){
        return new SNSSinkService(snsSinkConfig,
                snsClient,
                pluginMetrics, mock(PluginFactory.class), mock(PluginSetting.class));
    }

    @Test
    public void sns_sink_test_with_empty_collection_records(){
        numberOfRecordsSuccessCounter = mock(Counter.class);
        SNSSinkService snsSinkService = createObjectUnderTest();
        snsSinkService.output(List.of());
        verifyNoInteractions(numberOfRecordsSuccessCounter);
        verifyNoInteractions(numberOfRecordsFailedCounter);
    }

    @Test
    public void sns_sink_test_with_single_collection_record_success_push_to_sns(){
        when(sdkHttpResponse.statusCode()).thenReturn(500);
        SNSSinkService snsSinkService = createObjectUnderTest();
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        Collection<Record<Event>> records = List.of(eventRecord);
        snsSinkService.output(records);
        verify(numberOfRecordsSuccessCounter).increment(records.size());
    }

    @Test
    public void sns_sink_test_with_single_collection_record_failed_to_push_to_sns(){
        final Counter abc = mock(Counter.class);
        when(pluginMetrics.counter(SNSSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_SNS_FAILED)).thenReturn(abc);
        final SnsException snsException = (SnsException)SnsException.builder().message("internal server error").awsErrorDetails(AwsErrorDetails.builder().errorMessage("internal server error").build()).build();
        when(snsClient.publishBatch(any(PublishBatchRequest.class))).thenThrow(snsException);
        SNSSinkService snsSinkService = createObjectUnderTest();
        final Record<Event> eventRecord = new Record<>(JacksonEvent.fromMessage("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}"));
        Collection<Record<Event>> records = List.of(eventRecord);
        snsSinkService.output(records);
        verify(abc).increment(records.size());
    }

    @Test
    void sns_sink_service_test_output_with_single_record_ack_release() {
        final SNSSinkService snsSinkService = createObjectUnderTest();
        final Event event = mock(Event.class);
        given(event.toJsonString()).willReturn("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}");
        given(event.getEventHandle()).willReturn(mock(EventHandle.class));
        snsSinkService.output(List.of(new Record<>(event)));
        verify(numberOfRecordsSuccessCounter).increment(1);
    }
}
