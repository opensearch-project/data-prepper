/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class LambdaSinkServiceIT {

    private LambdaClient lambdaClient;
    private String functionName;
    private String lambdaRegion;
    private String role;
    private BufferFactory bufferFactory;
    @Mock
    private LambdaSinkConfig lambdaSinkConfig;
    @Mock
    private BatchOptions batchOptions;
    @Mock
    private ThresholdOptions thresholdOptions;
    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private DlqPushHandler dlqPushHandler;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private Counter numberOfRecordsSuccessCounter;
    @Mock
    private Counter numberOfRecordsFailedCounter;
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));


    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        lambdaRegion = System.getProperty("tests.sink.lambda.region");
        functionName = System.getProperty("tests.sink.lambda.functionName");
        role = System.getProperty("tests.sink.lambda.sts_role_arn");

        final Region region = Region.of(lambdaRegion);

        lambdaClient = LambdaClient.builder()
                .region(Region.of(lambdaRegion))
                .build();

        bufferFactory = new InMemoryBufferFactory();

        when(pluginMetrics.counter(LambdaSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS)).
                thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(LambdaSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED)).
                thenReturn(numberOfRecordsFailedCounter);
    }


    private static Record<Event> createRecord() {
        final JacksonEvent event = JacksonLog.builder().withData("[{\"name\":\"test\"}]").build();
        return new Record<>(event);
    }

    public LambdaSinkService createObjectUnderTest(final String config) throws JsonProcessingException {

        final LambdaSinkConfig lambdaSinkConfig = objectMapper.readValue(config, LambdaSinkConfig.class);
        OutputCodecContext codecContext = new OutputCodecContext("Tag", Collections.emptyList(), Collections.emptyList());
        pluginFactory = null;
        return new LambdaSinkService(lambdaClient,
                lambdaSinkConfig,
                pluginMetrics,
                pluginFactory,
                pluginSetting,
                codecContext,
                awsCredentialsSupplier,
                dlqPushHandler,
                bufferFactory);
    }

    public LambdaSinkService createObjectUnderTest(LambdaSinkConfig lambdaSinkConfig) throws JsonProcessingException {

        OutputCodecContext codecContext = new OutputCodecContext("Tag", Collections.emptyList(), Collections.emptyList());
        pluginFactory = null;
        return new LambdaSinkService(lambdaClient,
                lambdaSinkConfig,
                pluginMetrics,
                pluginFactory,
                pluginSetting,
                codecContext,
                awsCredentialsSupplier,
                dlqPushHandler,
                bufferFactory);
    }


    private static Collection<Record<Event>> generateRecords(int numberOfRecords) {
        List<Record<Event>> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {
            HashMap<String, String> eventData = new HashMap<>();
            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));

            Record<Event> eventRecord = new Record<>(JacksonEvent.builder().withData(eventData).withEventType("event").build());
            recordList.add(eventRecord);
        }
        return recordList;
    }

    @ParameterizedTest
    @ValueSource(ints = {1,5})
    void verify_flushed_records_to_lambda_success(final int recordCount) throws Exception {

        final String LAMBDA_SINK_CONFIG_YAML =
                "        function_name: " + functionName +"\n" +
                        "        aws:\n" +
                        "          region: us-east-1\n" +
                        "          sts_role_arn: " + role + "\n" +
                        "        max_retries: 3\n";
        LambdaSinkService objectUnderTest = createObjectUnderTest(LAMBDA_SINK_CONFIG_YAML);

        Collection<Record<Event>> recordsData = generateRecords(recordCount);
        objectUnderTest.output(recordsData);
        Thread.sleep(Duration.ofSeconds(10).toMillis());

        verify(numberOfRecordsSuccessCounter, times(recordCount)).increment(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {1,5,10})
    void verify_flushed_records_to_lambda_failed_and_dlq_works(final int recordCount) throws Exception {
        final String LAMBDA_SINK_CONFIG_INVALID_FUNCTION_NAME =
                          "        function_name: $$$\n" +
                        "        aws:\n" +
                        "          region: us-east-1\n" +
                        "          sts_role_arn: arn:aws:iam::176893235612:role/osis-s3-opensearch-role\n" +
                        "        max_retries: 3\n" +
                        "        dlq: #any failed even\n"+
                        "            s3:\n"+
                        "                bucket: test-bucket\n"+
                        "                key_path_prefix: dlq/\n";
        LambdaSinkService objectUnderTest = createObjectUnderTest(LAMBDA_SINK_CONFIG_INVALID_FUNCTION_NAME);

        Collection<Record<Event>> recordsData = generateRecords(recordCount);
        objectUnderTest.output(recordsData);
        Thread.sleep(Duration.ofSeconds(10).toMillis());

       verify( numberOfRecordsFailedCounter, times(recordCount)).increment(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {2,5})
    void verify_flushed_records_with_batching_to_lambda(final int recordCount) throws JsonProcessingException, InterruptedException {

        int event_count = 2;
        when(lambdaSinkConfig.getFunctionName()).thenReturn(functionName);
        when(lambdaSinkConfig.getMaxConnectionRetries()).thenReturn(3);
        when(thresholdOptions.getEventCount()).thenReturn(event_count);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("2mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.parse("PT10s"));
        when(batchOptions.getKeyName()).thenReturn("lambda_batch_key");
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        when(lambdaSinkConfig.getBatchOptions()).thenReturn(batchOptions);

        LambdaSinkService objectUnderTest = createObjectUnderTest(lambdaSinkConfig);
        Collection<Record<Event>> recordsData = generateRecords(recordCount);
        objectUnderTest.output(recordsData);
        Thread.sleep(Duration.ofSeconds(10).toMillis());
    }
}