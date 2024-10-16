package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class LambdaProcessorServiceIT {

    private LambdaAsyncClient lambdaAsyncClient;
    private String functionName;
    private String lambdaRegion;
    private String role;
    private BufferFactory bufferFactory;
    @Mock
    private LambdaProcessorConfig lambdaProcessorConfig;
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
    private PluginFactory pluginFactory;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private Counter numberOfRecordsSuccessCounter;
    @Mock
    private Counter numberOfRecordsFailedCounter;
    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));


    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        lambdaRegion = System.getProperty("tests.lambda.processor.region");
        functionName = System.getProperty("tests.lambda.processor.functionName");
        role = System.getProperty("tests.lambda.processor.sts_role_arn");

        final Region region = Region.of(lambdaRegion);

        lambdaAsyncClient = LambdaAsyncClient.builder()
                .region(Region.of(lambdaRegion))
                .build();

        bufferFactory = new InMemoryBufferFactory();

        when(pluginMetrics.counter(LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS)).
                thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED)).
                thenReturn(numberOfRecordsFailedCounter);
    }


    private static Record<Event> createRecord() {
        final JacksonEvent event = JacksonLog.builder().withData("[{\"name\":\"test\"}]").build();
        return new Record<>(event);
    }

    public LambdaProcessor createObjectUnderTest(final String config) throws JsonProcessingException {

        final LambdaProcessorConfig lambdaProcessorConfig = objectMapper.readValue(config, LambdaProcessorConfig.class);
        return new LambdaProcessor(pluginMetrics,lambdaProcessorConfig,awsCredentialsSupplier,expressionEvaluator);
    }

    public LambdaProcessor createObjectUnderTest(LambdaProcessorConfig lambdaSinkConfig) throws JsonProcessingException {
        return new LambdaProcessor(pluginMetrics,lambdaSinkConfig,awsCredentialsSupplier,expressionEvaluator);
    }


    private static Collection<Record<Event>> generateRecords(int numberOfRecords) {
        List<Record<Event>> recordList = new ArrayList<>();

        for (int rows = 1; rows <= numberOfRecords; rows++) {
            HashMap<String, String> eventData = new HashMap<>();
            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));

            Record<Event> eventRecord = new Record<>(JacksonEvent.builder().withData(eventData).withEventType("event").build());
            recordList.add(eventRecord);
        }
        return recordList;
    }

    @ParameterizedTest
    @ValueSource(ints = {1,3})
    void verify_records_to_lambda_success(final int recordCount) throws Exception {

        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(lambdaProcessorConfig.getMaxConnectionRetries()).thenReturn(3);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn("RequestResponse");

        LambdaProcessor objectUnderTest = createObjectUnderTest(lambdaProcessorConfig);

        Collection<Record<Event>> recordsData = generateRecords(recordCount);
        List<Record<Event>> recordsResult = (List<Record<Event>>) objectUnderTest.doExecute(recordsData);
        Thread.sleep(Duration.ofSeconds(10).toMillis());

        assertEquals(recordsResult.size(),recordCount);
    }

    @ParameterizedTest
    @ValueSource(ints = {1,3})
    void verify_records_with_batching_to_lambda(final int recordCount) throws JsonProcessingException, InterruptedException {

        when(lambdaProcessorConfig.getFunctionName()).thenReturn(functionName);
        when(lambdaProcessorConfig.getMaxConnectionRetries()).thenReturn(3);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn("RequestResponse");
        when(thresholdOptions.getEventCount()).thenReturn(1);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("2mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.parse("PT10s"));
        when(batchOptions.getKeyName()).thenReturn("lambda_batch_key");
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);

        LambdaProcessor objectUnderTest = createObjectUnderTest(lambdaProcessorConfig);
        Collection<Record<Event>> records = generateRecords(recordCount);
        Collection<Record<Event>> recordsResult = objectUnderTest.doExecute(records);
        Thread.sleep(Duration.ofSeconds(10).toMillis());
        assertEquals(recordsResult.size(),recordCount);
    }
}