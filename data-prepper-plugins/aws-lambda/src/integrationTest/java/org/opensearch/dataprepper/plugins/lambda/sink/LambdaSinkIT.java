package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory.convertToCredentialsOptions;

/**
 * Demonstrates testing threshold-based partial flush logic in LambdaSink
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LambdaSinkIT {
    private String lambdaRegion;
    private String functionName;
    private String roleArn;
    private ClientOptions clientOptions;
    private MockedStatic<PluginMetrics> pluginMetricsMock;
    private MockedStatic<LambdaClientFactory> factoryMock;

    // Mocks for config objects
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
    private PluginSetting pluginSetting;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private DlqPushHandler dlqPushHandler;

    // Mock pluginMetrics and the counters/timers
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter numberOfRecordsSuccessCounter;
    @Mock
    private Counter numberOfRecordsFailedCounter;
    @Mock
    private Counter numberOfRequestsSuccessCounter;
    @Mock
    private Counter numberOfRequestsFailedCounter;

    // The sink under test
    private LambdaSink lambdaSink;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

//        lambdaRegion = System.getProperty("tests.lambda.sink.region", "us-east-1");
//        functionName = System.getProperty("tests.lambda.sink.functionName", "testFunctionName");
//        roleArn = System.getProperty("tests.lambda.sink.sts_role_arn", "someRole");

        lambdaRegion = "us-west-2";
        functionName = "lambdaNoReturn";
        roleArn = "arn:aws:iam::176893235612:role/osis-s3-opensearch-role";

        // Mock pluginSetting
        when(pluginSetting.getName()).thenReturn("aws_lambda");
        when(pluginSetting.getPipelineName()).thenReturn("lambdaSinkITMultiBatch");

        // Configure pluginMetrics so that each named counter/timer returns a mock
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS))
                .thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED))
                .thenReturn(numberOfRecordsFailedCounter);
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA))
                .thenReturn(numberOfRequestsSuccessCounter);
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA))
                .thenReturn(numberOfRequestsFailedCounter);
        Timer genericTimer = mock(Timer.class);
        DistributionSummary genericSummary = mock(DistributionSummary.class);
        when(pluginMetrics.timer(anyString())).thenReturn(genericTimer);
        when(pluginMetrics.summary(anyString())).thenReturn(genericSummary);

        // Threshold config
        when(thresholdOptions.getEventCount()).thenReturn(5);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("1mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(120));

        // BatchOptions
        when(batchOptions.getKeyName()).thenReturn("lambdaSinkITKey");
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);

        // AWS Auth
        Region regionObj = Region.of(lambdaRegion);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(regionObj);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(roleArn);

        // LambdaSinkConfig
        when(lambdaSinkConfig.getFunctionName()).thenReturn(functionName);
        when(lambdaSinkConfig.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
        when(lambdaSinkConfig.getBatchOptions()).thenReturn(batchOptions);
        when(lambdaSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(lambdaSinkConfig.getDlqPluginSetting()).thenReturn(null);
        clientOptions = new ClientOptions();
        when(lambdaSinkConfig.getClientOptions()).thenReturn(clientOptions);

        // Now mock the static method PluginMetrics.fromPluginSetting(...) from AbstractSink
        pluginMetricsMock = mockStatic(PluginMetrics.class);
        pluginMetricsMock.when(() -> PluginMetrics.fromPluginSetting(pluginSetting))
                    .thenReturn(pluginMetrics);

        factoryMock = mockStatic(LambdaClientFactory.class);
        createLambdaClient(factoryMock, clientOptions);

        lambdaSink = objectUnderTest();
        lambdaSink.doInitialize();
    }

    @AfterEach
    void tearDown() {
        pluginMetricsMock.close();
        factoryMock.close();
    }

    private LambdaSink objectUnderTest() {
        // Build the sink
        lambdaSink = new LambdaSink(
                pluginSetting,
                lambdaSinkConfig,
                pluginFactory,
                null,  // real or mock SinkContext
                awsCredentialsSupplier,
                expressionEvaluator
        );
        return lambdaSink;
    }

    private void createLambdaClient(MockedStatic<LambdaClientFactory> factoryMock, ClientOptions clientOptions) {

        // Tell the mock to skip attaching MicrometerMetricPublisher:
        factoryMock.when(() ->
                LambdaClientFactory.createAsyncLambdaClient(eq(awsAuthenticationOptions),
                        eq(awsCredentialsSupplier), eq(clientOptions))
        ).thenAnswer(inv -> {
            // Build a normal client but omit .addMetricPublisher(...)
            // or just return a fully mock client.

            // a) If you want a near-real client
            NettyNioAsyncHttpClient httpClient = (NettyNioAsyncHttpClient) NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(clientOptions.getMaxConcurrency())
                    .connectionTimeout(clientOptions.getConnectionTimeout())
                    .build();

            ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                    // .addMetricPublisher(...) => skip
                    .apiCallTimeout(clientOptions.getApiCallTimeout())
                    .build();

            AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(awsAuthenticationOptions);
            AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

            return LambdaAsyncClient.builder()
                    .region(awsAuthenticationOptions.getAwsRegion())
                    .credentialsProvider(awsCredentialsProvider)
                    .overrideConfiguration(overrideConfig)
                    .httpClient(httpClient)
                    .build();
        });
    }

    @Test
    void testMultiBatchPartialAndFullFlushes() {
        // threshold=5
        // doOutput(3) => partial => no success
        List<Record<Event>> part1 = createEvents(3, "Batch1");
        lambdaSink.doOutput(part1);

        verify(numberOfRecordsSuccessCounter, never()).increment(anyDouble());
        verify(numberOfRequestsSuccessCounter, never()).increment();

        // doOutput(3) => total=6 => flush=5 => leftover=1 => success=5
        List<Record<Event>> part2 = createEvents(3, "Batch2");
        lambdaSink.doOutput(part2);

        verify(numberOfRecordsSuccessCounter).increment(5.0);
        verify(numberOfRequestsSuccessCounter).increment();

        // leftover=1

        // doOutput(4) => leftover(1)+4=5 => flush => success=5 => total=10
        List<Record<Event>> part3 = createEvents(4, "Batch3");
        lambdaSink.doOutput(part3);

        verify(numberOfRecordsSuccessCounter, times(2)).increment(5.0);
        verify(numberOfRequestsSuccessCounter, times(2)).increment();
    }

    @Test
    void testFinalPartialFlushOnShutdown() {

        // doOutput(3) => partial => success=0
        List<Record<Event>> smallList = createEvents(3, "PartialShutdown");
        lambdaSink.doOutput(smallList);

        verify(numberOfRecordsSuccessCounter, never()).increment(anyDouble());

        // shutdown => leftover=3 => flush => success=3
        lambdaSink.shutdown();
        verify(numberOfRecordsSuccessCounter).increment(3.0);
        verify(numberOfRequestsSuccessCounter).increment();
    }

    @Test
    void testSingleBatchFlushExceedThreshold() {
        // pass 6 => threshold=5 => flush=5 => leftover=1 => success=5
        List<Record<Event>> events = createEvents(6, "SingleBatch");
        lambdaSink.doOutput(events);

        verify(numberOfRecordsSuccessCounter).increment(5.0);
        verify(numberOfRequestsSuccessCounter).increment();

        lambdaSink.shutdown();
        // leftover=1 => flush => success=1 => total=6
        verify(numberOfRecordsSuccessCounter).increment(1.0);
        verify(numberOfRequestsSuccessCounter, times(2)).increment();
    }

    @Test
    void testTimeBasedThresholdFlush() throws InterruptedException {
        // Send 3 events (below the event count threshold)
        List<Record<Event>> events = createEvents(3, "TimeBatch1");
        lambdaSink.doOutput(events);

        // Wait for slightly less than the timeout
        Thread.sleep(400);

        // Send 2 more events
        events = createEvents(2, "TimeBatch2");
        lambdaSink.doOutput(events);

        // Wait for the timeout to be exceeded
        Thread.sleep(200);

        // Send an empty batch to trigger the time-based flush
        lambdaSink.doOutput(Collections.emptyList());

        // Verify that 5 events were flushed due to time-based threshold
        verify(numberOfRecordsSuccessCounter).increment(5.0);
        verify(numberOfRequestsSuccessCounter).increment();

        // Send 1 more event
        events = createEvents(1, "TimeBatch3");
        lambdaSink.doOutput(events);

        // Shutdown to flush any remaining events
        lambdaSink.shutdown();

        // Verify that the final event was flushed
        verify(numberOfRecordsSuccessCounter).increment(1.0);
        verify(numberOfRequestsSuccessCounter, times(2)).increment();
    }

    private List<Record<Event>> createEvents(int count, String prefix) {
        List<Record<Event>> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<String, Object> data = Map.of("id", i, "msg", prefix + i);
            EventMetadata metadata = DefaultEventMetadata.builder()
                    .withEventType("ITTest")
                    .build();
            Event event = JacksonEvent.builder()
                    .withData(data)
                    .withEventMetadata(metadata)
                    .build();
            records.add(new Record<>(event));
        }
        return records;
    }
}
