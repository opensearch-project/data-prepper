/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.invocation.Invocation;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSet;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetMetrics;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.Batch;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.BatchSender;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsClientFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsService;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.EntityResolver;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.ParallelFlusher;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.dlq.CloudWatchLogsSinkDlqData;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EXECUTOR_ACTIVE_THREADS;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EXECUTOR_QUEUE_DEPTH;

class CloudWatchLogsSinkTest {
    private static int TEST_MAX_RETRIES = 3;
    private PluginSetting mockPluginSetting;
    private PluginMetrics mockPluginMetrics;
    private PluginFactory mockPluginFactory;
    private CloudWatchLogsSinkConfig mockCloudWatchLogsSinkConfig;
    private AwsCredentialsSupplier mockCredentialSupplier;
    private AwsConfig mockAwsConfig;
    private ThresholdConfig thresholdConfig;
    private Map<String, String> mockHeaderOverrides;
    private CloudWatchLogsClient mockClient;
    private ExpressionEvaluator mockExpressionEvaluator;
    private static final String TEST_LOG_GROUP = "testLogGroup";
    private static final String TEST_LOG_STREAM= "testLogStream";
    private static final String TEST_PLUGIN_NAME = "testPluginName";
    private static final String TEST_PIPELINE_NAME = "testPipelineName";
    // Number of args in DlqPushHandler(pluginFactory, pluginSetting, pluginMetrics, dlqConfig, region, role, metricsPrefix).
    // Bumping this is the signal that positional get(4)/get(5) reads in captureDlqRegionAndRole() need re-auditing.
    private static final int EXPECTED_DLQ_PUSH_HANDLER_ARITY = 7;
    // Positional arguments of the public BatchSender constructor (client, metrics, maxRetries, createLogGroup, createLogStream,
    // random, clock). Bumping this is the signal that the get(2)/get(3)/get(4) reads below need re-auditing.
    private static final int EXPECTED_BATCH_SENDER_ARITY = 7;
    private static final String OVERSIZED_MESSAGE = "x".repeat(200);
    private static final String TEMPLATE_HEADER = "${getAzureDataSourceNameType(/resourceId, /category)}";

    enum DlqMode { NONE, OK, FALSE, THROWS }

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final List<ThreadPoolExecutor> pools = new ArrayList<>();
    private final List<EventHandle> handles = new ArrayList<>();
    private final List<List<DlqObject>> performed = Collections.synchronizedList(new ArrayList<>());
    private final List<Boolean> lockProbes = Collections.synchronizedList(new ArrayList<>());
    private volatile Runnable performHook = () -> { };
    private Random random;
    private CloudWatchLogsMetrics metrics;

    @BeforeEach
    void setUp() {
        mockPluginSetting = mock(PluginSetting.class);
        mockPluginMetrics = mock(PluginMetrics.class);
        mockPluginFactory = mock(PluginFactory.class);
        mockCloudWatchLogsSinkConfig = mock(CloudWatchLogsSinkConfig.class);
        mockCredentialSupplier = mock(AwsCredentialsSupplier.class);
        mockAwsConfig = mock(AwsConfig.class);
        thresholdConfig = new ThresholdConfig();
        mockHeaderOverrides = new HashMap<>();
        mockHeaderOverrides.put("X-Test-Header", "test-value");
        mockClient = mock(CloudWatchLogsClient.class);
        mockExpressionEvaluator = mock(ExpressionEvaluator.class);
        DistributionSummary summary = mock(DistributionSummary.class);
        when(mockPluginMetrics.summary(anyString())).thenReturn(summary);
        // A mocked Random draws 0.0, so every backoff sleep is zero and no test sleeps for real.
        random = mock(Random.class);

        when(mockCloudWatchLogsSinkConfig.getDlq()).thenReturn(null);
        when(mockCloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(mockAwsConfig);
        when(mockCloudWatchLogsSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(new HashMap<>());
        when(mockCloudWatchLogsSinkConfig.getLogGroup()).thenReturn(TEST_LOG_GROUP);
        when(mockCloudWatchLogsSinkConfig.getLogStream()).thenReturn(TEST_LOG_STREAM);
        when(mockCloudWatchLogsSinkConfig.getMaxRetries()).thenReturn(TEST_MAX_RETRIES);
        when(mockCloudWatchLogsSinkConfig.getWorkers()).thenReturn(10);
        when(mockCredentialSupplier.getDefaultRegion()).thenReturn(Optional.empty());
        when(mockCredentialSupplier.getDefaultStsRoleArn()).thenReturn(Optional.empty());

        when(mockPluginSetting.getName()).thenReturn(TEST_PLUGIN_NAME);
        when(mockPluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(PutLogEventsResponse.builder().build());
    }

    @AfterEach
    void tearDown() {
        Thread.interrupted();
        pools.forEach(ThreadPoolExecutor::shutdownNow);
    }

    CloudWatchLogsSink getTestCloudWatchSink() {
        return new CloudWatchLogsSink(mockPluginSetting, mockPluginMetrics, mockPluginFactory, mockCloudWatchLogsSinkConfig,
                mockCredentialSupplier, mockExpressionEvaluator);
    }

    private ThreadPoolExecutor pool(final int workers) {
        final ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(workers);
        pools.add(pool);
        return pool;
    }

    private ThreadPoolExecutor spyPool(final int workers) {
        final ThreadPoolExecutor pool = spy((ThreadPoolExecutor) Executors.newFixedThreadPool(workers));
        pools.add(pool);
        return pool;
    }

    private void setThreshold(final String field, final Object value) throws Exception {
        ReflectivelySetField.setField(ThresholdConfig.class, thresholdConfig, field, value);
    }

    private CloudWatchLogsSink createSink(final ThreadPoolExecutor pool, final DlqMode dlqMode) {
        if (dlqMode != DlqMode.NONE) {
            final PluginModel dlqConfig = mock(PluginModel.class);
            when(mockCloudWatchLogsSinkConfig.getDlq()).thenReturn(dlqConfig);
        }
        try (MockedStatic<CloudWatchLogsClientFactory> factory = mockStatic(CloudWatchLogsClientFactory.class);
             MockedConstruction<CloudWatchLogsMetrics> metricsConstruction = mockConstruction(CloudWatchLogsMetrics.class);
             MockedConstruction<DlqPushHandler> dlqConstruction = mockConstruction(DlqPushHandler.class,
                     (mock, context) -> stubDlq(mock, dlqMode))) {
            factory.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(), any(), any(), any())).thenReturn(mockClient);
            final CloudWatchLogsSink sink = new CloudWatchLogsSink(mockPluginSetting, mockPluginMetrics, mockPluginFactory,
                    mockCloudWatchLogsSinkConfig, mockCredentialSupplier, mockExpressionEvaluator, pool, random, now::get);
            metrics = metricsConstruction.constructed().get(0);
            sink.doInitialize();
            return sink;
        }
    }

    private void stubDlq(final DlqPushHandler dlqPushHandler, final DlqMode dlqMode) {
        final PluginSetting dlqPluginSetting = new PluginSetting("dlq", Map.of());
        dlqPluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        when(dlqPushHandler.getPluginSetting()).thenReturn(dlqPluginSetting);
        when(dlqPushHandler.perform(any())).thenAnswer(invocation -> {
            performed.add(new ArrayList<>(invocation.getArgument(0)));
            performHook.run();
            if (dlqMode == DlqMode.THROWS) {
                throw new RuntimeException("dlq writer failed");
            }
            return dlqMode == DlqMode.OK;
        });
    }

    private Record<Event> record(final String message) {
        final Event event = mock(Event.class);
        final EventHandle handle = mock(EventHandle.class);
        when(event.toJsonString()).thenReturn(message);
        when(event.getEventHandle()).thenReturn(handle);
        handles.add(handle);
        return new Record<>(event);
    }

    private List<Record<Event>> records(final int count) {
        return IntStream.range(0, count).mapToObj(i -> record("event-" + i)).collect(Collectors.toList());
    }

    private static EventHandle handleOf(final Record<Event> record) {
        return record.getData().getEventHandle();
    }

    private static void assertReleasedOnce(final EventHandle handle, final boolean value) {
        verify(handle).release(value);
        verify(handle, times(1)).release(anyBoolean());
    }

    private static void assertReleasedOnce(final List<Record<Event>> records, final boolean value) {
        records.forEach(record -> assertReleasedOnce(handleOf(record), value));
    }

    /** Disposition counts keyed by "counter:reason", read from every call made on the metrics mock. */
    private Map<String, Integer> dispositions() {
        final Map<String, Integer> counts = new HashMap<>();
        for (final Invocation invocation : mockingDetails(metrics).getInvocations()) {
            final String name = invocation.getMethod().getName();
            if (name.equals("increaseLogEventSuccessCounter")) {
                counts.merge("succeeded", invocation.<Integer>getArgument(0), Integer::sum);
            } else if (name.equals("increaseReleasedWithoutDeliveryCounter")) {
                counts.merge("releasedWithoutDelivery:" + invocation.getArgument(0), invocation.<Integer>getArgument(1), Integer::sum);
            } else if (name.equals("increaseEventsSentToDlqCounter")) {
                counts.merge("sentToDlq:" + invocation.getArgument(0), invocation.<Integer>getArgument(1), Integer::sum);
            } else if (name.equals("increaseEventsNegativelyAckedCounter")) {
                counts.merge("negativelyAcked:" + invocation.getArgument(0), invocation.<Integer>getArgument(1), Integer::sum);
            }
        }
        return counts;
    }

    /** The four disposition counters sum to the number of release calls made on the test's handles. */
    private void assertDispositionsConserved() {
        final long releaseCalls = handles.stream()
                .flatMap(handle -> mockingDetails(handle).getInvocations().stream())
                .filter(invocation -> invocation.getMethod().getName().equals("release"))
                .count();
        final int counted = dispositions().values().stream().mapToInt(Integer::intValue).sum();
        assertThat(counted, equalTo((int) releaseCalls));
    }

    private static boolean lockFreeFromAnotherThread(final ReentrantLock lock) throws Exception {
        final FutureTask<Boolean> probe = new FutureTask<>(() -> {
            if (lock.tryLock(1, TimeUnit.SECONDS)) {
                lock.unlock();
                return true;
            }
            return false;
        });
        new Thread(probe).start();
        return probe.get();
    }

    private static Thread start(final Runnable runnable) {
        final Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private void stubClientBlocking(final CountDownLatch entered, final CountDownLatch proceed) {
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            entered.countDown();
            proceed.await();
            return PutLogEventsResponse.builder().build();
        });
    }

    private static Batch batchOf(final List<Record<Event>> records) {
        return new Batch(records.stream().map(CloudWatchLogsSinkTest::handleOf).collect(Collectors.toList()),
                records.stream().map(record -> record.getData().toJsonString().getBytes(StandardCharsets.UTF_8))
                        .collect(Collectors.toList()), null, null);
    }

    @Test
    void WHEN_sink_is_initialized_THEN_sink_is_ready_returns_true() {
        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            CloudWatchLogsSink testCloudWatchSink = getTestCloudWatchSink();
            testCloudWatchSink.doInitialize();
            assertTrue(testCloudWatchSink.isReady());
        }
    }

    @Test
    void WHEN_awsConfig_and_awsCredentialsSupplier_null_THEN_should_throw() {
        mockCredentialSupplier = null;
        when(mockCloudWatchLogsSinkConfig.getAwsConfig()).thenReturn(null);
        try(MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            assertThrows(RuntimeException.class, ()-> getTestCloudWatchSink());
        }
    }

    static Stream<Arguments> headerOverrides() {
        return Stream.of(
                arguments(Map.of()),
                arguments(Map.of("X-Test-Header", "test-value")));
    }

    @ParameterizedTest
    @MethodSource("headerOverrides")
    void constructor_headerOverrides_passedToClientFactory(final Map<String, String> headerOverrides) {
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(headerOverrides);

        try (MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            getTestCloudWatchSink();

            mockedStatic.verify(() -> CloudWatchLogsClientFactory.createCwlClient(
                    eq(mockAwsConfig),
                    eq(mockCredentialSupplier),
                    eq(headerOverrides),
                    any()));
        }
    }

    private List<?> captureBatchSenderArguments() {
        final List<List<?>> senderArguments = new ArrayList<>();
        final List<Object> flusherPools = new ArrayList<>();
        try (MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class);
             MockedConstruction<BatchSender> senderMock = mockConstruction(BatchSender.class,
                     (mock, context) -> senderArguments.add(context.arguments()));
             MockedConstruction<ParallelFlusher> flusherMock = mockConstruction(ParallelFlusher.class,
                     (mock, context) -> flusherPools.add(context.arguments().get(0)));
             MockedConstruction<DlqPushHandler> dlqMock = mockConstruction(DlqPushHandler.class)) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(), any(), any(), any())).thenReturn(mockClient);

            getTestCloudWatchSink().doInitialize();
        }
        assertThat(senderArguments, hasSize(1));
        assertThat(senderArguments.get(0).size(), equalTo(EXPECTED_BATCH_SENDER_ARITY));
        final ArgumentCaptor<ThreadPoolExecutor> executorCaptor = ArgumentCaptor.forClass(ThreadPoolExecutor.class);
        verify(mockPluginMetrics).gauge(eq(CLOUDWATCH_LOGS_EXECUTOR_QUEUE_DEPTH), executorCaptor.capture(), any(ToDoubleFunction.class));
        verify(mockPluginMetrics).gauge(eq(CLOUDWATCH_LOGS_EXECUTOR_ACTIVE_THREADS), eq(executorCaptor.getValue()),
                any(ToDoubleFunction.class));
        assertThat(executorCaptor.getValue().getMaximumPoolSize(), equalTo(10));
        assertThat(flusherPools, equalTo(List.of(executorCaptor.getValue())));
        return senderArguments.get(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void constructor_withOrWithoutDlq_senderUsesConfiguredMaxRetries(final boolean dlqConfigured) {
        if (dlqConfigured) {
            final PluginModel dlqConfig = mock(PluginModel.class);
            when(mockCloudWatchLogsSinkConfig.getDlq()).thenReturn(dlqConfig);
            when(mockAwsConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
            when(mockAwsConfig.getAwsStsRoleArn()).thenReturn("role");
        }

        assertThat(captureBatchSenderArguments().get(2), equalTo(TEST_MAX_RETRIES));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void constructor_createFlags_passedToSender(final boolean createFlag) {
        when(mockCloudWatchLogsSinkConfig.getCreateLogGroup()).thenReturn(createFlag);
        when(mockCloudWatchLogsSinkConfig.getCreateLogStream()).thenReturn(createFlag);

        final List<?> arguments = captureBatchSenderArguments();

        assertThat(arguments.get(3), equalTo(createFlag));
        assertThat(arguments.get(4), equalTo(createFlag));
    }

    // API_CALL_TIMEOUT plus 5 s.
    @Test
    void terminationWait_is20Seconds() {
        assertThat(CloudWatchLogsSink.TERMINATION_WAIT, equalTo(Duration.ofSeconds(20)));
    }

    private Entity captureStaticEntity() {
        final List<List<?>> serviceArguments = new ArrayList<>();
        try (MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class);
             MockedConstruction<CloudWatchLogsService> serviceMock = mockConstruction(CloudWatchLogsService.class,
                     (mock, context) -> serviceArguments.add(context.arguments()))) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(), any(), any(), any())).thenReturn(mockClient);

            getTestCloudWatchSink();
        }
        assertThat(serviceArguments, hasSize(1));
        assertThat(serviceArguments.get(0).size(), equalTo(4));
        return (Entity) serviceArguments.get(0).get(3);
    }

    static Stream<Arguments> staticEntityConfigs() {
        return Stream.of(
                arguments(null, null),
                arguments(Map.of("Type", "RemoteService", "Name", "okta_auth0"), Map.of("AWS.ServiceNameSource", "UserConfiguration")));
    }

    @ParameterizedTest
    @MethodSource("staticEntityConfigs")
    void constructor_entityConfigNullOrStatic_serviceBuiltWithMatchingEntity(final Map<String, String> keyAttributes,
                                                                       final Map<String, String> attributes) throws Exception {
        if (keyAttributes != null) {
            final EntityConfig entityConfig = new EntityConfig();
            ReflectivelySetField.setField(EntityConfig.class, entityConfig, "keyAttributes", new HashMap<>(keyAttributes));
            ReflectivelySetField.setField(EntityConfig.class, entityConfig, "attributes", new HashMap<>(attributes));
            when(mockCloudWatchLogsSinkConfig.getEntityConfig()).thenReturn(entityConfig);
        }

        final Entity entity = captureStaticEntity();

        if (keyAttributes == null) {
            assertThat(entity, nullValue());
        } else {
            assertThat(entity, notNullValue());
            assertThat(entity.keyAttributes(), equalTo(keyAttributes));
            assertThat(entity.attributes(), equalTo(attributes));
        }
    }

    /**
     * Stubs a DLQ-enabled sink run and returns the {@code [region, role]} pair that was passed to
     * the {@link DlqPushHandler} constructor.  The mock is declared in a try-with-resources so it
     * is always closed, even when an assertion inside the block throws.
     * <p>
     * An arity guard fires immediately if the {@link DlqPushHandler} constructor signature changes,
     * keeping the positional {@code get(4)} / {@code get(5)} reads from silently rotting.
     */
    private String[] captureDlqRegionAndRole() {
        PluginModel dlqConfig = mock(PluginModel.class);
        when(dlqConfig.getPluginName()).thenReturn("s3");
        when(dlqConfig.getPluginSettings()).thenReturn(new HashMap<>());
        when(mockCloudWatchLogsSinkConfig.getDlq()).thenReturn(dlqConfig);
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(mockHeaderOverrides);

        final String[] captured = new String[2]; // [region, role]

        try (MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class);
             MockedConstruction<DlqPushHandler> dlqMock = mockConstruction(DlqPushHandler.class, (mock, context) -> {
                 assertThat(context.arguments().size(), equalTo(EXPECTED_DLQ_PUSH_HANDLER_ARITY));
                 captured[0] = (String) context.arguments().get(4); // region
                 captured[1] = (String) context.arguments().get(5); // role
             })) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            getTestCloudWatchSink().doInitialize();

            assertThat(dlqMock.constructed().size(), equalTo(1));
        }
        return captured;
    }

    static Stream<Arguments> dlqRegionAndRoleSources() {
        return Stream.of(
                // awsConfig has no region and no sts_role_arn: supplier defaults are used for both
                arguments(null, null, "us-east-1", "arn:aws:iam::123456789012:role/DefaultRole",
                        "us-east-1", "arn:aws:iam::123456789012:role/DefaultRole"),
                // awsConfig has a region but no sts_role_arn: role falls back to the supplier default
                arguments("eu-west-1", null, null, "arn:aws:iam::123456789012:role/SupplierRole",
                        "eu-west-1", "arn:aws:iam::123456789012:role/SupplierRole"),
                // awsConfig has sts_role_arn but no region: region falls back to the supplier default
                arguments(null, "arn:aws:iam::123456789012:role/SinkRole", "ap-southeast-1", null,
                        "ap-southeast-1", "arn:aws:iam::123456789012:role/SinkRole"));
    }

    @ParameterizedTest
    @MethodSource("dlqRegionAndRoleSources")
    void WHEN_dlq_configured_THEN_each_of_region_and_role_falls_back_to_credentials_supplier_default_when_awsConfig_lacks_it(
            final String configRegion, final String configRole, final String supplierRegion, final String supplierRole,
            final String expectedRegion, final String expectedRole) {
        when(mockAwsConfig.getAwsRegion()).thenReturn(configRegion == null ? null : Region.of(configRegion));
        when(mockAwsConfig.getAwsStsRoleArn()).thenReturn(configRole);
        if (supplierRegion != null) {
            when(mockCredentialSupplier.getDefaultRegion()).thenReturn(Optional.of(Region.of(supplierRegion)));
        }
        if (supplierRole != null) {
            when(mockCredentialSupplier.getDefaultStsRoleArn()).thenReturn(Optional.of(supplierRole));
        }

        final String[] result = captureDlqRegionAndRole();

        assertThat(result[0], equalTo(expectedRegion));
        assertThat(result[1], equalTo(expectedRole));
    }

    @Test
    void constructor_whenDynamicHeaderConfigured_passesMetricsToEntityResolver() {
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(Map.of("x-dynamic", TEMPLATE_HEADER));
        when(mockExpressionEvaluator.extractDynamicExpressionsFromFormatExpression(TEMPLATE_HEADER))
                .thenReturn(List.of("getAzureDataSourceNameType(/resourceId, /category)"));

        final Object[] resolverMetrics = new Object[1];
        try (MockedStatic<CloudWatchLogsClientFactory> mockedStatic = mockStatic(CloudWatchLogsClientFactory.class);
             MockedConstruction<EntityResolver> resolverMock = mockConstruction(EntityResolver.class, (mock, context) ->
                     resolverMetrics[0] = context.arguments().get(context.arguments().size() - 1))) {
            mockedStatic.when(() -> CloudWatchLogsClientFactory.createCwlClient(any(AwsConfig.class),
                            any(AwsCredentialsSupplier.class), any(), any()))
                    .thenReturn(mockClient);

            getTestCloudWatchSink();

            assertThat(resolverMock.constructed().size(), equalTo(1));
        }
        assertThat(resolverMetrics[0], instanceOf(CloudWatchLogsMetrics.class));
    }

    @Test
    void doOutput_dynamicHeaderConfigured_groupedBatchDelivered() throws Exception {
        setThreshold("batchSize", 1);
        when(mockCloudWatchLogsSinkConfig.getHeaderOverrides()).thenReturn(Map.of("x-dynamic", TEMPLATE_HEADER));
        when(mockExpressionEvaluator.extractDynamicExpressionsFromFormatExpression(TEMPLATE_HEADER))
                .thenReturn(List.of("getAzureDataSourceNameType(/resourceId, /category)"));
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.NONE);
        final List<Record<Event>> records = records(1);
        when(records.get(0).getData().formatString(eq(TEMPLATE_HEADER), any(), eq(""))).thenReturn("value");

        sink.doOutput(records);

        verify(mockClient).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(records, true);
        assertDispositionsConserved();
    }

    @ParameterizedTest
    @EnumSource(DlqMode.class)
    void doOutput_oversizedEvent_goesToUndeliverableTerminalStep(final DlqMode dlqMode) throws Exception {
        setThreshold("maxEventSize", ByteCount.parse("100b"));
        final CloudWatchLogsSink sink = createSink(pool(2), dlqMode);
        final Record<Event> oversized = record(OVERSIZED_MESSAGE);

        sink.doOutput(List.of(oversized));
        sink.shutdown();

        verify(mockClient, never()).putLogEvents(any(PutLogEventsRequest.class));
        final Map<String, Integer> dispositions = dispositions();
        switch (dlqMode) {
            case NONE:
                assertReleasedOnce(handleOf(oversized), true);
                verify(metrics).increaseLogLargeEventsDroppedCounter(1);
                assertThat(dispositions, equalTo(Map.of("releasedWithoutDelivery:undeliverable", 1)));
                break;
            case OK:
                assertReleasedOnce(handleOf(oversized), true);
                assertThat(dispositions, equalTo(Map.of("sentToDlq:undeliverable", 1)));
                break;
            default:
                assertReleasedOnce(handleOf(oversized), false);
                assertThat(dispositions, equalTo(Map.of("negativelyAcked:dlqWriteFailed", 1)));
                break;
        }
        assertThat(performed, hasSize(dlqMode == DlqMode.NONE ? 0 : 1));
    }

    static Stream<Arguments> serializationFailures() {
        return Stream.of(arguments(new RuntimeException("serialization failed")), arguments(new StackOverflowError()));
    }

    @ParameterizedTest
    @MethodSource("serializationFailures")
    void doOutput_serializationThrows_eventKTerminalRestDelivered(final Throwable failure) throws Exception {
        setThreshold("batchSize", 2);
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.NONE);
        final List<Record<Event>> records = records(3);
        when(records.get(1).getData().toJsonString()).thenThrow(failure);

        sink.doOutput(records);

        final ArgumentCaptor<PutLogEventsRequest> request = ArgumentCaptor.forClass(PutLogEventsRequest.class);
        verify(mockClient).putLogEvents(request.capture());
        assertThat(request.getValue().logEvents(), hasSize(2));
        assertReleasedOnce(handleOf(records.get(0)), true);
        assertReleasedOnce(handleOf(records.get(1)), false);
        assertReleasedOnce(handleOf(records.get(2)), true);
        assertThat(dispositions(), equalTo(Map.of("succeeded", 2, "negativelyAcked:unexpectedError", 1)));
        verify(metrics).increaseUnhandledErrorCounter(1);
    }

    @Test
    void doOutput_serializationThrowsOutOfMemoryError_propagates() {
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.NONE);
        final List<Record<Event>> records = records(2);
        when(records.get(0).getData().toJsonString()).thenThrow(new OutOfMemoryError("test"));

        assertThrows(OutOfMemoryError.class, () -> sink.doOutput(records));
    }

    static Stream<Arguments> groupingFailures() {
        return Stream.of(arguments(new RuntimeException("resolveKey failed")), arguments(new StackOverflowError()));
    }

    // A grouping failure under the lock, including StackOverflowError from a header or entity function, sends event k to the
    // terminal step only after the lock is released; the other events still reach their groups.
    @ParameterizedTest
    @MethodSource("groupingFailures")
    void doOutput_groupingThrowsUnderLock_eventKTerminalAfterUnlock(final Throwable failure) {
        final List<Record<Event>> records = records(3);
        final Event failing = records.get(1).getData();
        final AtomicReference<CloudWatchLogsSink> sinkReference = new AtomicReference<>();
        final List<Boolean> lockHeldAtRelease = new ArrayList<>();
        final EventHandle failingHandle = handleOf(records.get(1));
        doAnswer(invocation -> {
            lockHeldAtRelease.add(sinkReference.get().getProcessLock().isLocked());
            return true;
        }).when(failingHandle).release(anyBoolean());
        try (MockedConstruction<CloudWatchLogsService> serviceMock = mockConstruction(CloudWatchLogsService.class,
                (mock, context) -> doThrow(failure).when(mock).add(eq(failing), any(), any()))) {
            sinkReference.set(createSink(pool(2), DlqMode.NONE));

            sinkReference.get().doOutput(records);

            final CloudWatchLogsService service = serviceMock.constructed().get(0);
            verify(service).add(eq(records.get(0).getData()), any(), any());
            verify(service).add(eq(records.get(2).getData()), any(), any());
        }

        assertReleasedOnce(handleOf(records.get(1)), false);
        assertThat(lockHeldAtRelease, equalTo(List.of(false)));
        verify(handleOf(records.get(0)), never()).release(anyBoolean());
        verify(handleOf(records.get(2)), never()).release(anyBoolean());
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:unexpectedError", 1)));
    }

    // An empty doOutput call is still a readiness trigger; whatever takeReady returns is flushed.
    @Test
    void doOutput_emptyCollection_takesReadyGroups() {
        final List<Record<Event>> buffered = records(2);
        final AtomicReference<CloudWatchLogsSink> sinkReference = new AtomicReference<>();
        final List<Boolean> lockHeldInTakeReady = new ArrayList<>();
        try (MockedConstruction<CloudWatchLogsService> serviceMock = mockConstruction(CloudWatchLogsService.class,
                (mock, context) -> doAnswer(invocation -> {
                    lockHeldInTakeReady.add(sinkReference.get().getProcessLock().isHeldByCurrentThread());
                    invocation.<List<Batch>>getArgument(0).add(batchOf(buffered));
                    return null;
                }).when(mock).takeReady(any()))) {
            sinkReference.set(createSink(pool(2), DlqMode.NONE));

            sinkReference.get().doOutput(Collections.emptyList());
        }

        assertThat(lockHeldInTakeReady, equalTo(List.of(true)));
        verify(mockClient).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(buffered, true);
        assertDispositionsConserved();
    }

    @ParameterizedTest
    @EnumSource(value = DlqMode.class, names = {"NONE", "OK"})
    void shutdown_bufferedEvents_flushedWithOneAttempt(final DlqMode dlqMode) {
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(new RuntimeException("unavailable"));
        final CloudWatchLogsSink sink = createSink(pool(2), dlqMode);
        final List<Record<Event>> records = records(2);
        sink.doOutput(records);
        verify(mockClient, never()).putLogEvents(any(PutLogEventsRequest.class));

        sink.shutdown();

        verify(mockClient, times(1)).putLogEvents(any(PutLogEventsRequest.class));
        if (dlqMode == DlqMode.NONE) {
            assertReleasedOnce(records, false);
            assertThat(dispositions(), equalTo(Map.of("negativelyAcked:shutdown", 2)));
        } else {
            assertReleasedOnce(records, true);
            assertThat(dispositions(), equalTo(Map.of("sentToDlq:shutdown", 2)));
            assertThat(performed, hasSize(1));
        }
        assertDispositionsConserved();
    }

    @Test
    @Timeout(30)
    void shutdown_calledTwiceOrConcurrently_secondReturnsImmediately() throws Exception {
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.NONE);
        final List<Record<Event>> records = records(1);
        sink.doOutput(records);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        stubClientBlocking(entered, proceed);

        final Thread first = start(sink::shutdown);
        entered.await();
        final Thread second = start(sink::shutdown);
        second.join();

        assertThat(first.isAlive(), equalTo(true));
        proceed.countDown();
        first.join();
        sink.shutdown();
        verify(mockClient, times(1)).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(records, true);
        assertDispositionsConserved();
    }

    @ParameterizedTest
    @EnumSource(value = DlqMode.class, names = {"NONE", "OK"})
    void doOutput_afterShutdown_refusedWithShutdownReason(final DlqMode dlqMode) {
        final CloudWatchLogsSink sink = createSink(pool(2), dlqMode);
        sink.shutdown();
        final List<Record<Event>> records = records(2);

        sink.doOutput(records);

        verify(mockClient, never()).putLogEvents(any(PutLogEventsRequest.class));
        if (dlqMode == DlqMode.NONE) {
            assertReleasedOnce(records, false);
            assertThat(dispositions(), equalTo(Map.of("negativelyAcked:shutdown", 2)));
        } else {
            assertReleasedOnce(records, true);
            assertThat(dispositions(), equalTo(Map.of("sentToDlq:shutdown", 2)));
        }
        assertDispositionsConserved();
    }

    // A task outliving TERMINATION_WAIT still releases its batch exactly once when it finishes.
    @Test
    @Timeout(30)
    void shutdown_poolShutdownNeverShutdownNow_logsErrorWhenTaskOutlivesTerminationWait() throws Exception {
        setThreshold("batchSize", 1);
        final ThreadPoolExecutor pool = spyPool(1);
        doReturn(false).when(pool).awaitTermination(anyLong(), any(TimeUnit.class));
        final CloudWatchLogsSink sink = createSink(pool, DlqMode.NONE);
        final List<Record<Event>> records = records(1);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        stubClientBlocking(entered, proceed);
        final Thread caller = start(() -> sink.doOutput(records));
        entered.await();

        sink.shutdown();

        assertThat(caller.isAlive(), equalTo(true));
        verify(pool).shutdown();
        verify(pool, never()).shutdownNow();
        verify(pool).awaitTermination(eq(CloudWatchLogsSink.TERMINATION_WAIT.toNanos()), eq(TimeUnit.NANOSECONDS));
        // The active count is read only by the error log for a pool that did not terminate in time.
        verify(pool).getActiveCount();
        verify(handleOf(records.get(0)), never()).release(anyBoolean());
        proceed.countDown();
        caller.join();
        assertReleasedOnce(records, true);
        assertDispositionsConserved();
    }

    // Attempts that would start at or past the shutdown deadline are skipped and settled as shutdown; no handle is acked true
    // without a 2xx.
    @Test
    void shutdown_moreBatchesThanWorkers_boundedAndPastDeadlineSkipped() {
        final List<Record<Event>> records = records(4);
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            now.addAndGet(CloudWatchLogsClientFactory.API_CALL_TIMEOUT.toMillis());
            return PutLogEventsResponse.builder().build();
        });
        final CloudWatchLogsSink sink;
        try (MockedConstruction<CloudWatchLogsService> serviceMock = mockConstruction(CloudWatchLogsService.class,
                (mock, context) -> doAnswer(invocation -> {
                    records.forEach(record -> invocation.<List<Batch>>getArgument(0).add(batchOf(List.of(record))));
                    return null;
                }).when(mock).takeAll(any()))) {
            sink = createSink(pool(1), DlqMode.NONE);
        }

        sink.shutdown();

        // Deadline = start + 30 s, and each call takes 15 s, so only the first two batches start before it.
        verify(mockClient, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(handleOf(records.get(0)), true);
        assertReleasedOnce(handleOf(records.get(1)), true);
        assertReleasedOnce(handleOf(records.get(2)), false);
        assertReleasedOnce(handleOf(records.get(3)), false);
        assertThat(dispositions(), equalTo(Map.of("succeeded", 2, "negativelyAcked:shutdown", 2)));
    }

    enum LockPath { OVERSIZED, SERIALIZATION_FAILURE, REFUSED_AFTER_SHUTDOWN, BATCH_SEND_AND_DLQ }

    @ParameterizedTest
    @EnumSource(LockPath.class)
    void doOutput_ioAndDlqPaths_processLockNotHeld(final LockPath path) throws Exception {
        setThreshold("batchSize", 1);
        setThreshold("maxEventSize", ByteCount.parse("100b"));
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.OK);
        performHook = () -> probe(sink);
        final List<Record<Event>> records = records(1);
        switch (path) {
            case OVERSIZED:
                when(records.get(0).getData().toJsonString()).thenReturn(OVERSIZED_MESSAGE);
                break;
            case SERIALIZATION_FAILURE:
                when(records.get(0).getData().toJsonString()).thenThrow(new RuntimeException("serialization failed"));
                break;
            case REFUSED_AFTER_SHUTDOWN:
                sink.shutdown();
                break;
            default:
                when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
                    probe(sink);
                    throw new RuntimeException("unavailable");
                });
                break;
        }

        sink.doOutput(records);

        assertThat(lockProbes.isEmpty(), equalTo(false));
        assertThat(lockProbes, everyItem(equalTo(true)));
        assertThat(performed, hasSize(1));
        assertReleasedOnce(records, true);
    }

    private void probe(final CloudWatchLogsSink sink) {
        try {
            lockProbes.add(lockFreeFromAnotherThread(sink.getProcessLock()));
        } catch (final Exception e) {
            lockProbes.add(false);
        }
    }

    @Test
    @Timeout(30)
    void doOutput_handleAcquiredByTwoSinks_releasedOnceSetCompletesAfterOther() throws Exception {
        setThreshold("batchSize", 1);
        final ScheduledExecutorService ackExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            final CompletableFuture<Boolean> callback = new CompletableFuture<>();
            final DefaultAcknowledgementSet acknowledgementSet = new DefaultAcknowledgementSet(ackExecutor, callback::complete,
                    Duration.ofMinutes(5), mock(DefaultAcknowledgementSetMetrics.class));
            final Event event = JacksonEvent.fromMessage("shared");
            acknowledgementSet.add(event.getEventHandle());
            ((DefaultEventHandle) event.getEventHandle()).acquireReference();
            acknowledgementSet.complete();
            final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.NONE);

            sink.doOutput(List.of(new Record<>(event)));
            sink.shutdown();
            // Runs after any callback a second release from this sink would have scheduled.
            ackExecutor.submit(() -> { }).get();

            verify(mockClient).putLogEvents(any(PutLogEventsRequest.class));
            assertThat(callback.isDone(), equalTo(false));
            assertThat(dispositions(), equalTo(Map.of("succeeded", 1)));
            event.getEventHandle().release(true);
            assertThat(callback.get(10, TimeUnit.SECONDS), equalTo(true));
        } finally {
            ackExecutor.shutdownNow();
        }
    }

    // Tasks queued behind a full pool when shutdown runs make one attempt each, and the blocked doOutput returns only after they
    // finish.
    @ParameterizedTest
    @EnumSource(value = DlqMode.class, names = {"NONE", "OK"})
    @Timeout(30)
    void shutdown_racingBlockedDoOutput_queuedTasksOneAttemptEachReleasedOnce(final DlqMode dlqMode) throws Exception {
        setThreshold("batchSize", 1);
        final ThreadPoolExecutor pool = spyPool(1);
        final CountDownLatch poolShutdown = new CountDownLatch(1);
        doAnswer(invocation -> {
            invocation.callRealMethod();
            poolShutdown.countDown();
            return null;
        }).when(pool).shutdown();
        final CloudWatchLogsSink sink = createSink(pool, dlqMode);
        final List<Record<Event>> records = records(3);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            entered.countDown();
            proceed.await();
            throw new RuntimeException("unavailable");
        });
        final Thread caller = start(() -> sink.doOutput(records));
        entered.await();

        final Thread shutdown = start(sink::shutdown);
        poolShutdown.await();
        proceed.countDown();
        caller.join();
        shutdown.join();

        verify(mockClient, times(3)).putLogEvents(any(PutLogEventsRequest.class));
        if (dlqMode == DlqMode.NONE) {
            assertReleasedOnce(records, false);
            assertThat(dispositions(), equalTo(Map.of("negativelyAcked:shutdown", 3)));
        } else {
            assertReleasedOnce(records, true);
            assertThat(dispositions(), equalTo(Map.of("sentToDlq:shutdown", 3)));
        }
        assertDispositionsConserved();
    }

    @Test
    @Timeout(30)
    void shutdown_senderInBackoff_stopsAfterCurrentAttempt() throws Exception {
        setThreshold("batchSize", 1);
        when(random.nextDouble()).thenReturn(0.99);
        final CountDownLatch failed = new CountDownLatch(1);
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            failed.countDown();
            throw new RuntimeException("unavailable");
        });
        final CloudWatchLogsSink sink = createSink(pool(1), DlqMode.NONE);
        final List<Record<Event>> records = records(1);
        final Thread caller = start(() -> sink.doOutput(records));
        failed.await();

        sink.shutdown();
        caller.join();

        verify(mockClient, times(1)).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(records, false);
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:shutdown", 1)));
    }

    @Test
    void shutdown_interruptFlagSetOnEntry_stillFlushesAndRestoresFlag() {
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.NONE);
        final List<Record<Event>> records = records(2);
        sink.doOutput(records);
        Thread.currentThread().interrupt();

        sink.shutdown();

        assertThat(Thread.interrupted(), equalTo(true));
        verify(mockClient).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(records, true);
        assertDispositionsConserved();
    }

    @Test
    @Timeout(30)
    void shutdown_interruptedDuringFlushWait_stillWaitsAndRestoresFlag() throws Exception {
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.NONE);
        final List<Record<Event>> records = records(2);
        sink.doOutput(records);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        stubClientBlocking(entered, proceed);
        final AtomicBoolean interruptedOnReturn = new AtomicBoolean();
        final AtomicBoolean releasedBeforeReturn = new AtomicBoolean();

        final Thread shutdown = start(() -> {
            sink.shutdown();
            releasedBeforeReturn.set(!mockingDetails(handleOf(records.get(0))).getInvocations().isEmpty());
            interruptedOnReturn.set(Thread.currentThread().isInterrupted());
        });
        entered.await();
        shutdown.interrupt();
        proceed.countDown();
        shutdown.join();

        assertThat(interruptedOnReturn.get(), equalTo(true));
        assertThat(releasedBeforeReturn.get(), equalTo(true));
        assertReleasedOnce(records, true);
        assertDispositionsConserved();
    }

    static Stream<Arguments> unexpectedFailures() {
        return Stream.of(
                arguments("takeReady", new RuntimeException("unexpected")),
                arguments("takeReady", new StackOverflowError()),
                arguments("submit", new RuntimeException("unexpected")),
                arguments("submit", new StackOverflowError()));
    }

    @ParameterizedTest
    @MethodSource("unexpectedFailures")
    void doOutput_takeReadyOrFlushThrows_returnsNormallyAllReleasedOnce(final String where, final Throwable failure) throws Exception {
        setThreshold("batchSize", 1);
        final List<Record<Event>> records = records(1);
        final CloudWatchLogsSink sink;
        if (where.equals("takeReady")) {
            try (MockedConstruction<CloudWatchLogsService> serviceMock = mockConstruction(CloudWatchLogsService.class,
                    (mock, context) -> doAnswer(invocation -> {
                        invocation.<List<Batch>>getArgument(0).add(batchOf(records));
                        throw failure;
                    }).when(mock).takeReady(any()))) {
                sink = createSink(pool(2), DlqMode.NONE);
            }
        } else {
            final ThreadPoolExecutor pool = spyPool(2);
            doThrow(failure).when(pool).submit(any(Runnable.class));
            sink = createSink(pool, DlqMode.NONE);
        }

        sink.doOutput(records);

        verify(mockClient, never()).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(records, false);
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:unexpectedError", 1)));
    }

    @Test
    void doOutput_takeReadyThrows_oversizedEventStillTakesUndeliverableTerminalStep() throws Exception {
        setThreshold("maxEventSize", ByteCount.parse("100b"));
        final Record<Event> oversized = record(OVERSIZED_MESSAGE);
        final CloudWatchLogsSink sink;
        try (MockedConstruction<CloudWatchLogsService> serviceMock = mockConstruction(CloudWatchLogsService.class,
                (mock, context) -> doThrow(new RuntimeException("unexpected")).when(mock).takeReady(any()))) {
            sink = createSink(pool(2), DlqMode.NONE);
        }

        sink.doOutput(List.of(oversized));

        assertReleasedOnce(handleOf(oversized), true);
        assertThat(dispositions(), equalTo(Map.of("releasedWithoutDelivery:undeliverable", 1)));
    }

    @Test
    void doOutput_recordWithNullEventAndSubmitThrows_returnsNormallyOthersReleasedOnce() throws Exception {
        setThreshold("batchSize", 1);
        final ThreadPoolExecutor pool = spyPool(2);
        doThrow(new RuntimeException("unexpected")).when(pool).submit(any(Runnable.class));
        final CloudWatchLogsSink sink = createSink(pool, DlqMode.NONE);
        final List<Record<Event>> records = records(1);
        final List<Record<Event>> received = new ArrayList<>();
        received.add(new Record<>(null));
        received.addAll(records);

        sink.doOutput(received);

        assertReleasedOnce(records, false);
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:unexpectedError", 1)));
    }

    static Stream<Arguments> unexpectedShutdownFailures() {
        return Stream.of(
                arguments("takeAll", new RuntimeException("unexpected")),
                arguments("takeAll", new StackOverflowError()),
                arguments("submit", new RuntimeException("unexpected")),
                arguments("submit", new StackOverflowError()));
    }

    // The taken batches, and the groups a takeAll that threw part-way had not reached, are settled as shutdown.
    @ParameterizedTest
    @MethodSource("unexpectedShutdownFailures")
    void shutdown_takeAllOrSubmitThrows_returnsNormallySettledAsShutdownReleasedOnce(final String where, final Throwable failure) {
        final List<Record<Event>> records = records(2);
        final CloudWatchLogsSink sink;
        if (where.equals("takeAll")) {
            final AtomicInteger takeAllCalls = new AtomicInteger();
            try (MockedConstruction<CloudWatchLogsService> serviceMock = mockConstruction(CloudWatchLogsService.class,
                    (mock, context) -> doAnswer(invocation -> {
                        if (takeAllCalls.incrementAndGet() == 1) {
                            invocation.<List<Batch>>getArgument(0).add(batchOf(records.subList(0, 1)));
                            throw failure;
                        }
                        invocation.<List<Batch>>getArgument(0).add(batchOf(records.subList(1, 2)));
                        return null;
                    }).when(mock).takeAll(any()))) {
                sink = createSink(pool(2), DlqMode.NONE);
            }
        } else {
            final ThreadPoolExecutor pool = spyPool(2);
            doThrow(failure).when(pool).submit(any(Runnable.class));
            sink = createSink(pool, DlqMode.NONE);
            sink.doOutput(records);
        }

        sink.shutdown();

        verify(mockClient, never()).putLogEvents(any(PutLogEventsRequest.class));
        assertReleasedOnce(records, false);
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:shutdown", 2)));
    }

    @Test
    void doOutput_refusalsAndOversized_onePerformPerReason() throws Exception {
        setThreshold("maxEventSize", ByteCount.parse("100b"));
        final CloudWatchLogsSink sink = createSink(pool(2), DlqMode.OK);
        sink.shutdown();
        final List<Record<Event>> oversized = List.of(record(OVERSIZED_MESSAGE), record(OVERSIZED_MESSAGE));
        final List<Record<Event>> refused = records(2);
        final List<Record<Event>> all = new ArrayList<>(oversized);
        all.addAll(refused);

        sink.doOutput(all);

        assertThat(performed, hasSize(2));
        assertDlqPayload(performed.get(0), oversized, "Event blocked due to Max Size restriction! Event Size : 226");
        assertDlqPayload(performed.get(1), refused, "shutdown");
        assertThat(dispositions(), equalTo(Map.of("sentToDlq:undeliverable", 2, "sentToDlq:shutdown", 2)));
        assertReleasedOnce(all, true);
    }

    private static void assertDlqPayload(final List<DlqObject> dlqObjects, final List<Record<Event>> records,
                                         final String failureMessage) {
        assertThat(dlqObjects, hasSize(records.size()));
        for (int i = 0; i < records.size(); i++) {
            final DlqObject dlqObject = dlqObjects.get(i);
            assertThat(dlqObject.getEventHandles(), equalTo(List.of(handleOf(records.get(i)))));
            assertThat(dlqObject.getFailedData(), instanceOf(CloudWatchLogsSinkDlqData.class));
            final CloudWatchLogsSinkDlqData data = (CloudWatchLogsSinkDlqData) dlqObject.getFailedData();
            assertThat(data.getStatus(), equalTo(0));
            assertThat(data.getData(), equalTo(records.get(i).getData().toJsonString()));
            assertThat(data.getMessage(), startsWith(failureMessage));
            assertThat(data.getMessage(), not(equalTo("")));
        }
    }
}
