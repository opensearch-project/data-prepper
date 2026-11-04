package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudWatchLogsMetricsTest {
    private CloudWatchLogsMetrics testCloudWatchLogsMetrics;
    private PluginMetrics mockPluginMetrics;
    private final Map<String, Object> meters = new HashMap<>();

    @BeforeEach
    void setUp() {
        mockPluginMetrics = mock(PluginMetrics.class);
        when(mockPluginMetrics.counter(anyString())).thenAnswer(i -> meters.computeIfAbsent(i.getArgument(0), k -> mock(Counter.class)));
        when(mockPluginMetrics.summary(anyString()))
                .thenAnswer(i -> meters.computeIfAbsent(i.getArgument(0), k -> mock(DistributionSummary.class)));
        when(mockPluginMetrics.counterWithTags(anyString(), anyString(), anyString())).thenAnswer(i -> meters.computeIfAbsent(
                i.getArgument(0) + "/" + i.getArgument(1) + "=" + i.getArgument(2), k -> mock(Counter.class)));
        testCloudWatchLogsMetrics = new CloudWatchLogsMetrics(mockPluginMetrics);
    }

    @Test
    void WHEN_increase_entity_dropped_counter_called_THEN_counter_tagged_with_reason_is_incremented() {
        final Counter droppedCounter = mock(Counter.class);
        when(mockPluginMetrics.counterWithTags(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_DROPPED,
                CloudWatchLogsMetrics.REASON_TAG, "blank_value")).thenReturn(droppedCounter);

        testCloudWatchLogsMetrics.increaseEntityDroppedCounter("blank_value");

        verify(droppedCounter).increment();
    }

    @ParameterizedTest
    @MethodSource("counters")
    void counter_increased_incrementsOnlyThatCounter(final String name, final Consumer<CloudWatchLogsMetrics> action, final int expected) {
        action.accept(testCloudWatchLogsMetrics);
        verify((Counter) meters.get(name)).increment(expected);
        meters.entrySet().stream()
                .filter(e -> !e.getKey().equals(name) && e.getValue() instanceof Counter)
                .forEach(e -> verify((Counter) e.getValue(), never()).increment(anyDouble()));
    }

    static Stream<Arguments> counters() {
        return Stream.of(
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SUCCEEDED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseLogEventSuccessCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseRequestSuccessCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_FAILED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseLogEventFailCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseRequestFailCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_REJECTED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseEntityRejectedCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_UNHANDLED_ERROR,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseUnhandledErrorCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ACCESS_DENIED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseAccessDeniedCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseResourceNotFoundCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_THROTTLED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseThrottledCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_GROUPS_CREATED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseEntityGroupsCreatedCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_OVERFLOW_EVENTS,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseEntityOverflowEventsCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUEST_MULTI_FAILED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseRequestMultiFailCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_LARGE_EVENTS_DROPPED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseLogLargeEventsDroppedCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_TEMPLATE_EXPRESSION_FAILED,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseTemplateExpressionFailedCounter(1), 1),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_HEADER_VALUE_BLANK,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseHeaderValueBlankCounter(2), 2),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND_AFTER_CREATE,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseResourceNotFoundAfterCreateCounter(1), 1));
    }

    @ParameterizedTest
    @MethodSource("summaries")
    void summary_recorded_recordsThatSummary(final String name, final Consumer<CloudWatchLogsMetrics> action, final double expected) {
        action.accept(testCloudWatchLogsMetrics);
        verify((DistributionSummary) meters.get(name)).record(expected);
    }

    static Stream<Arguments> summaries() {
        return Stream.of(
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_STAGED_AGE_AT_DELIVERY,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.recordStagedAgeAtDelivery(1234L), 1234.0),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUEST_SIZE,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.recordRequestSize(100), 100.0),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_LOG_SIZE,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.recordLogSize(50), 50.0));
    }

    @Test
    void constructor_dispositionCounters_registersExactlySpecTagSetsUpFront() {
        final String released = CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_RELEASED_WITHOUT_DELIVERY + "/reason=";
        final String negativelyAcked = "cloudWatchLogsEventsNegativelyAcked/reason=";
        final String sentToDlq = "cloudWatchLogsEventsSentToDlq/reason=";
        final Set<String> expected = Set.of(
                released + "undeliverable", released + "rejected",
                negativelyAcked + "retriesExhausted", negativelyAcked + "permanentFailure", negativelyAcked + "unexpectedError",
                negativelyAcked + "dlqWriteFailed", negativelyAcked + "shutdown",
                sentToDlq + "undeliverable", sentToDlq + "rejected", sentToDlq + "retriesExhausted", sentToDlq + "permanentFailure",
                sentToDlq + "unexpectedError", sentToDlq + "shutdown");

        assertThat(taggedKeys(), equalTo(expected));
    }

    @ParameterizedTest
    @MethodSource("taggedIncrements")
    void taggedCounter_knownTagValue_incrementsPreRegisteredCounterForThatValue(final String key,
                                                                               final Consumer<CloudWatchLogsMetrics> action,
                                                                               final int expected) {
        action.accept(testCloudWatchLogsMetrics);
        verify((Counter) meters.get(key)).increment(expected);
    }

    static Stream<Arguments> taggedIncrements() {
        final String released = CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_RELEASED_WITHOUT_DELIVERY + "/"
                + CloudWatchLogsMetrics.REASON_TAG + "=";
        final String negativelyAcked = CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_NEGATIVELY_ACKED + "/"
                + CloudWatchLogsMetrics.REASON_TAG + "=";
        final String sentToDlq = CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SENT_TO_DLQ + "/" + CloudWatchLogsMetrics.REASON_TAG + "=";
        return Stream.of(
                CloudWatchLogsMetrics.RELEASED_WITHOUT_DELIVERY_REASONS.stream().map(r -> Arguments.of(released + r,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseReleasedWithoutDeliveryCounter(r, 5), 5)),
                CloudWatchLogsMetrics.NEGATIVELY_ACKED_REASONS.stream().map(r -> Arguments.of(negativelyAcked + r,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseEventsNegativelyAckedCounter(r, 3), 3)),
                CloudWatchLogsMetrics.SENT_TO_DLQ_REASONS.stream().map(r -> Arguments.of(sentToDlq + r,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.increaseEventsSentToDlqCounter(r, 2), 2))
        ).flatMap(s -> s);
    }

    @Test
    void taggedCounters_unknownReasonOrZeroValue_incrementsAndRegistersNothing() {
        final Set<String> registered = taggedKeys();

        testCloudWatchLogsMetrics.increaseReleasedWithoutDeliveryCounter("somethingElse", 1);
        testCloudWatchLogsMetrics.increaseReleasedWithoutDeliveryCounter(CloudWatchLogsMetrics.REASON_UNDELIVERABLE, 0);
        testCloudWatchLogsMetrics.increaseEventsNegativelyAckedCounter(CloudWatchLogsMetrics.REASON_REJECTED, 1);
        testCloudWatchLogsMetrics.increaseEventsNegativelyAckedCounter(null, 1);
        testCloudWatchLogsMetrics.increaseEventsSentToDlqCounter(CloudWatchLogsMetrics.REASON_DLQ_WRITE_FAILED, 1);
        testCloudWatchLogsMetrics.increaseEventsSentToDlqCounter(CloudWatchLogsMetrics.REASON_SHUTDOWN, 0);

        assertThat(taggedKeys(), equalTo(registered));
        meters.values().stream().filter(Counter.class::isInstance)
                .forEach(c -> verify((Counter) c, never()).increment(anyDouble()));
    }

    // A metric call must not throw, even when the registry returns no counter.
    @Test
    void taggedCounters_pluginMetricsReturnsNullCounters_doNotThrow() {
        final CloudWatchLogsMetrics metrics = new CloudWatchLogsMetrics(mock(PluginMetrics.class));
        metrics.increaseReleasedWithoutDeliveryCounter(CloudWatchLogsMetrics.REASON_REJECTED, 1);
        metrics.increaseEventsNegativelyAckedCounter(CloudWatchLogsMetrics.REASON_SHUTDOWN, 1);
        metrics.increaseEventsSentToDlqCounter(CloudWatchLogsMetrics.REASON_UNDELIVERABLE, 1);
    }

    // These metrics were removed and must not be registered again.
    @ParameterizedTest
    @ValueSource(strings = {"cloudWatchLogsEventsRejected", "cloudWatchLogsRequestsInRetry", "cloudWatchLogsOldestRetryAgeSeconds",
            "cloudWatchLogsUnretriedFailures", "cloudWatchLogsEffectiveMaxRetries"})
    void constructor_removedGapMetrics_notRegistered(final String removedName) {
        testCloudWatchLogsMetrics.registerExecutorGauges(mock(ThreadPoolExecutor.class));
        testCloudWatchLogsMetrics.registerRequestsInFlightGauge(new AtomicInteger());
        testCloudWatchLogsMetrics.registerEntityCardinalityGauge(new AtomicInteger(), AtomicInteger::get);

        assertThat(meters.keySet().stream().anyMatch(k -> k.equals(removedName) || k.startsWith(removedName + "/")), equalTo(false));
        verify(mockPluginMetrics, never()).gauge(eq(removedName), any(), any());
    }

    @ParameterizedTest
    @MethodSource("gauges")
    @SuppressWarnings("unchecked")
    void gauge_registered_samplesSuppliedStateLive(final String name, final Object expectedState,
                                                   final Consumer<CloudWatchLogsMetrics> register, final double expected,
                                                   final Runnable mutateState, final double expectedAfterMutation) {
        register.accept(testCloudWatchLogsMetrics);
        final ArgumentCaptor<ToDoubleFunction<Object>> function = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(mockPluginMetrics).gauge(eq(name), same(expectedState), function.capture());
        assertThat(function.getValue().applyAsDouble(expectedState), equalTo(expected));
        // The gauge must re-read the state on every sample; a value captured at registration would flatline.
        mutateState.run();
        assertThat(function.getValue().applyAsDouble(expectedState), equalTo(expectedAfterMutation));
    }

    static Stream<Arguments> gauges() {
        final ThreadPoolExecutor executor = mock(ThreadPoolExecutor.class);
        final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(4);
        queue.add(() -> { });
        queue.add(() -> { });
        when(executor.getQueue()).thenReturn(queue);
        when(executor.getActiveCount()).thenReturn(1);
        final Consumer<CloudWatchLogsMetrics> registerExecutor = m -> m.registerExecutorGauges(executor);
        final AtomicInteger requestsInFlight = new AtomicInteger(4);
        final AtomicInteger liveGroupCount = new AtomicInteger(3);
        return Stream.of(
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_IN_FLIGHT, requestsInFlight,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.registerRequestsInFlightGauge(requestsInFlight), 4.0,
                        (Runnable) () -> requestsInFlight.set(6), 6.0),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EXECUTOR_QUEUE_DEPTH, executor, registerExecutor, 2.0,
                        (Runnable) () -> queue.add(() -> { }), 3.0),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EXECUTOR_ACTIVE_THREADS, executor, registerExecutor, 1.0,
                        (Runnable) () -> when(executor.getActiveCount()).thenReturn(2), 2.0),
                Arguments.of(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_CARDINALITY, liveGroupCount,
                        (Consumer<CloudWatchLogsMetrics>) m -> m.registerEntityCardinalityGauge(liveGroupCount, AtomicInteger::get), 3.0,
                        (Runnable) () -> liveGroupCount.set(7), 7.0));
    }

    private Set<String> taggedKeys() {
        return meters.keySet().stream().filter(k -> k.contains("=")).collect(Collectors.toSet());
    }
}
