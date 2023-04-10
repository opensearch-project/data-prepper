/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.meter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import software.amazon.cloudwatchlogs.emf.environment.EnvironmentProvider;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.MetricsContext;
import software.amazon.cloudwatchlogs.emf.model.Unit;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.micrometer.core.instrument.Meter.Id;
import static io.micrometer.core.instrument.Meter.Type;
import static io.micrometer.core.instrument.Meter.Type.DISTRIBUTION_SUMMARY;
import static io.micrometer.core.instrument.Meter.Type.TIMER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EMFLoggingMeterRegistryTest {
    private static final String METER_NAME = "test";
    private static final String SERVICE_NAME = "DataPrepper";
    private static final String TEST_TAG_KEY = "testTagKey";
    private static final String TEST_TAG_VALUE = "testTagValue";
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MockClock clock = new MockClock();
    private final EMFLoggingMeterRegistry registry = spy(
            new EMFLoggingMeterRegistry(
                    EMFLoggingRegistryConfig.DEFAULT, new EnvironmentProvider().resolveEnvironment().join(), clock)
    );
    private final EMFLoggingMeterRegistry.Snapshot registrySnapshot = registry.new Snapshot();

    @Test
    void snapshotGetMetricName() {
        final Meter.Id id = new Meter.Id("name", Tags.empty(), null, null, Meter.Type.COUNTER);
        assertThat(registrySnapshot.getMetricName(id, "suffix"), equalTo("name.suffix"));
    }

    @Test
    void snapshotGetMetricNameWhenSuffixIsNullShouldNotAppend() {
        Id id = new Id("name", Tags.empty(), null, null, Type.COUNTER);
        assertThat(registrySnapshot.getMetricName(id, null), equalTo("name"));
    }

    @Test
    void snapshotGaugeDataValid() {
        final Gauge gauge = Gauge.builder("gauge", 1d, Number::doubleValue)
                .tags(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot.gaugeData(gauge)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(1));
        final EMFLoggingMeterRegistry.MetricDataPoint metricDataPoint = metricDataPoints.get(0);
        assertThat(metricDataPoint.key, equalTo("gauge.value"));
        assertThat(metricDataPoint.value, equalTo(1d));
        assertThat(metricDataPoint.unit, equalTo(Unit.NONE));
    }

    @Test
    void snapshotGaugeDataWhenNaNShouldNotAdd() {
        final Gauge gauge = Gauge.builder("gauge", Double.NaN, Number::doubleValue)
                .tags(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot.gaugeData(gauge)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(0));
    }

    @Test
    void snapshotCounterMetricDataValid() {
        final Counter counter = Counter.builder("counter").tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot.counterData(counter)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(1));
        final EMFLoggingMeterRegistry.MetricDataPoint metricDataPoint = metricDataPoints.get(0);
        assertThat(metricDataPoint.key, equalTo("counter.count"));
        assertThat(metricDataPoint.value, equalTo(0d));
        assertThat(metricDataPoint.unit, equalTo(Unit.COUNT));
    }

    @Test
    void snapshotShouldAddTimerAggregateDataWhenAtLeastOneEventHappened() {
        final Timer timer = mock(Timer.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(2L);

        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot.timerData(timer)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(4));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .containsAll(List.of("test.count", "test.sum", "test.avg", "test.max")), is(true));
    }

    @Test
    void snapshotShouldNotAddTimerAggregateDataWhenNoEventHappened() {
        Timer timer = mock(Timer.class);
        Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(0L);

        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot.timerData(timer)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(2));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .containsAll(List.of("test.count", "test.sum")), is(true));
    }

    @Test
    void snapshotShouldAddSummaryAggregateDataWhenAtLeastOneEventHappened() {
        final DistributionSummary summary = mock(DistributionSummary.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, DISTRIBUTION_SUMMARY);
        when(summary.getId()).thenReturn(meterId);
        when(summary.count()).thenReturn(2L);

        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot.summaryData(summary)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(4));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .containsAll(List.of("test.count", "test.sum", "test.avg", "test.max")), is(true));
    }

    @Test
    void shouldNotAddDistributionSumAggregateDataWhenNoEventHappened() {
        final DistributionSummary summary = mock(DistributionSummary.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, DISTRIBUTION_SUMMARY);
        when(summary.getId()).thenReturn(meterId);
        when(summary.count()).thenReturn(0L);

        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot.summaryData(summary)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(2));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .containsAll(List.of("test.count", "test.sum")), is(true));
    }

    @Test
    void snapshotLongTaskTimerDataValid() {
        final LongTaskTimer longTaskTimer = LongTaskTimer.builder("longTaskTimer")
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .longTaskTimerData(longTaskTimer)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(2));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .containsAll(List.of("longTaskTimer.activeTasks", "longTaskTimer.duration")), is(true));
    }

    @Test
    void snapshotTimeGaugeDataValid() {
        final AtomicReference<Double> testValue = new AtomicReference<>(0.0);
        final TimeGauge timeGauge = TimeGauge.builder("timeGaugeData", testValue, TimeUnit.MILLISECONDS, AtomicReference::get)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .timeGaugeData(timeGauge)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(1));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .contains("timeGaugeData.value"), is(true));
    }

    @Test
    void snapshotTimeGaugeDataWhenNaNShouldNotAdd() {
        final AtomicReference<Double> testValue = new AtomicReference<>(Double.NaN);
        final TimeGauge timeGauge = TimeGauge.builder("timeGaugeData", testValue, TimeUnit.MILLISECONDS, AtomicReference::get)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .timeGaugeData(timeGauge)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(0));
    }

    @Test
    void snapshotFunctionCounterDataValid() {
        final FunctionCounter functionCounter = FunctionCounter.builder("functionCounterData", 1d, Number::doubleValue)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .functionCounterData(functionCounter)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(1));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .contains("functionCounterData.count"), is(true));
    }

    @Test
    void snapshotFunctionCounterDataShouldClampInfiniteValues() {
        FunctionCounter functionCounter = FunctionCounter
                .builder("my.positive.infinity", Double.POSITIVE_INFINITY, Number::doubleValue).register(registry);
        clock.add(EMFLoggingRegistryConfig.DEFAULT.step());
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints1 = registrySnapshot
                .functionCounterData(functionCounter)
                .collect(Collectors.toList());
        assertThat(metricDataPoints1.size(), equalTo(1));
        assertThat(metricDataPoints1.get(0).value, equalTo(1.174271e+108));

        functionCounter = FunctionCounter
                .builder("my.negative.infinity", Double.NEGATIVE_INFINITY, Number::doubleValue).register(registry);
        clock.add(EMFLoggingRegistryConfig.DEFAULT.step());
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints2 = registrySnapshot
                .functionCounterData(functionCounter)
                .collect(Collectors.toList());
        assertThat(metricDataPoints2.size(), equalTo(1));
        assertThat(metricDataPoints2.get(0).value, equalTo(-1.174271e+108));
    }

    @Test
    void snapshotShouldAddFunctionTimerAggregateDataWhenAtLeastOneEventHappened() {
        final FunctionTimer timer = mock(FunctionTimer.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(2.0);

        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .functionTimerData(timer)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(3));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .containsAll(List.of("test.count", "test.sum", "test.avg")), is(true));
    }

    @Test
    void snapshotShouldNotAddFunctionTimerAggregateDataWhenNoEventHappened() {
        final FunctionTimer timer = mock(FunctionTimer.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(0.0);

        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .functionTimerData(timer)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(2));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .containsAll(List.of("test.count", "test.sum")), is(true));
    }

    @Test
    void snapshotShouldNotAddFunctionTimerDataWhenSumIsNaN() {
        final FunctionTimer functionTimer = FunctionTimer
                .builder("my.function.timer", Double.NaN, Number::longValue, Number::doubleValue, TimeUnit.MILLISECONDS)
                .register(registry);
        clock.add(EMFLoggingRegistryConfig.DEFAULT.step());
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .functionTimerData(functionTimer)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(0));
    }

    @Test
    void snapshotCustomMeterDataValid() {
        final Measurement measurement = new Measurement(() -> 2d, Statistic.VALUE);
        final List<Measurement> measurements = Collections.singletonList(measurement);
        final Meter meter = Meter.builder(METER_NAME, Meter.Type.GAUGE, measurements)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .metricData(meter)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(1));
        assertThat(metricDataPoints.stream().map(metricDataPoint -> metricDataPoint.key).collect(Collectors.toList())
                .contains("test"), is(true));
    }

    @Test
    void snapshotCustomMeterDataWhenNaNShouldNotAdd() {
        final Measurement measurement = new Measurement(() -> Double.NaN, Statistic.VALUE);
        final List<Measurement> measurements = Collections.singletonList(measurement);
        final Meter meter = Meter.builder(METER_NAME, Meter.Type.GAUGE, measurements)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<EMFLoggingMeterRegistry.MetricDataPoint> metricDataPoints = registrySnapshot
                .metricData(meter)
                .collect(Collectors.toList());
        assertThat(metricDataPoints.size(), equalTo(0));
    }

    @Test
    void registryMetricsLoggersValid() {
        registry.gauge("gauge", 1d);
        registry.counter("counter");
        registry.timer("timer");
        registry.summary("summary");
        registry.more().longTaskTimer("longTaskTimer");
        registry.more().timeGauge("timeGauge", Tags.empty(), 1d, TimeUnit.MILLISECONDS, Number::doubleValue);
        registry.more().counter("functionCounter", Tags.empty(), 1d);
        registry.more().timer("functionTimer", Tags.empty(), 1d, Number::longValue, Number::doubleValue, TimeUnit.MILLISECONDS);
        final List<Measurement> measurement = Collections.singletonList(new Measurement(() -> 2d, Statistic.VALUE));
        Meter.builder(METER_NAME, Type.OTHER, measurement).tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final List<MetricsLogger> metricsLoggerList = registry.metricsLoggers();
        assertThat(metricsLoggerList.size(), equalTo(2));
        final MetricsLogger metricsLogger = metricsLoggerList.get(1);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(hasDimension(context, TEST_TAG_KEY), is(true));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
    }

    @Test
    void registryMetricsLoggersDropEmptyTag() {
        registry.gauge("gauge", Tags.of("empty", ""), 1d);
        final List<MetricsLogger> metricsLoggerList = registry.metricsLoggers();
        assertThat(metricsLoggerList.size(), equalTo(1));
        final MetricsLogger metricsLogger = metricsLoggerList.get(0);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(hasDimension(context, "empty"), is(false));
    }

    @Test
    void registryPublish() {
        final MetricsLogger testMetricsLogger1 = mock(MetricsLogger.class);
        final MetricsLogger testMetricsLogger2 = mock(MetricsLogger.class);
        when(registry.metricsLoggers()).thenReturn(Arrays.asList(testMetricsLogger1, testMetricsLogger2));
        registry.publish();
        verify(testMetricsLogger1, times(1)).flush();
        verify(testMetricsLogger2, times(1)).flush();
    }

    private MetricsContext reflectivelyGetMetricsContext(final MetricsLogger metricsLogger) {
        final Field field;
        try {
            field = metricsLogger.getClass().getDeclaredField("context");
            field.setAccessible(true);
            final MetricsContext context = (MetricsContext) field.get(metricsLogger);
            field.setAccessible(false);
            return context;
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasDimension(final MetricsContext context, final String key) {
        final List<DimensionSet> dimensionSetList = context.getDimensions();
        for (final DimensionSet dimensionSet:dimensionSetList) {
            if (dimensionSet.getDimensionKeys().contains(key)) {
                return true;
            }
        }
        return false;
    }

    private void assertDimensionEqual(final MetricsContext context, final String key, final String value) {
        final List<DimensionSet> dimensionSetList = context.getDimensions();
        assertThat(dimensionSetList.size(), equalTo(1));
        final DimensionSet dimensionSet = dimensionSetList.get(0);
        assertThat(dimensionSet.getDimensionKeys().size(), equalTo(1));
        assertThat(dimensionSet.getDimensionValue(key), equalTo(value));
    }
}