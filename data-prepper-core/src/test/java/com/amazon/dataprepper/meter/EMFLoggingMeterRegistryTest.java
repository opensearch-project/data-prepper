/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.meter;

import com.fasterxml.jackson.core.JsonProcessingException;
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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    void snapshotGaugeMetricsLoggerValid() throws JsonProcessingException {
        final Gauge gauge = Gauge.builder("gauge", 1d, Number::doubleValue)
                .tags(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.gaugeDataMetricsLogger(gauge);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, gauge.getId(), "value"), is(true));
    }

    @Test
    void snapshotGaugeMetricsLoggerWhenNaNShouldNotAdd() throws JsonProcessingException {
        final Gauge gauge = Gauge.builder("gauge", Double.NaN, Number::doubleValue)
                .tags(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.gaugeDataMetricsLogger(gauge);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, gauge.getId(), "value"), is(false));
    }

    @Test
    void snapshotCounterMetricsLoggerValid() throws JsonProcessingException {
        final Counter counter = Counter.builder("counter").tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.counterDataMetricsLogger(counter);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, counter.getId(), "count"), is(true));
    }

    @Test
    void snapshotShouldAddTimerAggregateMetricWhenAtLeastOneEventHappened() throws JsonProcessingException {
        final Timer timer = mock(Timer.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(2L);

        final MetricsLogger metricsLogger = registrySnapshot.timerDataMetricsLogger(timer);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meterId, "count"), is(true));
        assertThat(hasMetric(context, meterId, "sum"), is(true));
        assertThat(hasMetric(context, meterId, "avg"), is(true));
        assertThat(hasMetric(context, meterId, "max"), is(true));
    }

    @Test
    void snapshotShouldNotAddTimerAggregateMetricWhenNoEventHappened() throws JsonProcessingException {
        Timer timer = mock(Timer.class);
        Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(0L);

        final MetricsLogger metricsLogger = registrySnapshot.timerDataMetricsLogger(timer);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meterId, "count"), is(true));
        assertThat(hasMetric(context, meterId, "sum"), is(true));
        assertThat(hasMetric(context, meterId, "avg"), is(false));
        assertThat(hasMetric(context, meterId, "max"), is(false));
    }

    @Test
    void snapshotShouldAddSummaryAggregateMetricWhenAtLeastOneEventHappened() throws JsonProcessingException {
        final DistributionSummary summary = mock(DistributionSummary.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, DISTRIBUTION_SUMMARY);
        when(summary.getId()).thenReturn(meterId);
        when(summary.count()).thenReturn(2L);

        final MetricsLogger metricsLogger = registrySnapshot.summaryDataMetricsLogger(summary);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meterId, "count"), is(true));
        assertThat(hasMetric(context, meterId, "sum"), is(true));
        assertThat(hasMetric(context, meterId, "avg"), is(true));
        assertThat(hasMetric(context, meterId, "max"), is(true));
    }

    @Test
    void shouldNotAddDistributionSumAggregateMetricWhenNoEventHappened() throws JsonProcessingException {
        final DistributionSummary summary = mock(DistributionSummary.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, DISTRIBUTION_SUMMARY);
        when(summary.getId()).thenReturn(meterId);
        when(summary.count()).thenReturn(0L);

        final MetricsLogger metricsLogger = registrySnapshot.summaryDataMetricsLogger(summary);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meterId, "count"), is(true));
        assertThat(hasMetric(context, meterId, "sum"), is(true));
        assertThat(hasMetric(context, meterId, "avg"), is(false));
        assertThat(hasMetric(context, meterId, "max"), is(false));
    }

    @Test
    void snapshotLongTaskTimerMetricsLoggerValid() throws JsonProcessingException {
        final LongTaskTimer longTaskTimer = LongTaskTimer.builder("longTaskTimer")
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.longTaskTimerDataMetricsLogger(longTaskTimer);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, longTaskTimer.getId(), "activeTasks"), is(true));
        assertThat(hasMetric(context, longTaskTimer.getId(), "duration"), is(true));
    }

    @Test
    void snapshotTimeGaugeDataMetricsLoggerValid() throws JsonProcessingException {
        final AtomicReference<Double> testValue = new AtomicReference<>(0.0);
        final TimeGauge timeGauge = TimeGauge.builder("timeGaugeData", testValue, TimeUnit.MILLISECONDS, AtomicReference::get)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.timeGaugeDataMetricsLogger(timeGauge);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, timeGauge.getId(), "value"), is(true));
    }

    @Test
    void snapshotTimeGaugeDataMetricsLoggerWhenNaNShouldNotAdd() throws JsonProcessingException {
        final AtomicReference<Double> testValue = new AtomicReference<>(Double.NaN);
        final TimeGauge timeGauge = TimeGauge.builder("timeGaugeData", testValue, TimeUnit.MILLISECONDS, AtomicReference::get)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.timeGaugeDataMetricsLogger(timeGauge);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, timeGauge.getId(), "value"), is(false));
    }

    @Test
    void snapshotFunctionCounterDataMetricsLoggerValid() throws JsonProcessingException {
        final FunctionCounter functionCounter = FunctionCounter.builder("functionCounterData", 1d, Number::doubleValue)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.functionCounterDataMetricsLogger(functionCounter);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, functionCounter.getId(), "count"), is(true));
    }

    @Test
    void snapshotFunctionCounterDataShouldClampInfiniteValues() throws JsonProcessingException {
        FunctionCounter functionCounter = FunctionCounter
                .builder("my.positive.infinity", Double.POSITIVE_INFINITY, Number::doubleValue).register(registry);
        clock.add(EMFLoggingRegistryConfig.DEFAULT.step());
        MetricsLogger metricsLogger = registrySnapshot.functionCounterDataMetricsLogger(functionCounter);
        MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertMetricEqual(context, functionCounter.getId(), "count", 1.174271e+108);

        functionCounter = FunctionCounter
                .builder("my.negative.infinity", Double.NEGATIVE_INFINITY, Number::doubleValue).register(registry);
        clock.add(EMFLoggingRegistryConfig.DEFAULT.step());
        metricsLogger = registrySnapshot.functionCounterDataMetricsLogger(functionCounter);
        context = reflectivelyGetMetricsContext(metricsLogger);
        assertMetricEqual(context, functionCounter.getId(), "count", -1.174271e+108);
    }

    @Test
    void snapshotShouldAddFunctionTimerAggregateMetricWhenAtLeastOneEventHappened() throws JsonProcessingException {
        final FunctionTimer timer = mock(FunctionTimer.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(2.0);

        final MetricsLogger metricsLogger = registrySnapshot.functionTimerDataMetricsLogger(timer);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meterId, "count"), is(true));
        assertThat(hasMetric(context, meterId, "sum"), is(true));
        assertThat(hasMetric(context, meterId, "avg"), is(true));
    }

    @Test
    void snapshotShouldNotAddFunctionTimerAggregateMetricWhenAtLeastOneEventHappened() throws JsonProcessingException {
        final FunctionTimer timer = mock(FunctionTimer.class);
        final Id meterId = new Id(METER_NAME, Tags.of(TEST_TAG_KEY, TEST_TAG_VALUE), null, null, TIMER);
        when(timer.getId()).thenReturn(meterId);
        when(timer.count()).thenReturn(0.0);

        final MetricsLogger metricsLogger = registrySnapshot.functionTimerDataMetricsLogger(timer);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meterId, "count"), is(true));
        assertThat(hasMetric(context, meterId, "sum"), is(true));
        assertThat(hasMetric(context, meterId, "avg"), is(false));
    }

    @Test
    void snapshotShouldNotAddFunctionTimerMetricWhenSumIsNaN() throws JsonProcessingException {
        final FunctionTimer functionTimer = FunctionTimer
                .builder("my.function.timer", Double.NaN, Number::longValue, Number::doubleValue, TimeUnit.MILLISECONDS)
                .register(registry);
        clock.add(EMFLoggingRegistryConfig.DEFAULT.step());
        final MetricsLogger metricsLogger = registrySnapshot.functionTimerDataMetricsLogger(functionTimer);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertThat(hasMetric(context, functionTimer.getId(), "count"), is(false));
        assertThat(hasMetric(context, functionTimer.getId(), "sum"), is(false));
        assertThat(hasMetric(context, functionTimer.getId(), "avg"), is(false));
    }

    @Test
    void snapshotCustomMeterDataMetricsLoggerValid() throws JsonProcessingException {
        final Measurement measurement = new Measurement(() -> 2d, Statistic.VALUE);
        final List<Measurement> measurements = Collections.singletonList(measurement);
        final Meter meter = Meter.builder(METER_NAME, Meter.Type.GAUGE, measurements)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.metricDataMetricsLogger(meter);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meter.getId(), null), is(true));
    }

    @Test
    void snapshotCustomMeterDataMetricsLoggerWhenNaNShouldNotAdd() throws JsonProcessingException {
        final Measurement measurement = new Measurement(() -> Double.NaN, Statistic.VALUE);
        final List<Measurement> measurements = Collections.singletonList(measurement);
        final Meter meter = Meter.builder(METER_NAME, Meter.Type.GAUGE, measurements)
                .tag(TEST_TAG_KEY, TEST_TAG_VALUE).register(registry);
        final MetricsLogger metricsLogger = registrySnapshot.metricDataMetricsLogger(meter);
        final MetricsContext context = reflectivelyGetMetricsContext(metricsLogger);
        assertThat(context.getNamespace(), equalTo(SERVICE_NAME));
        assertDimensionEqual(context, TEST_TAG_KEY, TEST_TAG_VALUE);
        assertThat(hasMetric(context, meter.getId(), null), is(false));
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
        assertThat(metricsLoggerList.size(), equalTo(9));
    }

    @Test
    void registryMetricsLoggerDropEmptyTag() {
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

    private boolean hasMetric(final MetricsContext context, final Id meterId, final String suffix) throws JsonProcessingException {
        final List<String> emfJsonList = context.serialize();
        assertThat(emfJsonList.size(), equalTo(1));
        final Map<String, Object> emfMap = objectMapper.readValue(emfJsonList.get(0), MAP_TYPE_REFERENCE);
        String metricName = meterId.getName();
        if (suffix != null) {
            metricName = metricName.concat(".").concat(suffix);
        }
        return emfMap.get(metricName) instanceof Double;
    }

    private void assertMetricEqual(final MetricsContext context, final Id meterId, final String suffix, final Double value)
            throws JsonProcessingException {
        final List<String> emfJsonList = context.serialize();
        assertThat(emfJsonList.size(), equalTo(1));
        final Map<String, Object> emfMap = objectMapper.readValue(emfJsonList.get(0), MAP_TYPE_REFERENCE);
        String metricName = meterId.getName();
        if (suffix != null) {
            metricName = metricName.concat(".").concat(suffix);
        }
        assertThat(emfMap.get(metricName), equalTo(value));
    }
}