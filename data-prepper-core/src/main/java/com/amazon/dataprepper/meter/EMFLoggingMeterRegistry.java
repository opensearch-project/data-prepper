/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.meter;

import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.cloudwatch2.CloudWatchNamingConvention;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.instrument.util.StringUtils;
import io.micrometer.core.lang.Nullable;
import io.micrometer.core.util.internal.logging.WarnThenDebugLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.cloudwatchlogs.emf.environment.Environment;
import software.amazon.cloudwatchlogs.emf.environment.EnvironmentProvider;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

public class EMFLoggingMeterRegistry extends StepMeterRegistry {
    private static final String NAMESPACE = "DataPrepper";
    private static final Map<String, Unit> UNIT_BY_LOWERCASE_VALUE;

    static {
        Map<String, Unit> unitByLowercaseValue = new HashMap<>();
        for (Unit unit: Unit.values()) {
            if (unit != Unit.UNKNOWN_TO_SDK_VERSION) {
                unitByLowercaseValue.put(unit.toString().toLowerCase(), unit);
            }
        }
        UNIT_BY_LOWERCASE_VALUE = Collections.unmodifiableMap(unitByLowercaseValue);
    }

    private final EMFLoggingRegistryConfig config;
    private final Environment environment;
    private final Logger logger = LoggerFactory.getLogger(CloudWatchMeterRegistry.class);
    private static final WarnThenDebugLogger warnThenDebugLogger = new WarnThenDebugLogger(EMFLoggingMeterRegistry.class);

    public EMFLoggingMeterRegistry() {
        this(new EnvironmentProvider().resolveEnvironment().join());
    }

    public EMFLoggingMeterRegistry(Environment environment) {
        this(EMFLoggingRegistryConfig.DEFAULT, environment, Clock.SYSTEM);
    }

    public EMFLoggingMeterRegistry(EMFLoggingRegistryConfig config, Environment environment, Clock clock) {
        this(config, environment, clock, new NamedThreadFactory("emf-logging-metrics-publisher"));
    }

    EMFLoggingMeterRegistry(EMFLoggingRegistryConfig config, Environment environment, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);
        this.config = config;
        this.environment = environment;

        config().namingConvention(new CloudWatchNamingConvention());
        start(threadFactory);
    }

    @Override
    protected void publish() {
        if (config.enabled()) {
            for (MetricsLogger metricsLogger: metricsLoggers()) {
                try {
                    metricsLogger.flush();
                } catch (Exception e) {
                    logger.error("error sending metric data.", e);
                }
            }
        }
    }

    //VisibleForTesting
    List<MetricsLogger> metricsLoggers() {
        Batch batch = new Batch();
        return getMeters().stream().map(m -> m.match(
                batch::gaugeDataMetricsLogger,
                batch::counterDataMetricsLogger,
                batch::timerDataMetricsLogger,
                batch::summaryDataMetricsLogger,
                batch::longTaskTimerDataMetricsLogger,
                batch::timeGaugeDataMetricsLogger,
                batch::functionCounterDataMetricsLogger,
                batch::functionTimerDataMetricsLogger,
                batch::metricDataMetricsLogger)
        ).collect(toList());
    }

    // VisibleForTesting
    class Batch {
        private final Instant timestamp = Instant.ofEpochMilli(clock.wallTime());

        private MetricsLogger gaugeDataMetricsLogger(Gauge gauge) {
            MetricsLogger metricsLogger = prepareMetricsLogger(gauge.getId());
            addMetricDatum(gauge.getId(), "value", gauge.value(), metricsLogger);
            return metricsLogger;
        }

        private MetricsLogger counterDataMetricsLogger(Counter counter) {
            MetricsLogger metricsLogger = prepareMetricsLogger(counter.getId());
            addMetricDatum(counter.getId(), "count", Unit.COUNT, counter.count(), metricsLogger);
            return metricsLogger;
        }

        // VisibleForTesting
        MetricsLogger timerDataMetricsLogger(Timer timer) {
            MetricsLogger metricsLogger = prepareMetricsLogger(timer.getId());
            addMetricDatum(timer.getId(), "sum", getBaseTimeUnit().name(), timer.totalTime(getBaseTimeUnit()), metricsLogger);
            long count = timer.count();
            addMetricDatum(timer.getId(), "count", Unit.COUNT, count, metricsLogger);
            if (count > 0) {
                addMetricDatum(timer.getId(), "avg", getBaseTimeUnit().name(), timer.mean(getBaseTimeUnit()), metricsLogger);
                addMetricDatum(timer.getId(), "max", getBaseTimeUnit().name(), timer.max(getBaseTimeUnit()), metricsLogger);
            }
            return metricsLogger;
        }

        // VisibleForTesting
        MetricsLogger summaryDataMetricsLogger(DistributionSummary summary) {
            MetricsLogger metricsLogger = prepareMetricsLogger(summary.getId());
            addMetricDatum(summary.getId(), "sum", summary.totalAmount(), metricsLogger);
            long count = summary.count();
            addMetricDatum(summary.getId(), "count", Unit.COUNT, count, metricsLogger);
            if (count > 0) {
                addMetricDatum(summary.getId(), "avg", summary.mean(), metricsLogger);
                addMetricDatum(summary.getId(), "max", summary.max(), metricsLogger);
            }
            return metricsLogger;
        }

        private MetricsLogger longTaskTimerDataMetricsLogger(LongTaskTimer longTaskTimer) {
            MetricsLogger metricsLogger = prepareMetricsLogger(longTaskTimer.getId());
            addMetricDatum(longTaskTimer.getId(), "activeTasks", longTaskTimer.activeTasks(), metricsLogger);
            addMetricDatum(longTaskTimer.getId(), "duration", longTaskTimer.duration(getBaseTimeUnit()), metricsLogger);
            return metricsLogger;
        }

        private MetricsLogger timeGaugeDataMetricsLogger(TimeGauge gauge) {
            MetricsLogger metricsLogger = prepareMetricsLogger(gauge.getId());
            addMetricDatum(gauge.getId(), "value", gauge.value(getBaseTimeUnit()), metricsLogger);
            return metricsLogger;
        }

        // VisibleForTesting
        MetricsLogger functionCounterDataMetricsLogger(FunctionCounter counter) {
            MetricsLogger metricsLogger = prepareMetricsLogger(counter.getId());
            addMetricDatum(counter.getId(), "count", Unit.COUNT, counter.count(), metricsLogger);
            return metricsLogger;
        }

        // VisibleForTesting
        MetricsLogger functionTimerDataMetricsLogger(FunctionTimer timer) {
            MetricsLogger metricsLogger = prepareMetricsLogger(timer.getId());
            // we can't know anything about max and percentiles originating from a function timer
            double sum = timer.totalTime(getBaseTimeUnit());
            if (!Double.isFinite(sum)) {
                return metricsLogger;
            }
            double count = timer.count();
            addMetricDatum(timer.getId(), "count", Unit.COUNT, count, metricsLogger);
            addMetricDatum(timer.getId(), "sum", count, metricsLogger);
            if (count > 0) {
                addMetricDatum(timer.getId(), "avg", timer.mean(getBaseTimeUnit()), metricsLogger);
            }
            return metricsLogger;
        }

        // VisibleForTesting
        MetricsLogger metricDataMetricsLogger(Meter m) {
            MetricsLogger metricsLogger = prepareMetricsLogger(m.getId());
            stream(m.measure().spliterator(), false)
                    .forEach(ms -> addMetricDatum(m.getId().withTag(ms.getStatistic()), ms.getValue(), metricsLogger));
            return metricsLogger;
        }

        private void addMetricDatum(Meter.Id id, double value, MetricsLogger metricsLogger) {
            addMetricDatum(id, null, id.getBaseUnit(), value, metricsLogger);
        }

        private void addMetricDatum(Meter.Id id, @Nullable String suffix, double value, MetricsLogger metricsLogger) {
            addMetricDatum(id, suffix, id.getBaseUnit(), value, metricsLogger);
        }

        private void addMetricDatum(Meter.Id id, @Nullable String suffix, @Nullable String unit, double value, MetricsLogger metricsLogger) {
            addMetricDatum(id, suffix, toUnit(unit), value, metricsLogger);
        }

        private void addMetricDatum(Meter.Id id, @Nullable String suffix, Unit unit, double value, MetricsLogger metricsLogger) {
            if (!Double.isNaN(value)) {
                metricsLogger.putMetric(getMetricName(id, suffix), EMFMetricUtils.clampMetricValue(value), unit);
            }
        }

        private void addDimensionSet(Meter.Id id, MetricsLogger metricsLogger) {
            List<Tag> tags = id.getConventionTags(config().namingConvention());
            metricsLogger.setDimensions(toDimensionSet(tags));
        }

        // VisibleForTesting
        String getMetricName(Meter.Id id, @Nullable String suffix) {
            String name = suffix != null ? id.getName() + "." + suffix : id.getName();
            return config().namingConvention().name(name, id.getType(), id.getBaseUnit());
        }

        // VisibleForTesting
        Unit toUnit(@Nullable String unit) {
            if (unit == null) {
                return Unit.NONE;
            }
            Unit unitObject = UNIT_BY_LOWERCASE_VALUE.get(unit.toLowerCase());
            return unitObject != null ? unitObject : Unit.NONE;
        }

        private List<Dimension> toDimensions(List<Tag> tags) {
            return tags.stream()
                    .filter(this::isAcceptableTag)
                    .map(tag -> Dimension.builder().name(tag.getKey()).value(tag.getValue()).build())
                    .collect(toList());
        }

        private DimensionSet toDimensionSet(List<Tag> tags) {
            DimensionSet dimensionSet = new DimensionSet();
            tags.stream().filter(this::isAcceptableTag).forEach(tag -> dimensionSet.addDimension(tag.getKey(), tag.getValue()));
            return dimensionSet;
        }

        private MetricsLogger prepareMetricsLogger(Meter.Id id) {
            MetricsLogger metricsLogger = new MetricsLogger(environment)
                    .setNamespace(NAMESPACE)
                    .setTimestamp(timestamp);
            addDimensionSet(id, metricsLogger);
            return metricsLogger;
        }

        private boolean isAcceptableTag(Tag tag) {
            if (StringUtils.isBlank(tag.getValue())) {
                warnThenDebugLogger.log("Dropping a tag with key '" + tag.getKey() + "' because its value is blank.");
                return false;
            }
            return true;
        }
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
