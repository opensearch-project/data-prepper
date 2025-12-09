/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.meter;

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
import io.micrometer.core.util.internal.logging.WarnThenDebugLogger;
import software.amazon.cloudwatchlogs.emf.environment.Environment;
import software.amazon.cloudwatchlogs.emf.environment.EnvironmentProvider;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * {@link StepMeterRegistry} for publishing Data Prepper metrics as
 * <a href="https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html">Embedded Metrics Format</a> logs.
 * @since 1.5
 */
public class EMFLoggingMeterRegistry extends StepMeterRegistry {
    private static final String NAMESPACE = "DataPrepper";
    private static final Map<String, Unit> UNIT_BY_LOWERCASE_VALUE;

    static {
        final Map<String, Unit> unitByLowercaseValue = new HashMap<>();
        for (final Unit unit: Unit.values()) {
            if (unit != Unit.UNKNOWN_TO_SDK_VERSION) {
                unitByLowercaseValue.put(unit.toString().toLowerCase(), unit);
            }
        }
        UNIT_BY_LOWERCASE_VALUE = Collections.unmodifiableMap(unitByLowercaseValue);
    }

    private final EMFLoggingRegistryConfig config;
    private final Environment environment;
    private static final WarnThenDebugLogger warnThenDebugLogger = new WarnThenDebugLogger(EMFLoggingMeterRegistry.class);

    public EMFLoggingMeterRegistry(final EMFLoggingRegistryConfig config) {
        this(config, new EnvironmentProvider().resolveEnvironment().join(), Clock.SYSTEM);
    }

    public EMFLoggingMeterRegistry(final EMFLoggingRegistryConfig config, final Environment environment, final Clock clock) {
        this(config, environment, clock, new NamedThreadFactory("emf-logging-metrics-publisher"));
    }

    EMFLoggingMeterRegistry(final EMFLoggingRegistryConfig config, final Environment environment, final Clock clock, final ThreadFactory threadFactory) {
        super(config, clock);
        this.config = config;
        this.environment = environment;

        config().namingConvention(new CloudWatchNamingConvention());
        start(threadFactory);
    }

    @Override
    protected void publish() {
        if (config.enabled()) {
            metricsLoggers().forEach(MetricsLogger::flush);
        }
    }

    //VisibleForTesting
    List<MetricsLogger> metricsLoggers() {
        final Snapshot snapshot = new Snapshot();
        final Map<List<Tag>, List<Meter>> tagsToMeters = getMeters().stream().collect(
                groupingBy(meter -> meter.getId().getConventionTags(config().namingConvention())));
        return tagsToMeters.entrySet().stream()
                .map(entry -> {
                    final List<Tag> tags = entry.getKey();
                    final MetricsLogger metricsLogger = prepareMetricsLogger(tags, snapshot.timestamp);
                    final List<Meter> meters = entry.getValue();
                    meters.stream().flatMap(m -> m.match(
                            snapshot::gaugeData,
                            snapshot::counterData,
                            snapshot::timerData,
                            snapshot::summaryData,
                            snapshot::longTaskTimerData,
                            snapshot::timeGaugeData,
                            snapshot::functionCounterData,
                            snapshot::functionTimerData,
                            snapshot::metricData
                    )).forEach(metricDataPoint -> 
                            metricsLogger.putMetric(metricDataPoint.key, metricDataPoint.value, metricDataPoint.unit));
                    return metricsLogger;
                }).collect(toList());
    }

    private MetricsLogger prepareMetricsLogger(final List<Tag> tags, final Instant timestamp) {
        final MetricsLogger metricsLogger = new MetricsLogger(environment)
                .setNamespace(NAMESPACE)
                .setTimestamp(timestamp);
        addDimensionSet(tags, metricsLogger);
        addAdditionalProperties(metricsLogger);
        return metricsLogger;
    }

    private void addDimensionSet(final List<Tag> tags, final MetricsLogger metricsLogger) {
        metricsLogger.setDimensions(toDimensionSet(tags));
    }

    private void addAdditionalProperties(final MetricsLogger metricsLogger) {
        config.additionalProperties().forEach(metricsLogger::putProperty);
    }

    private DimensionSet toDimensionSet(final List<Tag> tags) {
        final DimensionSet dimensionSet = new DimensionSet();
        tags.stream().filter(this::isAcceptableTag).forEach(tag -> dimensionSet.addDimension(tag.getKey(), tag.getValue()));
        return dimensionSet;
    }

    private boolean isAcceptableTag(final Tag tag) {
        if (StringUtils.isBlank(tag.getValue())) {
            warnThenDebugLogger.log("Dropping a tag with key '" + tag.getKey() + "' because its value is blank.");
            return false;
        }
        return true;
    }

    // VisibleForTesting
    class Snapshot {
        final Instant timestamp = Instant.ofEpochMilli(clock.wallTime());

        Stream<MetricDataPoint> gaugeData(final Gauge gauge) {
            return Stream.ofNullable(
                    metricDataPoint(gauge.getId(), "value", gauge.value())
            );
        }

        Stream<MetricDataPoint> counterData(final Counter counter) {
            return Stream.ofNullable(
                    metricDataPoint(counter.getId(), "count", Unit.COUNT, counter.count())
            );
        }

        Stream<MetricDataPoint> timerData(final Timer timer) {
            final Stream.Builder<MetricDataPoint> metrics = Stream.builder();
            metrics.add(
                    metricDataPoint(timer.getId(), "sum", getBaseTimeUnit().name(), timer.totalTime(getBaseTimeUnit()))
            );
            final long count = timer.count();
            metrics.add(
                    metricDataPoint(timer.getId(), "count", Unit.COUNT, count)
            );
            if (count > 0) {
                metrics.add(
                        metricDataPoint(timer.getId(), "avg", getBaseTimeUnit().name(), timer.mean(getBaseTimeUnit()))
                );
                metrics.add(
                        metricDataPoint(timer.getId(), "max", getBaseTimeUnit().name(), timer.max(getBaseTimeUnit()))
                );
            }
            return metrics.build();
        }

        Stream<MetricDataPoint> summaryData(final DistributionSummary summary) {
            final Stream.Builder<MetricDataPoint> metrics = Stream.builder();
            metrics.add(metricDataPoint(summary.getId(), "sum", summary.totalAmount()));
            final long count = summary.count();
            metrics.add(metricDataPoint(summary.getId(), "count", Unit.COUNT, count));
            if (count > 0) {
                metrics.add(metricDataPoint(summary.getId(), "avg", summary.mean()));
                metrics.add(metricDataPoint(summary.getId(), "max", summary.max()));
            }
            return metrics.build();
        }

        Stream<MetricDataPoint> longTaskTimerData(final LongTaskTimer longTaskTimer) {
            final Stream.Builder<MetricDataPoint> metrics = Stream.builder();
            metrics.add(metricDataPoint(longTaskTimer.getId(), "activeTasks", longTaskTimer.activeTasks()));
            metrics.add(metricDataPoint(longTaskTimer.getId(), "duration", longTaskTimer.duration(getBaseTimeUnit())));
            return metrics.build();
        }

        Stream<MetricDataPoint> timeGaugeData(final TimeGauge gauge) {
            return Stream.ofNullable(metricDataPoint(gauge.getId(), "value", gauge.value(getBaseTimeUnit())));
        }

        Stream<MetricDataPoint> functionCounterData(final FunctionCounter counter) {
            return Stream.ofNullable(metricDataPoint(counter.getId(), "count", Unit.COUNT, counter.count()));
        }

        Stream<MetricDataPoint> functionTimerData(final FunctionTimer timer) {
            final Stream.Builder<MetricDataPoint> metrics = Stream.builder();
            // we can't know anything about max and percentiles originating from a function timer
            final double sum = timer.totalTime(getBaseTimeUnit());
            if (!Double.isFinite(sum)) {
                return Stream.empty();
            }
            final double count = timer.count();
            metrics.add(metricDataPoint(timer.getId(), "count", Unit.COUNT, count));
            metrics.add(metricDataPoint(timer.getId(), "sum", sum));
            if (count > 0) {
                metrics.add(metricDataPoint(timer.getId(), "avg", timer.mean(getBaseTimeUnit())));
            }
            return metrics.build();
        }

        Stream<MetricDataPoint> metricData(final Meter m) {
            return stream(m.measure().spliterator(), false)
                    .map(ms -> metricDataPoint(m.getId().withTag(ms.getStatistic()), ms.getValue()))
                    .filter(Objects::nonNull);
        }

        private MetricDataPoint metricDataPoint(final Meter.Id id, final double value) {
            return metricDataPoint(id, null, id.getBaseUnit(), value);
        }

        private MetricDataPoint metricDataPoint(final Meter.Id id, @Nullable final String suffix, final double value) {
            return metricDataPoint(id, suffix, id.getBaseUnit(), value);
        }

        private MetricDataPoint metricDataPoint(final Meter.Id id, @Nullable final String suffix, @Nullable final String unit, final double value) {
            return metricDataPoint(id, suffix, toUnit(unit), value);
        }

        private MetricDataPoint metricDataPoint(final Meter.Id id, @Nullable final String suffix, final Unit unit, final double value) {
            if (Double.isNaN(value)) {
                return null;
            }
            return new MetricDataPoint(getMetricName(id, suffix), EMFMetricUtils.clampMetricValue(value), unit);
        }

        String getMetricName(final Meter.Id id, @Nullable final String suffix) {
            final String name = suffix != null ? id.getName() + "." + suffix : id.getName();
            return config().namingConvention().name(name, id.getType(), id.getBaseUnit());
        }

        Unit toUnit(@Nullable final String unit) {
            if (unit == null) {
                return Unit.NONE;
            }
            final Unit unitObject = UNIT_BY_LOWERCASE_VALUE.get(unit.toLowerCase());
            return unitObject != null ? unitObject : Unit.NONE;
        }
    }

    static class MetricDataPoint {
        String key;
        double value;
        Unit unit;

        MetricDataPoint(String key, double value, Unit unit) {
            this.key = key;
            this.value = value;
            this.unit = unit;
        }
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
