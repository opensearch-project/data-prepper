/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.DataPrepper;
import org.opensearch.dataprepper.core.meter.EMFLoggingMeterRegistry;
import org.opensearch.dataprepper.core.meter.JvmMemoryAggregateMetrics;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.parser.model.MetricRegistryType;
import org.opensearch.dataprepper.core.parser.model.OtlpMetricsConfig;
import org.opensearch.dataprepper.core.pipeline.server.CloudWatchMeterRegistryProvider;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class MetricsConfigTest {
    private static final OtlpMetricsConfig UNREACHABLE_OTLP_METRICS_CONFIG =
            new OtlpMetricsConfig("http://127.0.0.1:1/v1/metrics", null, null, null);
    private static MetricsConfig metricsConfig;

    @BeforeAll
    static void setResources() {
        metricsConfig = new MetricsConfig();
    }

    @AfterAll
    static void clearResources() {
        metricsConfig = null;
        Metrics.globalRegistry.close();
    }

    @Test
    public void testYamlFactory() {
        final YAMLFactory factory = metricsConfig.yamlFactory();

        assertThat(factory, isA(YAMLFactory.class));
    }

    @Test
    public void testObjectMapper() {
        final ObjectMapper mapper = metricsConfig.objectMapper(null);

        assertThat(mapper, isA(ObjectMapper.class));
    }

    @Test
    public void testClassLoaderMetrics() {
        final ClassLoaderMetrics actual = metricsConfig.classLoaderMetrics();

        assertThat(actual, isA(ClassLoaderMetrics.class));
    }

    @Test
    public void testJvmMemoryMetrics() {
        final JvmMemoryMetrics actual = metricsConfig.jvmMemoryMetrics();

        assertThat(actual, isA(JvmMemoryMetrics.class));
    }

    @Test
    public void testJvmGcMetrics() {
        final JvmGcMetrics actual = metricsConfig.jvmGcMetrics();

        assertThat(actual, isA(JvmGcMetrics.class));
    }

    @Test
    public void testProcessorMetrics() {
        final ProcessorMetrics actual = metricsConfig.processorMetrics();

        assertThat(actual, isA(ProcessorMetrics.class));
    }

    @Test
    public void testJvmThreadMetrics() {
        final JvmThreadMetrics actual = metricsConfig.jvmThreadMetrics();

        assertThat(actual, isA(JvmThreadMetrics.class));
    }

    @Test
    public void testGivenConfigWithPrometheusMeterRegistryThenMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.Prometheus));

        final MeterRegistry meterRegistry = metricsConfig.prometheusMeterRegistry(dataPrepperConfiguration);

        assertThat(meterRegistry, isA(PrometheusMeterRegistry.class));
    }

    @Test
    void prometheusMeterRegistry_with_aggregate_jvm_memory_metric_does_not_export_it() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.Prometheus));

        final MeterRegistry meterRegistry = metricsConfig.prometheusMeterRegistry(dataPrepperConfiguration);
        new JvmMemoryAggregateMetrics().bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);

        final Collection<Gauge> gauges = meterRegistry.find("jvm.memory.used").gauges();
        assertThat(gauges, not(empty()));
        gauges.forEach(gauge -> assertThat(gauge.getId().getTag("id"), notNullValue()));
    }

    @Test
    public void testGivenEmptyConfigThenMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.emptyList());

        final MeterRegistry meterRegistry = metricsConfig.prometheusMeterRegistry(dataPrepperConfiguration);

        assertThat(meterRegistry, is(nullValue()));
    }

    @Test
    public void testGivenConfigWithNoCloudWatchMeterRegistryThenNoMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.Prometheus));

        final MeterRegistry meterRegistry = metricsConfig.cloudWatchMeterRegistry(dataPrepperConfiguration, null);

        assertThat(meterRegistry, is(nullValue()));
    }

    @ParameterizedTest
    @MethodSource("provideMetricRegistryTypesAndCreators")
    void testGivenDisabledMetricsThenDisabledMetricsFilterDeniesMatchingMetric(final MetricRegistryType metricRegistryType,
                                                                             final Function<DataPrepperConfiguration, MeterRegistry> creator) {
        final DataPrepperConfiguration config = mock(DataPrepperConfiguration.class);

        when(config.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(metricRegistryType));
        when(config.getMetricTags()).thenReturn(Collections.emptyMap());
        when(config.getMetricTagFilters()).thenReturn(Collections.emptyList());
        when(config.getDisabledMetrics()).thenReturn(List.of("test.metric.blocked"));

        final MeterRegistry registry = creator.apply(config);
        try {
            final Counter blockedCounter = registry.counter("test.metric.blocked");
            final Counter allowedCounter = registry.counter("test.metric.allowed");

            assertThat(registry.find("test.metric.blocked").counter(), is(nullValue()));
            assertThat(blockedCounter.count(), equalTo(0.0));
            allowedCounter.increment();
            assertThat(registry.find("test.metric.allowed").counter(), notNullValue());
        } finally {
            registry.close();
        }
    }

    @Test
    public void testGivenConfigWithCloudWatchMeterRegistryThenNoMeterRegistryCreated() {
        final CloudWatchMeterRegistryProvider provider = mock(CloudWatchMeterRegistryProvider.class);
        final CloudWatchMeterRegistry expected = mock(CloudWatchMeterRegistry.class);
        final MeterRegistry.Config config = mock(MeterRegistry.Config.class);
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        when(provider.getCloudWatchMeterRegistry())
                .thenReturn(expected);
        when(expected.config())
                .thenReturn(config);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.CloudWatch));

        final MeterRegistry meterRegistry = metricsConfig.cloudWatchMeterRegistry(dataPrepperConfiguration, provider);

        assertThat(meterRegistry, is(expected));
    }

    @Test
    public void testGivenConfigWithEMFLoggingMeterRegistryThenMeterRegistryCreated() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);

        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Collections.singletonList(MetricRegistryType.EmbeddedMetricsFormat));

        final MeterRegistry meterRegistry = metricsConfig.emfLoggingMeterRegistry(dataPrepperConfiguration);
        final Counter counter = meterRegistry.counter("counter");
        final List<Tag> commonTags = counter.getId().getConventionTags(meterRegistry.config().namingConvention());

        assertThat(meterRegistry, isA(EMFLoggingMeterRegistry.class));
        assertThat(commonTags.size(), equalTo(1));
        final Tag commonTag = commonTags.get(0);
        assertThat(commonTag.getKey(), equalTo(MetricNames.SERVICE_NAME));
        assertThat(commonTag.getValue(), equalTo(DataPrepper.getServiceNameForMetrics()));
    }

    @Test
    void otlpMeterRegistry_with_OTLP_listed_creates_registry() {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        when(dataPrepperConfiguration.getMetricRegistryTypes())
                .thenReturn(Arrays.asList(MetricRegistryType.EmbeddedMetricsFormat, MetricRegistryType.OTLP));
        when(dataPrepperConfiguration.getOtlpMetricsConfig()).thenReturn(UNREACHABLE_OTLP_METRICS_CONFIG);

        final MeterRegistry meterRegistry = metricsConfig.otlpMeterRegistry(dataPrepperConfiguration);
        try {
            assertThat(meterRegistry, isA(OtlpMeterRegistry.class));
        } finally {
            meterRegistry.close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetricRegistryType.class, names = "OTLP", mode = EnumSource.Mode.EXCLUDE)
    void otlpMeterRegistry_without_OTLP_listed_returns_null(final MetricRegistryType metricRegistryType) {
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        when(dataPrepperConfiguration.getMetricRegistryTypes()).thenReturn(Collections.singletonList(metricRegistryType));

        assertThat(metricsConfig.otlpMeterRegistry(dataPrepperConfiguration), is(nullValue()));
    }

    @Test
    void otlpMeterRegistry_with_metric_tag_filters_applies_tags_to_matching_metric() {
        final DataPrepperConfiguration config = mock(DataPrepperConfiguration.class);
        when(config.getMetricRegistryTypes()).thenReturn(Collections.singletonList(MetricRegistryType.OTLP));
        when(config.getOtlpMetricsConfig()).thenReturn(UNREACHABLE_OTLP_METRICS_CONFIG);
        final MetricTagFilter metricTagFilter = mock(MetricTagFilter.class);
        when(metricTagFilter.getPattern()).thenReturn("filtered.**");
        when(metricTagFilter.getTags()).thenReturn(Map.of("filterKey", "filterValue"));
        when(config.getMetricTagFilters()).thenReturn(List.of(metricTagFilter));

        final MeterRegistry registry = metricsConfig.otlpMeterRegistry(config);
        try {
            final Counter filtered = registry.counter("filtered.counter");
            final Counter unfiltered = registry.counter("other.counter");

            assertThat(filtered.getId().getTag("filterKey"), equalTo("filterValue"));
            assertThat(unfiltered.getId().getTag("filterKey"), is(nullValue()));
        } finally {
            registry.close();
        }
    }

    @ParameterizedTest
    @MethodSource("provideOtlpResourceAttributesAndExpectedServiceName")
    void otlpMeterRegistry_exports_plugin_metrics_with_configured_settings_to_receiver(
            final Map<String, String> configuredResourceAttributes, final String expectedServiceName) throws IOException {
        final List<byte[]> requestBodies = new CopyOnWriteArrayList<>();
        final HttpServer receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/v1/metrics", exchange -> {
            requestBodies.add(exchange.getRequestBody().readAllBytes());
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        receiver.start();

        final String endpoint = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/v1/metrics";
        final DataPrepperConfiguration config = mock(DataPrepperConfiguration.class);
        when(config.getMetricRegistryTypes()).thenReturn(Collections.singletonList(MetricRegistryType.OTLP));
        final OtlpMetricsConfig otlpMetricsConfig = new OtlpMetricsConfig(endpoint, "cumulative", null, configuredResourceAttributes);
        when(config.getOtlpMetricsConfig()).thenReturn(otlpMetricsConfig);
        when(config.getMetricTags()).thenReturn(Collections.emptyMap());
        when(config.getMetricTagFilters()).thenReturn(Collections.emptyList());
        when(config.getDisabledMetrics()).thenReturn(Collections.emptyList());

        final MeterRegistry otlpMeterRegistry = metricsConfig.otlpMeterRegistry(config);
        metricsConfig.systemMeterRegistry(Collections.emptyList(), List.of(otlpMeterRegistry), config);
        final String pipelineName = "otlp-test-pipeline-" + UUID.randomUUID();
        final String meterName = pipelineName + ".test_plugin.recordsIn";
        try {
            PluginMetrics.fromNames("test_plugin", pipelineName).counter("recordsIn").increment(5);

            otlpMeterRegistry.close();

            final List<ResourceMetrics> exported = new ArrayList<>();
            for (final byte[] body : requestBodies) {
                exported.addAll(ExportMetricsServiceRequest.parseFrom(body).getResourceMetricsList());
            }
            final List<KeyValue> resourceAttributes = exported.get(0).getResource().getAttributesList();
            assertThat(resourceAttributes, hasItem(stringAttribute("service.name", expectedServiceName)));
            configuredResourceAttributes.forEach((key, value) -> assertThat(resourceAttributes, hasItem(stringAttribute(key, value))));
            final Metric metric = exported.stream()
                    .flatMap(resourceMetrics -> resourceMetrics.getScopeMetricsList().stream())
                    .flatMap(scopeMetrics -> scopeMetrics.getMetricsList().stream())
                    .filter(candidate -> candidate.getName().equals(meterName))
                    .findFirst()
                    .orElseThrow();
            assertThat(metric.getSum().getAggregationTemporality(), equalTo(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE));
            assertThat(metric.getSum().getDataPoints(0).getAsDouble(), equalTo(5.0));
        } finally {
            otlpMeterRegistry.close();
            Metrics.removeRegistry(otlpMeterRegistry);
            receiver.stop(0);
        }
    }

    @ParameterizedTest
    @MethodSource("provideMetricRegistryTypesAndCreators")
    public void testGivenConfigWithMetricTagsThenMeterRegistryConfigured(final MetricRegistryType metricRegistryType,
                                                                         final Function<DataPrepperConfiguration, MeterRegistry> creator) {
        final String testKey = "testKey";
        final String testValue = "testValue";
        final String testServiceName = "testServiceName";
        final DataPrepperConfiguration dataPrepperConfiguration = mock(DataPrepperConfiguration.class);
        when(dataPrepperConfiguration.getMetricRegistryTypes()).thenReturn(Collections.singletonList(metricRegistryType));
        when(dataPrepperConfiguration.getMetricTags()).thenReturn(Map.of(testKey, testValue));

        MeterRegistry meterRegistry = creator.apply(dataPrepperConfiguration);
        Counter counter = meterRegistry.counter("counter");
        List<Tag> commonTags = counter.getId().getConventionTags(meterRegistry.config().namingConvention());
        meterRegistry.close();

        assertThat(commonTags, equalTo(
                Arrays.asList(
                        Tag.of(MetricNames.SERVICE_NAME, DataPrepper.getServiceNameForMetrics()),
                        Tag.of(testKey, testValue))
        ));

        when(dataPrepperConfiguration.getMetricRegistryTypes()).thenReturn(Collections.singletonList(metricRegistryType));
        when(dataPrepperConfiguration.getMetricTags()).thenReturn(Map.of(MetricNames.SERVICE_NAME, testServiceName));

        meterRegistry = creator.apply(dataPrepperConfiguration);
        counter = meterRegistry.counter("counter");
        commonTags = counter.getId().getConventionTags(meterRegistry.config().namingConvention());
        meterRegistry.close();

        assertThat(commonTags, equalTo(List.of(Tag.of(MetricNames.SERVICE_NAME, testServiceName))));
    }

    @Test
    public void testGivenListOfMeterBindersWhenSystemMeterRegistryThenAllMeterBindersRegistered() {
        final Random r = new Random();
        final int copies = r.nextInt(10) + 5;
        final MeterBinder binder = mock(MeterBinder.class);
        final List<MeterBinder> meterBinders = Collections.nCopies(copies, binder);

        final MeterRegistry realRegistry = new SimpleMeterRegistry();
        final List<MeterRegistry> meterRegistries = Collections.singletonList(realRegistry);

        final DataPrepperConfiguration config = mock(DataPrepperConfiguration.class);
        when(config.getMetricTags()).thenReturn(Collections.emptyMap());
        when(config.getMetricTagFilters()).thenReturn(Collections.emptyList());
        when(config.getDisabledMetrics()).thenReturn(Collections.emptyList());

        final CompositeMeterRegistry meterRegistry = metricsConfig.systemMeterRegistry(
                meterBinders,
                meterRegistries,
                config);

        assertThat(meterRegistry, isA(CompositeMeterRegistry.class));
        verify(binder, times(copies)).bindTo(any(MeterRegistry.class));
    }


    @Test
    public void testGivenEmptyListOfMeterBindersWhenSystemMeterRegistryThenNoMeterBindersRegistered() {
        final List<MeterBinder> meterBinders = Collections.emptyList();

        final MeterRegistry meterRegistryMock = new SimpleMeterRegistry();
        final List<MeterRegistry> meterRegistries = Collections.nCopies(1, meterRegistryMock);

        final DataPrepperConfiguration config = mock(DataPrepperConfiguration.class);
        when(config.getMetricTags()).thenReturn(Collections.emptyMap());
        when(config.getMetricTagFilters()).thenReturn(Collections.emptyList());
        when(config.getDisabledMetrics()).thenReturn(Collections.emptyList());

        final CompositeMeterRegistry meterRegistry = metricsConfig.systemMeterRegistry(
                meterBinders,
                meterRegistries,
                config);

        assertThat(meterRegistry, isA(CompositeMeterRegistry.class));
    }


    private static KeyValue stringAttribute(final String key, final String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value)).build();
    }

    private static Stream<Arguments> provideOtlpResourceAttributesAndExpectedServiceName() {
        return Stream.of(
                Arguments.of(Map.of("service.name", "configured-service"), "configured-service"),
                Arguments.of(Map.of("deployment.environment", "test"), DataPrepper.getServiceNameForMetrics())
        );
    }

    private static Stream<Arguments> provideMetricRegistryTypesAndCreators() {
        return Stream.of(
                Arguments.of(MetricRegistryType.EmbeddedMetricsFormat, (Function<DataPrepperConfiguration, MeterRegistry>) metricsConfig::emfLoggingMeterRegistry),
                Arguments.of(MetricRegistryType.Prometheus, (Function<DataPrepperConfiguration, MeterRegistry>) metricsConfig::prometheusMeterRegistry),
                Arguments.of(MetricRegistryType.OTLP, (Function<DataPrepperConfiguration, MeterRegistry>) configuration -> {
                    when(configuration.getOtlpMetricsConfig()).thenReturn(UNREACHABLE_OTLP_METRICS_CONFIG);
                    return metricsConfig.otlpMeterRegistry(configuration);
                })
        );
    }
}