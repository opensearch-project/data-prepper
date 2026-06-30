/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CloudWatchLogsMetricsIT {

    private static final String PIPELINE_NAME = "pipeline";
    private static final String PLUGIN_NAME = "name";
    private static final String METRICS_PREFIX = PIPELINE_NAME + "." + PLUGIN_NAME + ".";

    private WireMockServer wireMockServer;

    @BeforeAll
    void setUp() {
        MetricsTestUtil.initMetrics();
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
    }

    @AfterAll
    void tearDown() {
        if (wireMockServer != null && wireMockServer.isRunning()) {
            wireMockServer.stop();
        }
        MetricsTestUtil.initMetrics();
    }

    @Test
    void throttled_counter_increments_on_http_429_from_wiremock_IT() {
        MetricsTestUtil.initMetrics();

        wireMockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.post(
                        com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/"))
                .willReturn(aResponse()
                        .withStatus(429)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withHeader("x-amzn-ErrorType", "Throttling")
                        .withBody("{\"__type\":\"Throttling\",\"message\":\"Rate exceeded\"}")));

        final CloudWatchLogsSink sink = buildSinkWithEndpoint(false, false);
        sink.doInitialize();

        final Collection<Record<Event>> records = createRecords(2);
        sink.doOutput(records);

        await().atMost(java.time.Duration.ofSeconds(60))
                .pollInterval(java.time.Duration.ofMillis(500))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    final double throttledCount = getCounterValue(
                            METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_THROTTLED);
                    assertThat("cloudWatchLogsThrottled should have incremented",
                            throttledCount, greaterThan(0.0));
                });

        final double requestsFailedCount = getCounterValue(
                METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED);
        assertThat("cloudWatchLogsRequestsFailed must also increment on throttling",
                requestsFailedCount, greaterThan(0.0));

        final double throttledCount = getCounterValue(
                METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_THROTTLED);
        assertThat("requestsFailed >= throttled",
                requestsFailedCount, greaterThanOrEqualTo(throttledCount));
    }

    @Test
    void resource_not_found_counter_increments_on_terminal_rnf_from_wiremock_IT() {
        MetricsTestUtil.initMetrics();

        wireMockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.post(
                        com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/"))
                .willReturn(aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withHeader("x-amzn-ErrorType", "ResourceNotFoundException")
                        .withBody("{\"__type\":\"ResourceNotFoundException\",\"message\":\"The specified log group does not exist.\"}")));

        final CloudWatchLogsSink sink = buildSinkWithEndpoint(false, false);
        sink.doInitialize();

        final Collection<Record<Event>> records = createRecords(2);
        sink.doOutput(records);

        await().atMost(java.time.Duration.ofSeconds(60))
                .pollInterval(java.time.Duration.ofMillis(500))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    final double rnfCount = getCounterValue(
                            METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND);
                    assertThat("cloudWatchLogsResourceNotFound should have incremented",
                            rnfCount, greaterThan(0.0));
                });

        final double requestsFailedCount = getCounterValue(
                METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED);
        assertThat("cloudWatchLogsRequestsFailed must also increment on terminal RNF",
                requestsFailedCount, greaterThan(0.0));

        final double rnfCount = getCounterValue(
                METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND);
        assertThat("requestsFailed >= resourceNotFound",
                requestsFailedCount, greaterThanOrEqualTo(rnfCount));
    }

    @Test
    void resource_not_found_counter_does_not_increment_when_autocreate_succeeds_IT() {
        MetricsTestUtil.initMetrics();
        wireMockServer.resetAll();

        final String scenarioName = "autocreate-success";
        final String stateAfterRnf = "resources-created";

        wireMockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.post(
                        com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/"))
                .inScenario(scenarioName)
                .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
                .withHeader("X-Amz-Target", com.github.tomakehurst.wiremock.client.WireMock.containing("PutLogEvents"))
                .willReturn(aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withHeader("x-amzn-ErrorType", "ResourceNotFoundException")
                        .withBody("{\"__type\":\"ResourceNotFoundException\",\"message\":\"The specified log group does not exist.\"}"))
                .willSetStateTo(stateAfterRnf));

        wireMockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.post(
                        com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/"))
                .withHeader("X-Amz-Target", com.github.tomakehurst.wiremock.client.WireMock.containing("CreateLogGroup"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withBody("{}")));

        wireMockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.post(
                        com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/"))
                .withHeader("X-Amz-Target", com.github.tomakehurst.wiremock.client.WireMock.containing("CreateLogStream"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withBody("{}")));

        wireMockServer.stubFor(com.github.tomakehurst.wiremock.client.WireMock.post(
                        com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo("/"))
                .inScenario(scenarioName)
                .whenScenarioStateIs(stateAfterRnf)
                .withHeader("X-Amz-Target", com.github.tomakehurst.wiremock.client.WireMock.containing("PutLogEvents"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/x-amz-json-1.1")
                        .withBody("{\"nextSequenceToken\":\"token\",\"rejectedLogEventsInfo\":null}")));

        final CloudWatchLogsSink sink = buildSinkWithEndpoint(true, true);
        sink.doInitialize();

        final Collection<Record<Event>> records = createRecords(2);
        sink.doOutput(records);

        await().atMost(java.time.Duration.ofSeconds(60))
                .pollInterval(java.time.Duration.ofMillis(500))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    final double successCount = getCounterValue(
                            METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED);
                    assertThat("cloudWatchLogsRequestsSucceeded should have incremented after auto-create",
                            successCount, greaterThan(0.0));
                });

        final double rnfCount = getCounterValue(
                METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND);
        assertThat("cloudWatchLogsResourceNotFound must NOT increment when auto-create succeeds",
                rnfCount, org.hamcrest.Matchers.equalTo(0.0));

        final double requestsFailedCount = getCounterValue(
                METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED);
        assertThat("cloudWatchLogsRequestsFailed must NOT increment when auto-create recovers",
                requestsFailedCount, org.hamcrest.Matchers.equalTo(0.0));
    }

    @Test
    void access_denied_counter_increments_with_real_aws_IT() {
        final String logGroup = System.getProperty("tests.cloudwatch.log_group");
        final String logStream = System.getProperty("tests.cloudwatch.log_stream");
        final String region = System.getProperty("tests.aws.region");
        final String role = System.getProperty("tests.aws.role");

        Assumptions.assumeTrue(logGroup != null && !logGroup.isEmpty(),
                "Skipping real-AWS AccessDenied test: tests.cloudwatch.log_group not set");
        Assumptions.assumeTrue(logStream != null && !logStream.isEmpty(),
                "Skipping real-AWS AccessDenied test: tests.cloudwatch.log_stream not set");
        Assumptions.assumeTrue(region != null && !region.isEmpty(),
                "Skipping real-AWS AccessDenied test: tests.aws.region not set");
        Assumptions.assumeTrue(role != null && !role.isEmpty(),
                "Skipping real-AWS AccessDenied test: tests.aws.role not set");

        MetricsTestUtil.initMetrics();

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, Collections.emptyMap());
        pluginSetting.setPipelineName(PIPELINE_NAME);
        final PluginMetrics pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);

        final AwsConfig awsConfig = mock(AwsConfig.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.of(region));
        when(awsConfig.getAwsStsRoleArn()).thenReturn(role);
        when(awsConfig.getAwsStsExternalId()).thenReturn(null);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(null);

        final AwsCredentialsSupplier awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(awsCredentialsSupplier.getProvider(Mockito.any(AwsCredentialsOptions.class)))
                .thenReturn(software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.create());

        final CloudWatchLogsSinkConfig config = mock(CloudWatchLogsSinkConfig.class);
        when(config.getAwsConfig()).thenReturn(awsConfig);
        when(config.getLogGroup()).thenReturn(logGroup);
        when(config.getLogStream()).thenReturn(logStream);
        when(config.getEndpoint()).thenReturn(null);
        when(config.getMaxRetries()).thenReturn(3);
        when(config.getWorkers()).thenReturn(1);
        when(config.getDlq()).thenReturn(null);
        when(config.getHeaderOverrides()).thenReturn(new HashMap<>());
        when(config.getCreateLogGroup()).thenReturn(false);
        when(config.getCreateLogStream()).thenReturn(false);
        when(config.getEntityConfig()).thenReturn(null);

        final ThresholdConfig thresholdConfig = mock(ThresholdConfig.class);
        when(thresholdConfig.getBatchSize()).thenReturn(1);
        when(thresholdConfig.getFlushInterval()).thenReturn(5L);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(10000L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(10000L);
        when(config.getThresholdConfig()).thenReturn(thresholdConfig);

        final PluginFactory pluginFactory = mock(PluginFactory.class);

        final CloudWatchLogsSink sink = new CloudWatchLogsSink(
                pluginSetting, pluginMetrics, pluginFactory, config, awsCredentialsSupplier);
        sink.doInitialize();

        final Collection<Record<Event>> records = createRecords(1);
        sink.doOutput(records);

        await().atMost(java.time.Duration.ofSeconds(60))
                .pollInterval(java.time.Duration.ofMillis(500))
                .untilAsserted(() -> {
                    sink.doOutput(Collections.emptyList());
                    final double accessDeniedCount = getCounterValue(
                            METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ACCESS_DENIED);
                    assertThat("cloudWatchLogsAccessDenied should have incremented",
                            accessDeniedCount, greaterThan(0.0));
                });

        final double requestsFailedCount = getCounterValue(
                METRICS_PREFIX + CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED);
        assertThat("cloudWatchLogsRequestsFailed must also increment on access denied",
                requestsFailedCount, greaterThan(0.0));
    }

    private CloudWatchLogsSink buildSinkWithEndpoint(final boolean createLogGroup, final boolean createLogStream) {
        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, Collections.emptyMap());
        pluginSetting.setPipelineName(PIPELINE_NAME);
        final PluginMetrics pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);

        final AwsConfig awsConfig = mock(AwsConfig.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsConfig.getAwsStsRoleArn()).thenReturn(null);
        when(awsConfig.getAwsStsExternalId()).thenReturn(null);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(null);

        final StaticCredentialsProvider staticCreds = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("test", "test"));

        final AwsCredentialsSupplier awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(awsCredentialsSupplier.getProvider(Mockito.any(AwsCredentialsOptions.class)))
                .thenReturn(staticCreds);
        lenient().when(awsCredentialsSupplier.getDefaultRegion()).thenReturn(Optional.of(Region.US_EAST_1));

        final CloudWatchLogsSinkConfig config = mock(CloudWatchLogsSinkConfig.class);
        when(config.getAwsConfig()).thenReturn(awsConfig);
        when(config.getLogGroup()).thenReturn("test-log-group");
        when(config.getLogStream()).thenReturn("test-log-stream");
        when(config.getEndpoint()).thenReturn("http://localhost:" + wireMockServer.port());
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getWorkers()).thenReturn(1);
        when(config.getDlq()).thenReturn(null);
        when(config.getHeaderOverrides()).thenReturn(new HashMap<>());
        when(config.getCreateLogGroup()).thenReturn(createLogGroup);
        when(config.getCreateLogStream()).thenReturn(createLogStream);
        when(config.getEntityConfig()).thenReturn(null);

        final ThresholdConfig thresholdConfig = mock(ThresholdConfig.class);
        when(thresholdConfig.getBatchSize()).thenReturn(1);
        when(thresholdConfig.getFlushInterval()).thenReturn(5L);
        when(thresholdConfig.getMaxEventSizeBytes()).thenReturn(10000L);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(10000L);
        when(config.getThresholdConfig()).thenReturn(thresholdConfig);

        final PluginFactory pluginFactory = mock(PluginFactory.class);

        return new CloudWatchLogsSink(pluginSetting, pluginMetrics, pluginFactory, config, awsCredentialsSupplier);
    }

    private Collection<Record<Event>> createRecords(final int count) {
        final Collection<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final Map<String, String> data = new HashMap<>();
            data.put("message", "test-event-" + i);
            final EventHandle eventHandle = mock(EventHandle.class);
            final Event event = JacksonLog.builder()
                    .withData(data)
                    .withEventHandle(eventHandle)
                    .build();
            records.add(new Record<>(event));
        }
        return records;
    }

    private double getCounterValue(final String meterName) {
        try {
            return Metrics.globalRegistry.get(meterName).counter().count();
        } catch (final io.micrometer.core.instrument.search.MeterNotFoundException e) {
            return 0.0;
        }
    }
}
