/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus;

import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkThresholdConfig;

import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertUnixNanosToISO8601;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;

import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.BasicAuthCredentials;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthenticationOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith(MockitoExtension.class)
public class PrometheusSinkAMPIT {
    private static final String TEST_PIPELINE_NAME = "testPipeline";
    private static final long NANO_MULTIPLIER = 1_000_000_000L;
    private static final int TEST_READ_BATCH_TIMEOUT = 500;
    private static final int TEST_PROCESSOR_THREADS = 1;
    private static final ObjectMapper OBJECT_MAPPER =  new ObjectMapper();
    private String sumMetricName;
    private String summaryMetricName;
    private String gaugeMetricName;
    private String histogramMetricName;
    private String exponentialHistogramMetricName;
    private Integer metricValue;
    static final int NUM_RECORDS = 10;

    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private AwsConfig awsConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private AwsCredentialsSupplier awsQueryCredentialsSupplier;
    @Mock
    private Counter metricsSuccessCounter;
    @Mock
    private Counter metricsFailedCounter;
    @Mock
    private Counter requestsSuccessCounter;
    @Mock
    private Counter requestsFailedCounter;
    @Mock
    private DistributionSummary summary;
    @Mock
    private EventHandle eventHandle;
    @Mock
    private Pipeline dlqPipeline;
    @Mock
    private PipelineDescription pipelineDescription;

    private String awsRegion;
    private String awsRole;
    private AtomicInteger count;
    private PrometheusSinkConfiguration prometheusSinkConfig;
    private String baseUrl;
    private String queryUrl;
    private String queryRangeUrl;
    private Instant testStartTime;
    private Instant testEndTime;
    private int metricsInAMP;
    private List<Object> result;
    private AwsCredentialsProvider awsCredentialsProvider;
    private WebClient webClient;
    private PrometheusSinkThresholdConfig thresholdConfig;


    @BeforeEach
    void setUp() {
        webClient = WebClient.builder()
                .factory(ClientFactory.builder()
                    .connectTimeout(Duration.ofSeconds(60))
                    .idleTimeout(Duration.ofSeconds(60))
                    .build())

                .build();

        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsQueryCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        eventHandle = mock(EventHandle.class);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsProvider = DefaultCredentialsProvider.create();
        metricsInAMP = 0;
        testStartTime = Instant.now();
        sumMetricName = "sumMetric_"+RandomStringUtils.randomAlphabetic(5);
        summaryMetricName = "summaryMetric_"+RandomStringUtils.randomAlphabetic(5);
        gaugeMetricName = "gaugeMetric_"+RandomStringUtils.randomAlphabetic(5);
        histogramMetricName = "histogramMetric_"+RandomStringUtils.randomAlphabetic(5);
        exponentialHistogramMetricName = "expHistogramMetric_"+RandomStringUtils.randomAlphabetic(5);
        Random random = new Random();
        metricValue = random.nextInt() % 1000;
        count = new AtomicInteger(0);
        pluginFactory = mock(PluginFactory.class);
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        awsConfig = mock(AwsConfig.class);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        when(pluginSetting.getName()).thenReturn("name");

        metricsSuccessCounter = mock(Counter.class);
        metricsFailedCounter = mock(Counter.class);
        requestsSuccessCounter = mock(Counter.class);
        requestsFailedCounter = mock(Counter.class);
        summary = mock(DistributionSummary.class);

        when(pluginMetrics.counter(eq("sinkRequestsSucceeded"))).thenReturn(requestsSuccessCounter);
        when(pluginMetrics.counter(eq("sinkRequestsFailed"))).thenReturn(requestsFailedCounter);
        when(pluginMetrics.counter(eq("sinkMetricsSucceeded"))).thenReturn(metricsSuccessCounter);
        when(pluginMetrics.counter(eq("sinkMetricsFailed"))).thenReturn(metricsFailedCounter);

        when(pluginMetrics.summary(any(String.class))).thenReturn(summary);

        awsRegion = System.getProperty("tests.aws.region");
        when(awsConfig.getAwsRegion()).thenReturn(Region.of(awsRegion));
        awsRole = System.getProperty("tests.aws.role");
        when(awsConfig.getAwsStsRoleArn()).thenReturn(awsRole);
        String url = System.getProperty("tests.prometheus.url");
        baseUrl = url;
        queryRangeUrl = "api/v1/query_range";
        String remoteWriteUrl = url + "api/v1/remote_write";
        queryUrl = url + "api/v1/query";
        when(awsCredentialsSupplier.getProvider(any())).thenAnswer(options -> DefaultCredentialsProvider.create());
        lenient().when(awsQueryCredentialsSupplier.getProvider(any())).thenAnswer(options -> DefaultCredentialsProvider.create());
        thresholdConfig = mock(PrometheusSinkThresholdConfig.class);
        when(thresholdConfig.getMaxEvents()).thenReturn(NUM_RECORDS);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(100000L);
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(60L);
        prometheusSinkConfig = mock(PrometheusSinkConfiguration.class);
        when(prometheusSinkConfig.getMaxRetries()).thenReturn(5);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(0));
        when(prometheusSinkConfig.getSanitizeNames()).thenReturn(false);
        when(prometheusSinkConfig.getUrl()).thenReturn(remoteWriteUrl);
        when(prometheusSinkConfig.getContentType()).thenReturn("application/x-protobuf");
        when(prometheusSinkConfig.getEncoding()).thenReturn(CompressionOption.SNAPPY);
        when(prometheusSinkConfig.getRemoteWriteVersion()).thenReturn("0.1.0");
        when(prometheusSinkConfig.getThresholdConfig()).thenReturn(thresholdConfig);
        AuthenticationOptions authentication = mock(AuthenticationOptions.class);
        BasicAuthCredentials httpBasic = mock(BasicAuthCredentials.class);
        AwsAuthenticationOptions awsAuthOptions = mock(AwsAuthenticationOptions.class);
        when(prometheusSinkConfig.getAwsConfig()).thenReturn(awsConfig);
        when(prometheusSinkConfig.getConnectionTimeout()).thenReturn(Duration.ofMillis(60000L));
        when(prometheusSinkConfig.getIdleTimeout()).thenReturn(Duration.ofMillis(30000L));
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private PrometheusSink createObjectUnderTest() {
        return new PrometheusSink(pluginSetting, pluginMetrics, pipelineDescription, prometheusSinkConfig, awsCredentialsSupplier);
    }

    private void getMetricsFromAMP(final String metricName, final String qs) throws Exception {
        final AuthTypeOptions authType =  AuthTypeOptions.HTTP_BASIC;
        Instant endTime = testEndTime;
        
        String query;
        String queryStr = "";
        // Encode query parameters
        if (qs.equals("histogram")) {
            queryStr = metricName+"_sum|"+metricName+"_count|"+metricName+"_min|"+metricName+"_max";
            query = "{__name__=~\""+queryStr+"\"}";
        } else if (qs.equals("exphistogram")) {
            queryStr = metricName+"_bucket|"+metricName+"_sum|"+metricName+"_count|"+metricName+"_zero_count";
            query = "{__name__=~\""+queryStr+"\"}";
        } else if (qs.equals("summary")) {
            queryStr = metricName+"|"+metricName+"_sum|"+metricName+"_count|attrKey1|attrKey2";
            query = "{__name__=~\""+queryStr+"\"}";
        } else if (qs.equals("sum")) {
            queryStr = metricName+"|"+metricName+"_total";
            query = "{__name__=~\""+queryStr+"\"}";
        } else {
            query = metricName;
        }
        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
        String getUrlQuery = "query=" + query + "&start="+testStartTime+"&end="+endTime+"&step=1s";
        String getUrl = queryRangeUrl+"?query=" + encodedQuery + "&start="+testStartTime+"&end="+endTime+"&step=1s";
        PrometheusSigV4Signer signer = new PrometheusSigV4Signer(awsQueryCredentialsSupplier, prometheusSinkConfig, baseUrl + queryRangeUrl);
        final SdkHttpFullRequest signedRequest = signer.signQueryRequest(getUrlQuery);

        final RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .scheme(signedRequest.getUri().getScheme())
                .path(signedRequest.encodedPath())
                .authority(signedRequest.getUri().getAuthority());
        signedRequest.headers().forEach((k, vList) -> {
            vList.forEach(v -> {
                headersBuilder.add(k, v);
            });
        });

        HttpRequest request = HttpRequest.of(headersBuilder.build(), HttpData.ofAscii(getUrlQuery));
        webClient.execute(request).aggregate()
            .thenAccept(response -> {
                final String responseStr = response.content().toStringUtf8();
                try {
                    Map<String, Object> responseMap = (Map<String, Object>) OBJECT_MAPPER.readValue(responseStr, Map.class);
                    Map<String, Object> data = (Map<String, Object>) responseMap.get("data");
                    result = (List<Object>)data.get("result");
                    metricsInAMP = result.size();
                } catch (Exception e) {}
            })
            .exceptionally(throwable -> {
                return null;
            })
            .join();
        
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    void TestSumMetrics(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));
        PrometheusSink sink = createObjectUnderTest();
        long startTimeSeconds = testStartTime.getEpochSecond();
        Instant time = Instant.now();
        Collection<Record<Event>> records = getSumRecordList(NUM_RECORDS, sumMetricName, 0);
        sink.doOutput(records);
        Thread.sleep(window*1000);

        await().atMost(Duration.ofSeconds(60))
            .untilAsserted(() -> {
                if (window > 0) {
                    sink.doOutput(Collections.emptyList());
                }
                metricsInAMP = 0;
                Set<Double> expectedMetrics = new HashSet<>();
                for (Record record: records) {
                    JacksonSum sum = (JacksonSum)record.getData();
                    expectedMetrics.add(sum.getValue());
                }
                long endTimeSeconds = Instant.now().getEpochSecond();
                getMetricsFromAMP(sumMetricName, "sum");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                Map<String, Object> metricMap = (Map<String, Object> )result.get(0);
                Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                assertThat(metricValuesMatrix.size(), equalTo(NUM_RECORDS));
                for (int i = 0; i < metricValuesMatrix.size(); i++) {
                    List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(i);
                    assertThat(metricValuesList.size(), equalTo(2));

                    Long metricTime = (long)(int)metricValuesList.get(0);
                    Double actualMetricValue = (double)Double.parseDouble((String)metricValuesList.get(1));
                    assertThat(expectedMetrics, hasItem(actualMetricValue));
                    expectedMetrics.remove(actualMetricValue);
                
                    assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                    assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                }
                assertThat(metric.get("resource_attrKey1"), equalTo("1"));
                assertThat(metric.get("resource_attrKey2_attrKey3"), equalTo("attrValue3"));
        });
        verify(requestsSuccessCounter, times(1)).increment(1);
        verify(requestsFailedCounter, times(0)).increment(1);
        verify(metricsSuccessCounter, times(1)).increment(NUM_RECORDS);
        verify(metricsFailedCounter, times(0)).increment(1);
        verify(eventHandle, times(NUM_RECORDS)).release(eq(true));
    }

    @Test
    void TestSumMetricsFailuresWithDLQ() throws Exception {
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        dlqPipeline = mock(Pipeline.class);
        doAnswer(a -> {
            Collection<Record<Event>> records = (Collection<Record<Event>>)a.getArgument(0);
            for (final Record<Event> record : records) {
                record.getData().getEventHandle().release(true);
            }
            return null;
        }).when(dlqPipeline).sendEvents(any(Collection.class));
        when(thresholdConfig.getMaxEvents()).thenReturn(1);
        PrometheusSink sink = createObjectUnderTest();
        sink.setDlqPipeline(dlqPipeline);

        long startTimeSeconds = testStartTime.getEpochSecond();
        Instant time = Instant.now();
        Collection<Record<Event>> records = getSumRecordList(NUM_RECORDS-1, sumMetricName, 1);
        sink.doOutput(records);

        await().atMost(Duration.ofSeconds(60))
            .untilAsserted(() -> {
                metricsInAMP = 0;
                Set<Double> expectedMetrics = new HashSet<>();
                for (Record record: records) {
                    JacksonSum sum = (JacksonSum)record.getData();
                    expectedMetrics.add(sum.getValue());
                }
                long endTimeSeconds = Instant.now().getEpochSecond();
                getMetricsFromAMP(sumMetricName, "sum");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                Map<String, Object> metricMap = (Map<String, Object> )result.get(0);
                Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                assertThat(metricValuesMatrix.size(), greaterThanOrEqualTo(NUM_RECORDS-1));
                for (int i = 0; i < NUM_RECORDS-1; i++) {
                    List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(i);
                    assertThat(metricValuesList.size(), equalTo(2));

                    Long metricTime = (long)(int)metricValuesList.get(0);
                    Double actualMetricValue = (double)Double.parseDouble((String)metricValuesList.get(1));
                    assertThat(expectedMetrics, hasItem(actualMetricValue));
                    expectedMetrics.remove(actualMetricValue);
                
                    assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                    assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                }
        });
        verify(dlqPipeline, times(1)).sendEvents(any(Collection.class));
        verify(requestsSuccessCounter, times(NUM_RECORDS-1)).increment(1);
        verify(requestsFailedCounter, times(1)).increment(1);
        verify(metricsSuccessCounter, times(NUM_RECORDS-1)).increment(1);
        verify(metricsFailedCounter, times(1)).increment(1);
        verify(eventHandle, times(NUM_RECORDS)).release(eq(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    void TestSumMetricsFailuresWithoutDLQ(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));
        when(thresholdConfig.getMaxEvents()).thenReturn(1);
        PrometheusSink sink = createObjectUnderTest();

        long startTimeSeconds = testStartTime.getEpochSecond();
        Instant time = Instant.now();
        Collection<Record<Event>> records = getSumRecordList(NUM_RECORDS-1, sumMetricName, 1);
        sink.doOutput(records);
        Thread.sleep(window*1000);

        await().atMost(Duration.ofSeconds(60))
            .untilAsserted(() -> {
                if (window > 0) {
                    sink.doOutput(Collections.emptyList());
                }
                metricsInAMP = 0;
                Set<Double> expectedMetrics = new HashSet<>();
                for (Record record: records) {
                    JacksonSum sum = (JacksonSum)record.getData();
                    expectedMetrics.add(sum.getValue());
                }
                long endTimeSeconds = Instant.now().getEpochSecond();
                getMetricsFromAMP(sumMetricName, "sum");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                Map<String, Object> metricMap = (Map<String, Object> )result.get(0);
                Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                assertThat(metricValuesMatrix.size(), greaterThanOrEqualTo(NUM_RECORDS-1));
                for (int i = 0; i < NUM_RECORDS-1; i++) {
                    List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(i);
                    assertThat(metricValuesList.size(), equalTo(2));

                    Long metricTime = (long)(int)metricValuesList.get(0);
                    Double actualMetricValue = (double)Double.parseDouble((String)metricValuesList.get(1));
                    assertThat(expectedMetrics, hasItem(actualMetricValue));
                    expectedMetrics.remove(actualMetricValue);
                
                    assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                    assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                }
        });
        verify(requestsSuccessCounter, times(NUM_RECORDS-1)).increment(1);
        verify(requestsFailedCounter, times(1)).increment(1);
        verify(metricsSuccessCounter, times(NUM_RECORDS-1)).increment(1);
        verify(metricsFailedCounter, times(1)).increment(1);
        verify(eventHandle, times(NUM_RECORDS-1)).release(eq(true));
        verify(eventHandle, times(1)).release(eq(false));
    }

    private Collection<Record<Event>> getSumRecordList(int numberOfRecords, final String metricName, int numberOfInvalidRecords) {
        Instant time = Instant.now();
        long timeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        List<Record<Event>> records = new ArrayList<>();
        long endTimeNanos = timeNanos;
        for (int i = 0;i < numberOfRecords+numberOfInvalidRecords; i++) {
            final String mName = (i >= numberOfRecords) ? metricName+"-invalidName" : metricName;
            long startTimeNanos = timeNanos + i*NANO_MULTIPLIER;
            endTimeNanos = startTimeNanos + 2 * NANO_MULTIPLIER;
            JacksonSum sum = JacksonSum.builder()
                .withName(mName)
                .withDescription("Test Sum Metric")
                .withTime(convertUnixNanosToISO8601(endTimeNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withIsMonotonic(true)
                .withUnit("1")
                .withAggregationTemporality("AGGREGATION_TEMPORALITY_CUMULATIVE")
                .withResource(Map.of("attributes", Map.of("attrKey1", 1, "attrKey2", Map.of("attrKey3", "attrValue3"))))
                .withValue((double)metricValue+i)
                .withEventHandle(eventHandle)
                .build(false);
            Event event = (Event)sum;
            records.add(new Record<>(event));
        }
        testEndTime = Instant.ofEpochSecond(endTimeNanos/NANO_MULTIPLIER + 1);
        return records;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    void TestGaugeMetrics(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));

        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getGaugeRecordList(NUM_RECORDS);
        sink.doOutput(records);
        Thread.sleep(window*1000);

        long startTimeSeconds = testStartTime.getEpochSecond();
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    if (window > 0) {
                        sink.doOutput(Collections.emptyList());
                    }
                    metricsInAMP = 0;
                    long endTimeSeconds = Instant.now().getEpochSecond();
                    getMetricsFromAMP(gaugeMetricName, "");
                    assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                    Map<String, Object> metricMap = (Map<String, Object> )result.get(0);
                    Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                    List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(i);
                        assertThat(metricValuesList.size(), equalTo(2));

                        int metricTime = (int)metricValuesList.get(0);
                        int actualMetricValue = (int)Integer.parseInt((String)metricValuesList.get(1));
                        assertThat(actualMetricValue, equalTo(metricValue+i));
                        assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                        assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                    }
                    assertThat(metric.get("scope_attrKey1"), equalTo("1"));
                    assertThat(metric.get("scope_attrKey2_attrKey3"), equalTo("attrValue3"));
        });
        verify(requestsSuccessCounter, times(1)).increment(1);
        verify(requestsFailedCounter, times(0)).increment(1);
        verify(metricsSuccessCounter, times(1)).increment(NUM_RECORDS);
        verify(metricsFailedCounter, times(0)).increment(1);
        verify(eventHandle, times(NUM_RECORDS)).release(eq(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    void TestGaugeMetricsWithMaxRequestSizeLimitAndFlushTimeout(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));

        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(220L);
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(20L);
        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getGaugeRecordList(NUM_RECORDS);
        sink.doOutput(records);
        Thread.sleep(window*1000);

        long startTimeSeconds = testStartTime.getEpochSecond();
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    if (window > 0) {
                        sink.doOutput(Collections.emptyList());
                    }
                    metricsInAMP = 0;
                    sink.doOutput(Collections.emptyList());
                    long endTimeSeconds = Instant.now().getEpochSecond();
                    getMetricsFromAMP(gaugeMetricName, "");
                    assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                    Map<String, Object> metricMap = (Map<String, Object> )result.get(0);
                    Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                    List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                    for (int i = 0; i < NUM_RECORDS; i++) {
                        List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(i);
                        assertThat(metricValuesList.size(), equalTo(2));

                        int metricTime = (int)metricValuesList.get(0);
                        int actualMetricValue = (int)Integer.parseInt((String)metricValuesList.get(1));
                        assertThat(actualMetricValue, equalTo(metricValue+i));
                        assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                        assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                    }
        });
        verify(requestsSuccessCounter, times(5)).increment(1);
        verify(requestsFailedCounter, times(0)).increment(1);
        verify(metricsSuccessCounter, times(5)).increment(2);
        verify(metricsFailedCounter, times(0)).increment(1);
        verify(eventHandle, times(NUM_RECORDS)).release(eq(true));
    }

    private Collection<Record<Event>> getGaugeRecordList(int numberOfRecords) {
        Instant time = Instant.now();
        long timeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        
        long endTimeNanos = timeNanos;
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0;i < numberOfRecords; i++) {
            long startTimeNanos = timeNanos + i*NANO_MULTIPLIER;
            endTimeNanos = startTimeNanos + 2 * NANO_MULTIPLIER;
            JacksonGauge gauge = JacksonGauge.builder()
                .withName(gaugeMetricName)
                .withDescription("Test Gauge Metric")
                .withTimeReceived(time.plusSeconds(i*1))
                .withTime(convertUnixNanosToISO8601(endTimeNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withScope(Map.of("attributes", Map.of("attrKey1", 1, "attrKey2", Map.of("attrKey3", "attrValue3"))))
                .withUnit("1")
                .withValue((double)metricValue+i)
                .withEventHandle(eventHandle)
                .build(false);
            Event event = (Event)gauge;
            records.add(new Record<>(event));
        }
        testEndTime = Instant.ofEpochSecond(endTimeNanos/NANO_MULTIPLIER + 1);
        return records;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    void TestSummaryMetrics(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));

        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getSummaryRecordList(NUM_RECORDS);
        sink.doOutput(records);
        Thread.sleep(window*1000);

        long startTimeSeconds = testStartTime.getEpochSecond();
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    if (window > 0) {
                        sink.doOutput(Collections.emptyList());
                    }
                    long endTimeSeconds = Instant.now().getEpochSecond()+10;
                    metricsInAMP = 0;
                    getMetricsFromAMP(summaryMetricName, "summary");
                    assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                    for (int i = 0; i < metricsInAMP; i++) {
                        Map<String, Object> metricMap = (Map<String, Object> )result.get(i);
                        Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                        List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                        String name = (String)metric.get("__name__");
                        for (int j = 0; j < NUM_RECORDS; j++) {
                            List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(j);
                            assertThat(metricValuesList.size(), equalTo(2));
                            int actualMetricValue = (int)Integer.parseInt((String)metricValuesList.get(1));
                            int metricTime = (int)metricValuesList.get(0);

                            if (name.contains("_sum")) {
                                assertThat(actualMetricValue, equalTo(50+j));
                            } else if (name.contains("_count")) {
                                assertThat(actualMetricValue, equalTo(10+j));
                            } else if (metric.get("quantile") != null) {
                                String q = (String)metric.get("quantile");
                                if (q.equals("0.5")) {
                                    assertThat(actualMetricValue, equalTo(10));
                                } else if (q.equals("0.75")) {
                                    assertThat(actualMetricValue, equalTo(20));
                                } else if (q.equals("0.9")) {
                                    assertThat(actualMetricValue, equalTo(30));
                                } else if (q.equals("0.99")) {
                                    assertThat(actualMetricValue, equalTo(5));
                                }
                            } 
                            assertThat(metric.get("attrKey1"), equalTo("1"));
                            assertThat(metric.get("attrKey2_attrKey3"), equalTo("attrValue3"));

                            assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                            assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                        }
                    }
        });
        verify(requestsSuccessCounter, times(1)).increment(1);
        verify(requestsFailedCounter, times(0)).increment(1);
        verify(metricsSuccessCounter, times(1)).increment(NUM_RECORDS);
        verify(metricsFailedCounter, times(0)).increment(1);
        verify(eventHandle, times(NUM_RECORDS)).release(eq(true));
    }

    @Test
    void TestSummaryMetricsWithExceptionDuringFlush() throws Exception {
        doAnswer(a -> {
            throw new RuntimeException("exception");
        }).when(requestsSuccessCounter).increment(any(Double.class));
        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getSummaryRecordList(NUM_RECORDS);
        sink.doOutput(records);

        verify(eventHandle, times(NUM_RECORDS)).release(eq(false));
    }

    private Collection<Record<Event>> getSummaryRecordList(int numberOfRecords) {
        Instant time = Instant.now();
        long timeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        
        List<Record<Event>> records = new ArrayList<>();
        List<Quantile> quantiles = Arrays.asList(
            new DefaultQuantile(0.5d, 10d),
            new DefaultQuantile(0.75d, 20d),
            new DefaultQuantile(0.9d, 30d),
            new DefaultQuantile(0.99d, 5d)
        );
        long endTimeNanos = timeNanos;
        for (int i = 0;i < numberOfRecords; i++) {
            long startTimeNanos = timeNanos + i*NANO_MULTIPLIER;
            endTimeNanos = startTimeNanos + 2 * NANO_MULTIPLIER;
            JacksonSummary summary = JacksonSummary.builder()
                .withName(summaryMetricName)
                .withDescription("Test Summary Metric")
                .withTimeReceived(time.plusSeconds(i*1))
                .withTime(convertUnixNanosToISO8601(endTimeNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withUnit("1")
                .withSum(50D+i)
                .withCount(10L+i)
                .withQuantilesValueCount(4)
                .withQuantiles(quantiles)
                .withAttributes(Map.of("attrKey1", 1, "attrKey2", Map.of("attrKey3", "attrValue3")))
                .withEventHandle(eventHandle)
                .build(false);

            Event event = (Event)summary;
            records.add(new Record<>(event));
        }
        testEndTime = Instant.ofEpochSecond(endTimeNanos/NANO_MULTIPLIER + 1);
        return records;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    void TestHistogramMetrics(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));

        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getHistogramRecordList(NUM_RECORDS);
        sink.doOutput(records);
        Thread.sleep(window*1000);

        long startTimeSeconds = testStartTime.getEpochSecond();
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    if (window > 0) {
                        sink.doOutput(Collections.emptyList());
                    }
                    metricsInAMP = 0;
                    long endTimeSeconds = Instant.now().getEpochSecond()+10;
                    getMetricsFromAMP(histogramMetricName, "histogram");
                    assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                    for (int i = 0; i < result.size(); i++) {
                        Map<String, Object> metricMap = (Map<String, Object> )result.get(i);
                        Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                        List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                        assertThat(metricValuesMatrix.size(), equalTo(NUM_RECORDS));

                        for (int j = 0; j < NUM_RECORDS; j++) {
                            List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(j);
                            assertThat(metricValuesList.size(), equalTo(2));
                            int metricTime = (int)metricValuesList.get(0);
                            int actualMetricValue = (int)Integer.parseInt((String)metricValuesList.get(1));
                            String name = (String)metric.get("__name__");
                            if (name.contains("_bucket")) {
                                String le = (String)metric.get("le");
                                if (le.equals("+Inf")) {
                                    assertThat(actualMetricValue, equalTo(7));
                                } else if (le.equals("5.0")) {
                                    assertThat(actualMetricValue, equalTo(1));
                                } else if (le.equals("10.0")) {
                                    assertThat(actualMetricValue, equalTo(5));
                                } else if (le.equals("100.0")) {
                                    assertThat(actualMetricValue, equalTo(3));
                                }
                                
                            } else if (name.contains("_sum")) {
                                assertThat(actualMetricValue, equalTo(50+j));
                            } else if (name.contains("_count")) {
                                assertThat(actualMetricValue, equalTo(10+j));
                            } else if (name.contains("_min")) {
                                assertThat(actualMetricValue, equalTo(1+j));
                            } else if (name.contains("_max")) {
                                assertThat(actualMetricValue, equalTo(10+j));
                            }
                            assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                            assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                        }
                    }
        });
        verify(requestsSuccessCounter, times(1)).increment(1);
        verify(requestsFailedCounter, times(0)).increment(1);
        verify(metricsSuccessCounter, times(1)).increment(NUM_RECORDS);
        verify(metricsFailedCounter, times(0)).increment(1);
        verify(eventHandle, times(NUM_RECORDS)).release(eq(true));
    }


    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    void TestExponentialHistogramMetrics(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));

        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getExponentialHistogramRecordList(NUM_RECORDS);
        sink.doOutput(records);
        Thread.sleep(window*1000);

        long startTimeSeconds = testStartTime.getEpochSecond();
        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> {
                    if (window > 0) {
                        sink.doOutput(Collections.emptyList());
                    }
                    metricsInAMP = 0;
                    long endTimeSeconds = Instant.now().getEpochSecond()+10;
                    getMetricsFromAMP(exponentialHistogramMetricName, "exphistogram");
                    assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                    List<String> metricNameSuffixes = Arrays.asList("_bucket", "_sum", "_zero_count", "_count");
                    for (int i = 0; i < result.size(); i++) {
                        Map<String, Object> metricMap = (Map<String, Object> )result.get(i);
                        Map<String, Object> metric = (Map<String, Object>)metricMap.get("metric");
                        List<Object> metricValuesMatrix = (List<Object>)metricMap.get("values");
                        assertThat(metricValuesMatrix.size(), equalTo(NUM_RECORDS));

                        for (int j = 0; j < NUM_RECORDS; j++) {
                            List<Object> metricValuesList = (List<Object>) metricValuesMatrix.get(j);
                            assertThat(metricValuesList.size(), equalTo(2));
                            int metricTime = (int)metricValuesList.get(0);
                            int actualMetricValue = (int)Integer.parseInt((String)metricValuesList.get(1));
                            String name = (String)metric.get("__name__");
                            if (name.contains("_bucket")) {
                                String le = (String)metric.get("le");
                                if (le != null) {
                                    if (le.equals("1.0")) {
                                        assertThat(actualMetricValue, equalTo(1));
                                    } else if (le.equals("2.0")) {
                                        assertThat(actualMetricValue, equalTo(5));
                                    } 
                                }
                                String ge = (String)metric.get("ge");
                                if (ge != null) {
                                    if (ge.equals("1.0")) {
                                        assertThat(actualMetricValue, equalTo(8));
                                    } else if (ge.equals("2.0")) {
                                        assertThat(actualMetricValue, equalTo(6));
                                    } 
                                }
                            } else if (name.contains("_sum")) {
                                assertThat(actualMetricValue, equalTo(50+j));
                            } else if (name.contains("_zero_count")) {
                                assertThat(actualMetricValue, equalTo(3));
                            } else if (name.contains("_count")) {
                                assertThat(actualMetricValue, equalTo(10+j));
                            }
                            assertThat((long)metricTime, greaterThanOrEqualTo(startTimeSeconds));
                            assertThat((long)metricTime, lessThanOrEqualTo(endTimeSeconds));
                        }
                    }
        });
        verify(requestsSuccessCounter, times(1)).increment(1);
        verify(requestsFailedCounter, times(0)).increment(1);
        verify(metricsSuccessCounter, times(1)).increment(NUM_RECORDS);
        verify(metricsFailedCounter, times(0)).increment(1);
        verify(eventHandle, times(NUM_RECORDS)).release(eq(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5})
    public void TestMultipleMetrics(final int window) throws Exception {
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(6L);
        when(prometheusSinkConfig.getOutOfOrderWindow()).thenReturn(Duration.ofSeconds(window));
        when(thresholdConfig.getMaxEvents()).thenReturn(1);
        long startTimeSeconds = testStartTime.getEpochSecond();
        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getExponentialHistogramRecordList(NUM_RECORDS/5);
        records.addAll(getHistogramRecordList(NUM_RECORDS/5));
        records.addAll(getSummaryRecordList(NUM_RECORDS/5));
        records.addAll(getGaugeRecordList(NUM_RECORDS/5));
        records.addAll(getSumRecordList(NUM_RECORDS/5, sumMetricName, 0));
        sink.doOutput(records);
        Thread.sleep(window*1000);

        await().atMost(Duration.ofSeconds(60))
            .untilAsserted(() -> {
                if (window > 0) {
                    sink.doOutput(Collections.emptyList());
                }
                
                int totalMetrics = 0;
                metricsInAMP = 0;
                getMetricsFromAMP(sumMetricName, "sum");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                totalMetrics += metricsInAMP;
                metricsInAMP = 0;
                getMetricsFromAMP(gaugeMetricName, "");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                totalMetrics += metricsInAMP;
                metricsInAMP = 0;
                getMetricsFromAMP(summaryMetricName, "summary");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                totalMetrics += metricsInAMP;

                metricsInAMP = 0;
                getMetricsFromAMP(histogramMetricName, "histogram");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                totalMetrics += metricsInAMP;

                metricsInAMP = 0;
                getMetricsFromAMP(exponentialHistogramMetricName, "exphistogram");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
                totalMetrics += metricsInAMP;

                verify(metricsSuccessCounter, times(10)).increment(1);
        });
    }

    private Collection<Record<Event>> getHistogramRecordList(int numberOfRecords) {
        Instant time = Instant.now();
        long timeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        List<Record<Event>> records = new ArrayList<>();
        final List<Bucket> TEST_BUCKETS = Arrays.asList(
                new DefaultBucket(Double.NEGATIVE_INFINITY, 5.0, 2222L),
                new DefaultBucket(5.0, 10.0, 5555L),
                new DefaultBucket(10.0, 100.0, 3333L),
                new DefaultBucket(100.0, Double.POSITIVE_INFINITY, 7777L)
        );
        final List<Long> TEST_BUCKET_COUNTS_LIST = Arrays.asList(2222L, 5555L, 3333L, 7777L);
        final List<Double> TEST_EXPLICIT_BOUNDS_LIST = Arrays.asList(5D, 10D, 100D);
        long endTimeNanos = timeNanos;
        for (int i = 0;i < numberOfRecords; i++) {
            long startTimeNanos = timeNanos + i*NANO_MULTIPLIER;
            endTimeNanos = startTimeNanos + 2 * NANO_MULTIPLIER;
            JacksonHistogram histogram = JacksonHistogram.builder()
            .withName(histogramMetricName)
            .withDescription("Test Histogram Metric")
            .withTime(convertUnixNanosToISO8601(endTimeNanos))
            .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
            .withUnit("1")
            .withSum(50D+i)
            .withMin(1D+i)
            .withMax(10D+i)
            .withCount(10+i)
            .withBucketCount(TEST_BUCKETS.size())
            .withExplicitBoundsCount(TEST_EXPLICIT_BOUNDS_LIST.size())
            .withAggregationTemporality("cumulative")
            .withBuckets(TEST_BUCKETS)
            .withBucketCountsList(TEST_BUCKET_COUNTS_LIST)
            .withExplicitBoundsList(TEST_EXPLICIT_BOUNDS_LIST)
            .withEventHandle(eventHandle)
            .build(false);
            Event event = (Event)histogram;
            records.add(new Record<>(event));
        }
        testEndTime = Instant.ofEpochSecond(endTimeNanos/NANO_MULTIPLIER + 1);
        return records;
    }

    private Collection<Record<Event>> getExponentialHistogramRecordList(int numberOfRecords) {
        Instant time = Instant.now();
        int scale = 1;
        long timeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        long endTimeNanos = timeNanos;
        List<Record<Event>> records = new ArrayList<>();
        final List<Long> TEST_POSITIVE_COUNTS = Arrays.asList(1L, 3L, 5L);
        final List<Long> TEST_NEGATIVE_COUNTS = Arrays.asList(4L, 8L, 2L, 6L);
        for (int i = 0;i < numberOfRecords; i++) {
            long startTimeNanos = timeNanos + i * NANO_MULTIPLIER;
            endTimeNanos = startTimeNanos + 2 * NANO_MULTIPLIER;
            JacksonExponentialHistogram histogram = JacksonExponentialHistogram.builder()
            .withName(exponentialHistogramMetricName)
            .withDescription("Test Exponential Histogram Metric")
            .withTime(convertUnixNanosToISO8601(endTimeNanos))
            .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
            .withUnit("1")
            .withSum(50D+i)
            .withCount(10+i)
            .withScale(scale)
            .withPositiveOffset(-1)
            .withNegativeOffset(2)
            .withZeroCount(3)
            .withAggregationTemporality("cumulative")
            .withPositive(TEST_POSITIVE_COUNTS)
            .withNegative(TEST_NEGATIVE_COUNTS)
            .withEventHandle(eventHandle)
            .build(false);
            Event event = (Event)histogram;
            records.add(new Record<>(event));
        }
        testEndTime = Instant.ofEpochSecond(endTimeNanos/NANO_MULTIPLIER + 1);
        return records;
    }

    @Test
    void testToVerifyLackOfCredentialsResultInFailure() throws Exception {

        AwsCredentialsProvider provider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(provider);
        lenient().when(thresholdConfig.getFlushInterval()).thenReturn(1L);
        when(thresholdConfig.getMaxEvents()).thenReturn(1);
        PrometheusSink sink = createObjectUnderTest();
        Collection<Record<Event>> records = getHistogramRecordList(NUM_RECORDS);
        sink.doOutput(records);

        long startTimeSeconds = testStartTime.getEpochSecond();
        assertThrows( org.awaitility.core.ConditionTimeoutException.class, () ->  await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                metricsInAMP = 0;
                getMetricsFromAMP(histogramMetricName, "histogram");
                assertThat(metricsInAMP, greaterThanOrEqualTo(1));
        }));

        verify(metricsSuccessCounter, times(0)).increment(NUM_RECORDS);
    }
} 
