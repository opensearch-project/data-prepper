/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.record.Record;

import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.prometheus.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.prometheus.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.prometheus.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.prometheus.OAuthAccessTokenManager;
import org.opensearch.dataprepper.plugins.sink.prometheus.certificate.HttpClientSSLConnectionManager;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.BasicAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.BearerTokenAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.handler.MultiAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.util.PrometheusSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.plugins.sink.prometheus.handler.BearerTokenAuthPrometheusSinkHandler.AUTHORIZATION;

/**
 * This service class contains logic for sending data to Http Endpoints
 */
public class PrometheusSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkService.class);

    public static final String HTTP_SINK_RECORDS_SUCCESS_COUNTER = "httpSinkRecordsSuccessPushToEndPoint";

    public static final String HTTP_SINK_RECORDS_FAILED_COUNTER = "httpSinkRecordsFailedToPushEndPoint";

    private final Collection<EventHandle> bufferedEventHandles;

    private final PrometheusSinkConfiguration prometheusSinkConfiguration;

    private final BufferFactory bufferFactory;

    private final Map<String,HttpAuthOptions> httpAuthOptions;

    private DlqPushHandler dlqPushHandler;

    private final Lock reentrantLock;

    private final HttpClientBuilder httpClientBuilder;

    private final Counter httpSinkRecordsSuccessCounter;

    private final Counter httpSinkRecordsFailedCounter;

    private final OAuthAccessTokenManager oAuthAccessTokenManager;

    private CertificateProviderFactory certificateProviderFactory;

    private HttpClientConnectionManager httpClientConnectionManager;

    private Buffer currentBuffer;

    private final PluginSetting httpPluginSetting;

    private MultiAuthPrometheusSinkHandler multiAuthPrometheusSinkHandler;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Pattern PREFIX_PATTERN = Pattern.compile("^[^a-zA-Z_:]");
    private static final Pattern BODY_PATTERN = Pattern.compile("[^a-zA-Z0-9_:]");

    public PrometheusSinkService(final PrometheusSinkConfiguration prometheusSinkConfiguration,
                                 final BufferFactory bufferFactory,
                                 final DlqPushHandler dlqPushHandler,
                                 final HttpClientBuilder httpClientBuilder,
                                 final PluginMetrics pluginMetrics,
                                 final PluginSetting httpPluginSetting){
        this.prometheusSinkConfiguration = prometheusSinkConfiguration;
        this.bufferFactory = bufferFactory;
        this.dlqPushHandler = dlqPushHandler;
        this.reentrantLock = new ReentrantLock();
        this.bufferedEventHandles = new LinkedList<>();
        this.httpClientBuilder = httpClientBuilder;
        this.httpPluginSetting = httpPluginSetting;
        this.oAuthAccessTokenManager = new OAuthAccessTokenManager(httpClientBuilder);
        if (prometheusSinkConfiguration.isSsl() || prometheusSinkConfiguration.useAcmCertForSSL()) {
            this.certificateProviderFactory = new CertificateProviderFactory(prometheusSinkConfiguration);
            prometheusSinkConfiguration.validateAndInitializeCertAndKeyFileInS3();
            this.httpClientConnectionManager = new HttpClientSSLConnectionManager()
                    .createHttpClientConnectionManager(prometheusSinkConfiguration, certificateProviderFactory);
        }
        this.httpAuthOptions = buildAuthHttpSinkObjectsByConfig(prometheusSinkConfiguration);
        this.httpSinkRecordsSuccessCounter = pluginMetrics.counter(HTTP_SINK_RECORDS_SUCCESS_COUNTER);
        this.httpSinkRecordsFailedCounter = pluginMetrics.counter(HTTP_SINK_RECORDS_FAILED_COUNTER);
    }

    /**
     * This method process buffer records and send to Http End points based on configured codec
     * @param records Collection of Event
     */
    public void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        if (currentBuffer == null) {
            this.currentBuffer = bufferFactory.getBuffer();
        }
        try {
            records.forEach(record -> {
                final Event event = record.getData();
                byte[] bytes = null;
                if (event.getMetadata().getEventType().equals("METRIC")) {
                    Remote.WriteRequest message = null;
                    if (event instanceof JacksonGauge) {
                        JacksonGauge jacksonGauge = (JacksonGauge) event;
                        message = buildRemoteWriteRequest(jacksonGauge.getTime(),
                                jacksonGauge.getStartTime(), jacksonGauge.getValue(), jacksonGauge.getAttributes());
                    } else if (event instanceof JacksonSum) {
                        JacksonSum jacksonSum = (JacksonSum) event;
                        message = buildRemoteWriteRequest(jacksonSum.getTime(),
                                jacksonSum.getStartTime(), jacksonSum.getValue(), jacksonSum.getAttributes());
                    } else if (event instanceof JacksonSummary) {
                        JacksonSummary jacksonSummary = (JacksonSummary) event;
                        message = buildRemoteWriteRequest(jacksonSummary.getTime(),
                                jacksonSummary.getStartTime(), jacksonSummary.getSum(), jacksonSummary.getAttributes());
                    } else if (event instanceof JacksonHistogram) {
                        JacksonHistogram jacksonHistogram = (JacksonHistogram) event;
                        message = buildRemoteWriteRequest(jacksonHistogram.getTime(),
                                jacksonHistogram.getStartTime(), jacksonHistogram.getSum(), jacksonHistogram.getAttributes());
                    } else if (event instanceof JacksonExponentialHistogram) {
                        JacksonExponentialHistogram jacksonExpHistogram = (JacksonExponentialHistogram) event;
                        message = buildRemoteWriteRequest(jacksonExpHistogram.getTime(),
                                jacksonExpHistogram.getStartTime(), jacksonExpHistogram.getSum(), jacksonExpHistogram.getAttributes());
                    } else {
                        LOG.error("No valid Event type found");
                    }
                    bytes = message.toByteArray();
                }
                if (event.getEventHandle() != null) {
                    this.bufferedEventHandles.add(event.getEventHandle());
                }
                HttpEndPointResponse failedHttpEndPointResponses = null;
                try {
                    failedHttpEndPointResponses = pushToEndPoint(bytes);
                } catch (IOException e) {
                    LOG.info("Error while pushing to the end point");
                }
                if (failedHttpEndPointResponses != null) {
                    logFailedData(failedHttpEndPointResponses);
                    releaseEventHandles(Boolean.FALSE);
                } else {
                    LOG.info("data pushed to the end point successfully");
                    releaseEventHandles(Boolean.TRUE);
                }

            });

        }finally {
            reentrantLock.unlock();
        }
    }

    /**
     * * This method build Remote.WriteRequest
     *  @param time time
     *  @param startTime start time
     *  @param value value
     *  @param attributeMap attributes
     */
    private static Remote.WriteRequest buildRemoteWriteRequest(String time, String startTime,
                                                               Double value, Map<String, Object> attributeMap) {
        Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();

        Types.TimeSeries.Builder timeSeriesBuilder = Types.TimeSeries.newBuilder();

        List<Types.Label> arrayList = new ArrayList<>();

        prepareLabelList(attributeMap, arrayList);

        Types.Sample.Builder prometheusSampleBuilder = Types.Sample.newBuilder();
        long timeStampVal;
        if (time != null) {
            timeStampVal = getTimeStampVal(time);
        } else {
            timeStampVal = getTimeStampVal(startTime);
        }

        prometheusSampleBuilder.setValue(value).setTimestamp(timeStampVal);
        Types.Sample prometheusSample = prometheusSampleBuilder.build();

        timeSeriesBuilder.addAllLabels(arrayList);
        timeSeriesBuilder.addAllSamples(Arrays.asList(prometheusSample));

        Types.TimeSeries timeSeries = timeSeriesBuilder.build();
        writeRequestBuilder.addAllTimeseries(Arrays.asList(timeSeries));

        return writeRequestBuilder.build();
    }

    private static void prepareLabelList(Map<String, Object> hashMap, List<Types.Label> arrayList) {
        for (Map.Entry<String, Object> entry : hashMap.entrySet()) {
            String key = sanitizeName(entry.getKey());
            Object value = entry.getValue();
            if (entry.getValue() instanceof Map) {
                Object innerMap = entry.getValue();
                prepareLabelList(objectMapper.convertValue(innerMap, Map.class), arrayList);
                continue;
            }
            Types.Label.Builder labelBuilder = Types.Label.newBuilder();
            labelBuilder.setName(key).setValue(value.toString());
            Types.Label label = labelBuilder.build();
            arrayList.add(label);
        }
    }

    private static String sanitizeName(String name) {
        return BODY_PATTERN
                .matcher(PREFIX_PATTERN.matcher(name).replaceFirst("_"))
                .replaceAll("_");
    }

    private static long getTimeStampVal(String time) {
        LocalDateTime localDateTimeParse = LocalDateTime.parse(time,
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"));
        LocalDateTime localDateTime = LocalDateTime.parse(localDateTimeParse.toString());
        ZonedDateTime zdt = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        return zdt.toInstant().toEpochMilli();
    }

    /**
     * * This method logs Failed Data to DLQ and Webhook
     *  @param endPointResponses HttpEndPointResponses.
     */
    private void logFailedData(final HttpEndPointResponse endPointResponses) {
        FailedDlqData failedDlqData =
                FailedDlqData.builder()
                        .withUrl(endPointResponses.getUrl())
                        .withMessage(endPointResponses.getErrorMessage())
                        .withStatus(endPointResponses.getStatusCode()).build();
        LOG.info("Failed to push the data. Failed DLQ Data: {}",failedDlqData);

        logFailureForDlqObjects(failedDlqData);
    }

    private void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }
        bufferedEventHandles.clear();
    }

    /**
     * * This method pushes bufferData to configured HttpEndPoints
     *  @param currentBufferData bufferData.
     */
    private HttpEndPointResponse pushToEndPoint(final byte[] currentBufferData) throws IOException {
        HttpEndPointResponse httpEndPointResponses = null;
        final ClassicRequestBuilder classicHttpRequestBuilder =
                httpAuthOptions.get(prometheusSinkConfiguration.getUrl()).getClassicHttpRequestBuilder();

        final byte[] compressedBufferData = Snappy.compress(currentBufferData);

        HttpEntity entity = new ByteArrayEntity(compressedBufferData,
                ContentType.create(prometheusSinkConfiguration.getContentType()), prometheusSinkConfiguration.getEncoding());

        classicHttpRequestBuilder.setEntity(entity);
        classicHttpRequestBuilder.addHeader("Content-Encoding", prometheusSinkConfiguration.getEncoding());
        classicHttpRequestBuilder.addHeader("Content-Type", prometheusSinkConfiguration.getContentType());
        classicHttpRequestBuilder.addHeader("X-Prometheus-Remote-Write-Version", prometheusSinkConfiguration.getRemoteWriteVersion());

        try {
            if(AuthTypeOptions.BEARER_TOKEN.equals(prometheusSinkConfiguration.getAuthType()))
                accessTokenIfExpired(classicHttpRequestBuilder.getFirstHeader(AUTHORIZATION).getValue(),prometheusSinkConfiguration.getUrl());

            httpAuthOptions.get(prometheusSinkConfiguration.getUrl()).getHttpClientBuilder().build()
                    .execute(classicHttpRequestBuilder.build(), HttpClientContext.create());
            LOG.info("No of Records successfully pushed to endpoint {}", prometheusSinkConfiguration.getUrl() +" " + currentBuffer.getEventCount());
            httpSinkRecordsSuccessCounter.increment(currentBuffer.getEventCount());
        } catch (IOException e) {
            httpSinkRecordsFailedCounter.increment(currentBuffer.getEventCount());
            LOG.info("No of Records failed to push endpoint {}",currentBuffer.getEventCount());
            LOG.error("Exception while pushing buffer data to end point. URL : {}, Exception : ", prometheusSinkConfiguration.getUrl(), e);
            httpEndPointResponses = new HttpEndPointResponse(prometheusSinkConfiguration.getUrl(), HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return httpEndPointResponses;
    }

    /**
     * * This method sends Failed objects to DLQ
     *  @param failedDlqData FailedDlqData.
     */
    private void logFailureForDlqObjects(final FailedDlqData failedDlqData){
        dlqPushHandler.perform(httpPluginSetting, failedDlqData);
    }


    /**
     * * This method gets Auth Handler classes based on configuration
     *  @param authType AuthTypeOptions.
     *  @param authOptions HttpAuthOptions.Builder.
     */
    private HttpAuthOptions getAuthHandlerByConfig(final AuthTypeOptions authType,
                                                   final HttpAuthOptions.Builder authOptions){
        switch(authType) {
            case HTTP_BASIC:
                multiAuthPrometheusSinkHandler = new BasicAuthPrometheusSinkHandler(
                        prometheusSinkConfiguration.getAuthentication().getHttpBasic().getUsername(),
                        prometheusSinkConfiguration.getAuthentication().getHttpBasic().getPassword(),
                        httpClientConnectionManager);
                break;
            case BEARER_TOKEN:
                multiAuthPrometheusSinkHandler = new BearerTokenAuthPrometheusSinkHandler(
                        prometheusSinkConfiguration.getAuthentication().getBearerTokenOptions(),
                        httpClientConnectionManager, oAuthAccessTokenManager);
                break;
            case UNAUTHENTICATED:
            default:
                return authOptions.setHttpClientBuilder(httpClientBuilder
                        .setConnectionManager(httpClientConnectionManager)
                        .addResponseInterceptorLast(new FailedHttpResponseInterceptor(authOptions.getUrl()))).build();
        }
        return multiAuthPrometheusSinkHandler.authenticate(authOptions);
    }

    /**
     * * This method build HttpAuthOptions class based on configurations
     *  @param prometheusSinkConfiguration PrometheusSinkConfiguration.
     */
    private Map<String,HttpAuthOptions> buildAuthHttpSinkObjectsByConfig(final PrometheusSinkConfiguration prometheusSinkConfiguration){
        final Map<String,HttpAuthOptions> authMap = new HashMap<>();

        final HTTPMethodOptions httpMethod = prometheusSinkConfiguration.getHttpMethod();
        final AuthTypeOptions authType =  prometheusSinkConfiguration.getAuthType();
        final String proxyUrlString =  prometheusSinkConfiguration.getProxy();
        final ClassicRequestBuilder classicRequestBuilder = buildRequestByHTTPMethodType(httpMethod).setUri(prometheusSinkConfiguration.getUrl());



        if(Objects.nonNull(prometheusSinkConfiguration.getCustomHeaderOptions()))
            addCustomHeaders(classicRequestBuilder,prometheusSinkConfiguration.getCustomHeaderOptions());

        if(Objects.nonNull(proxyUrlString)) {
            httpClientBuilder.setProxy(PrometheusSinkUtil.getHttpHostByURL(PrometheusSinkUtil.getURLByUrlString(proxyUrlString)));
            LOG.info("sending data via proxy {}",proxyUrlString);
        }

        final HttpAuthOptions.Builder authOptions = new HttpAuthOptions.Builder()
                .setUrl(prometheusSinkConfiguration.getUrl())
                .setClassicHttpRequestBuilder(classicRequestBuilder)
                .setHttpClientBuilder(httpClientBuilder);

        authMap.put(prometheusSinkConfiguration.getUrl(),getAuthHandlerByConfig(authType,authOptions));
        return authMap;
    }

    /**
     * * This method adds SageMakerHeaders as custom Header in the request
     *  @param classicRequestBuilder ClassicRequestBuilder.
     *  @param customHeaderOptions CustomHeaderOptions .
     */
    private void addCustomHeaders(final ClassicRequestBuilder classicRequestBuilder,
                                  final Map<String, List<String>> customHeaderOptions) {

        customHeaderOptions.forEach((k, v) -> classicRequestBuilder.addHeader(k,v.toString()));
    }

    /**
     * * builds ClassicRequestBuilder based on configured HttpMethod
     *  @param httpMethodOptions Http Method.
     */
    private ClassicRequestBuilder buildRequestByHTTPMethodType(final HTTPMethodOptions httpMethodOptions) {
        final ClassicRequestBuilder classicRequestBuilder;
        switch (httpMethodOptions) {
            case PUT:
                classicRequestBuilder = ClassicRequestBuilder.put();
                break;
            case POST:
            default:
                classicRequestBuilder = ClassicRequestBuilder.post();
                break;
        }
        return classicRequestBuilder;
    }

    private void accessTokenIfExpired(final String token,final String url){
        if(oAuthAccessTokenManager.isTokenExpired(token)) {
            httpAuthOptions.get(url).getClassicHttpRequestBuilder()
                    .setHeader(AUTHORIZATION, oAuthAccessTokenManager.getAccessToken(prometheusSinkConfiguration.getAuthentication().getBearerTokenOptions()));
        }
    }
}