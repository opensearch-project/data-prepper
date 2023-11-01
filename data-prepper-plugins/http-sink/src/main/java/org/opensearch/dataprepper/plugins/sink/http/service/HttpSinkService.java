/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;

import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.http.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.http.OAuthAccessTokenManager;
import org.opensearch.dataprepper.plugins.sink.ThresholdValidator;

import org.opensearch.dataprepper.plugins.sink.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.http.certificate.HttpClientSSLConnectionManager;
import org.opensearch.dataprepper.plugins.sink.http.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.http.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.http.handler.BasicAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.http.handler.BearerTokenAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.http.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.http.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.http.handler.MultiAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.http.util.HttpSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This service class contains logic for sending data to Http Endpoints
 */
public class HttpSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkService.class);

    public static final String HTTP_SINK_RECORDS_SUCCESS_COUNTER = "httpSinkRecordsSuccessPushToEndPoint";

    public static final String HTTP_SINK_RECORDS_FAILED_COUNTER = "httpSinkRecordsFailedToPushEndPoint";

    private final Collection<EventHandle> bufferedEventHandles;

    private final HttpSinkConfiguration httpSinkConfiguration;

    private final BufferFactory bufferFactory;

    private final Map<String, HttpAuthOptions> httpAuthOptions;

    private DlqPushHandler dlqPushHandler;

    private final PluginSetting pluginSetting;

    private final Lock reentrantLock;

    private final HttpClientBuilder httpClientBuilder;

    private final int maxEvents;

    private final ByteCount maxBytes;

    private final long maxCollectionDuration;

    private final Counter httpSinkRecordsSuccessCounter;

    private final Counter httpSinkRecordsFailedCounter;

    private final OAuthAccessTokenManager oAuthAccessTokenManager;

    private CertificateProviderFactory certificateProviderFactory;

    private WebhookService webhookService;

    private HttpClientConnectionManager httpClientConnectionManager;

    private Buffer currentBuffer;

    private final PluginSetting httpPluginSetting;

    private MultiAuthHttpSinkHandler multiAuthHttpSinkHandler;

    private final OutputCodec codec;

    private final OutputCodecContext codecContext;

    public HttpSinkService(final HttpSinkConfiguration httpSinkConfiguration,
                           final BufferFactory bufferFactory,
                           final DlqPushHandler dlqPushHandler,
                           final PluginSetting pluginSetting,
                           final WebhookService webhookService,
                           final HttpClientBuilder httpClientBuilder,
                           final PluginMetrics pluginMetrics,
                           final PluginSetting httpPluginSetting,
                           final OutputCodec codec,
                           final OutputCodecContext codecContext) {

        this.httpSinkConfiguration = httpSinkConfiguration;
        this.bufferFactory = bufferFactory;
        this.dlqPushHandler = dlqPushHandler;
        this.pluginSetting = pluginSetting;
        this.reentrantLock = new ReentrantLock();
        this.webhookService = webhookService;
        this.bufferedEventHandles = new LinkedList<>();
        this.httpClientBuilder = httpClientBuilder;
        this.maxEvents = httpSinkConfiguration.getThresholdOptions().getEventCount();
        this.maxBytes = httpSinkConfiguration.getThresholdOptions().getMaximumSize();
        this.maxCollectionDuration = httpSinkConfiguration.getThresholdOptions().getEventCollectTimeOut().getSeconds();
        this.httpPluginSetting = httpPluginSetting;
		this.oAuthAccessTokenManager = new OAuthAccessTokenManager();

        if ((!httpSinkConfiguration.isInsecureSkipVerify()) || (httpSinkConfiguration.useAcmCertForSSL())) {
            this.certificateProviderFactory = new CertificateProviderFactory(httpSinkConfiguration);
            this.httpClientConnectionManager = new HttpClientSSLConnectionManager()
                    .createHttpClientConnectionManager(httpSinkConfiguration, certificateProviderFactory);
        }
        else{
            try {
                this.httpClientConnectionManager = new HttpClientSSLConnectionManager().createHttpClientConnectionManagerWithoutValidation();
            }catch(NoSuchAlgorithmException | KeyStoreException | KeyManagementException ex){
                LOG.error("Exception while insecure_skip_verify is true ",ex);
            }
        }
        this.httpAuthOptions = buildAuthHttpSinkObjectsByConfig(httpSinkConfiguration);
        this.httpSinkRecordsSuccessCounter = pluginMetrics.counter(HTTP_SINK_RECORDS_SUCCESS_COUNTER);
        this.httpSinkRecordsFailedCounter = pluginMetrics.counter(HTTP_SINK_RECORDS_FAILED_COUNTER);
        this.codec= codec;
        this.codecContext = codecContext;
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
            OutputStream outputStream = currentBuffer.getOutputStream();
            records.forEach(record -> {
                try {
                    final Event event = record.getData();
                    if(currentBuffer.getEventCount() == 0) {
                        codec.start(outputStream,event , codecContext);
                    }
                    codec.writeEvent(event, outputStream);
                    int count = currentBuffer.getEventCount() +1;
                    currentBuffer.setEventCount(count);

                    bufferedEventHandles.add(event.getEventHandle());
                    if (ThresholdValidator.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration)) {
                        codec.complete(outputStream);
                        final HttpEndPointResponse failedHttpEndPointResponses = pushToEndPoint(getCurrentBufferData(currentBuffer));
                        if (failedHttpEndPointResponses != null) {
                            logFailedData(failedHttpEndPointResponses);
                            releaseEventHandles(Boolean.FALSE);
                        } else {
                            LOG.info("data pushed to the end point successfully");
                            releaseEventHandles(Boolean.TRUE);
                        }
                        currentBuffer = bufferFactory.getBuffer();
                    }}
                catch (IOException e) {
                    throw new RuntimeException(e);
                }});
        }finally {
            reentrantLock.unlock();
        }
    }

    private byte[] getCurrentBufferData(final Buffer currentBuffer) {
        try {
            return currentBuffer.getSinkBufferData();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * * This method logs Failed Data to DLQ and Webhook
     *  @param endPointResponses HttpEndPointResponses.
     */
    private void logFailedData(final HttpEndPointResponse endPointResponses) {
        FailedDlqData failedDlqData =
                FailedDlqData.builder()
                        .withUrl(endPointResponses.getUrl())
                        .withMessage(endPointResponses.getErrMessage())
                        .withStatus(endPointResponses.getStatusCode()).build();

        LOG.info("Failed to push the data. Failed DLQ Data: {}",failedDlqData);

        logFailureForDlqObjects(failedDlqData);
        if(Objects.nonNull(webhookService)){
            logFailureForWebHook(failedDlqData);
        }
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
    private HttpEndPointResponse pushToEndPoint(final byte[] currentBufferData) {
        HttpEndPointResponse httpEndPointResponses = null;
        final ClassicRequestBuilder classicHttpRequestBuilder =
                httpAuthOptions.get(httpSinkConfiguration.getUrl()).getClassicHttpRequestBuilder();
        classicHttpRequestBuilder.setEntity(currentBufferData, ContentType.APPLICATION_JSON);
        try {
           if(AuthTypeOptions.BEARER_TOKEN.equals(httpSinkConfiguration.getAuthType()))
               accessTokenIfExpired(httpSinkConfiguration.getAuthentication().getBearerTokenOptions().getTokenExpired(),httpSinkConfiguration.getUrl());
            httpAuthOptions.get(httpSinkConfiguration.getUrl()).getHttpClientBuilder().build()
                    .execute(classicHttpRequestBuilder.build(), HttpClientContext.create());
            LOG.info("No of Records successfully pushed to endpoint {}", httpSinkConfiguration.getUrl() +" " + currentBuffer.getEventCount());
            httpSinkRecordsSuccessCounter.increment(currentBuffer.getEventCount());
        } catch (IOException e) {
            httpSinkRecordsFailedCounter.increment(currentBuffer.getEventCount());
            LOG.info("No of Records failed to push endpoint {}",currentBuffer.getEventCount());
            LOG.error("Exception while pushing buffer data to end point. URL : {}, Exception : ", httpSinkConfiguration.getUrl(), e);
            httpEndPointResponses = new HttpEndPointResponse(httpSinkConfiguration.getUrl(), HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
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
     * * This method push Failed objects to Webhook
     *  @param failedDlqData FailedDlqData.
     */
    private void logFailureForWebHook(final FailedDlqData failedDlqData){
        webhookService.pushWebhook(failedDlqData);
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
                multiAuthHttpSinkHandler = new BasicAuthHttpSinkHandler(
                         httpSinkConfiguration.getAuthentication().getHttpBasic().getUsername(),
                         httpSinkConfiguration.getAuthentication().getHttpBasic().getPassword(),
                         httpClientConnectionManager);
                break;
            case BEARER_TOKEN:
                multiAuthHttpSinkHandler = new BearerTokenAuthHttpSinkHandler(
                        httpSinkConfiguration.getAuthentication().getBearerTokenOptions(),
                        httpClientConnectionManager, oAuthAccessTokenManager);
                break;
            case UNAUTHENTICATED:
            default:
                return authOptions.setHttpClientBuilder(httpClientBuilder
                        .setConnectionManager(httpClientConnectionManager)
                        .addResponseInterceptorLast(new FailedHttpResponseInterceptor(authOptions.getUrl()))).build();
        }
        return multiAuthHttpSinkHandler.authenticate(authOptions);
    }

    /**
     * * This method build HttpAuthOptions class based on configurations
     *  @param httpSinkConfiguration HttpSinkConfiguration.
     */
    private Map<String,HttpAuthOptions> buildAuthHttpSinkObjectsByConfig(final HttpSinkConfiguration httpSinkConfiguration){
        final Map<String,HttpAuthOptions> authMap = new HashMap<>();

        final HTTPMethodOptions httpMethod = httpSinkConfiguration.getHttpMethod();
        final AuthTypeOptions authType =  httpSinkConfiguration.getAuthType();
        final String proxyUrlString =  httpSinkConfiguration.getProxy();
        final ClassicRequestBuilder classicRequestBuilder = buildRequestByHTTPMethodType(httpMethod).setUri(httpSinkConfiguration.getUrl());



        if(Objects.nonNull(httpSinkConfiguration.getCustomHeaderOptions()))
            addCustomHeaders(classicRequestBuilder,httpSinkConfiguration.getCustomHeaderOptions());

        if(httpSinkConfiguration.isAwsSigv4() && httpSinkConfiguration.isValidAWSUrl()){
            classicRequestBuilder.addHeader("x-amz-content-sha256","required");
        }

        if(Objects.nonNull(proxyUrlString)) {
            httpClientBuilder.setProxy(HttpSinkUtil.getHttpHostByURL(HttpSinkUtil.getURLByUrlString(proxyUrlString)));
            LOG.info("sending data via proxy {}",proxyUrlString);
        }
        if(httpSinkConfiguration.getRequestTimout() != null) {
            httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofMilliseconds(httpSinkConfiguration.getRequestTimout().toMillis())).build());
        }

        final HttpAuthOptions.Builder authOptions = new HttpAuthOptions.Builder()
                .setUrl(httpSinkConfiguration.getUrl())
                .setClassicHttpRequestBuilder(classicRequestBuilder)
                .setHttpClientBuilder(httpClientBuilder);

        authMap.put(httpSinkConfiguration.getUrl(),getAuthHandlerByConfig(authType,authOptions));
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

    private void accessTokenIfExpired(final Integer tokenExpired,final String url){
        if(oAuthAccessTokenManager.isTokenExpired(tokenExpired)) {
            httpAuthOptions.get(url).getClassicHttpRequestBuilder()
                    .setHeader(BearerTokenAuthHttpSinkHandler.AUTHORIZATION, oAuthAccessTokenManager.getAccessToken(httpSinkConfiguration.getAuthentication().getBearerTokenOptions()));
        }
    }

}
