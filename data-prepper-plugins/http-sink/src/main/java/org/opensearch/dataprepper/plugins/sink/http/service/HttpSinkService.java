/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.http.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.http.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.http.certificate.HttpClientSSLConnectionManager;
import org.opensearch.dataprepper.plugins.sink.http.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.configuration.UrlConfigurationOption;
import org.opensearch.dataprepper.plugins.sink.http.handler.BasicAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.http.handler.BearerTokenAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.http.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.http.handler.MultiAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.http.util.HttpSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String TOKEN = "token";

    public static final String BEARER = "Bearer ";

    private final Collection<EventHandle> bufferedEventHandles;

    private final HttpSinkConfiguration httpSinkConfiguration;

    private final BufferFactory bufferFactory;

    private final Map<String,HttpAuthOptions> httpAuthOptions;

    private final PluginSetting pluginSetting;

    private final Lock reentrantLock;

    private final HttpClientBuilder httpClientBuilder;

    private CertificateProviderFactory certificateProviderFactory;

    private HttpClientConnectionManager httpClientConnectionManager;

    private Buffer currentBuffer;

    public HttpSinkService(final HttpSinkConfiguration httpSinkConfiguration,
                           final BufferFactory bufferFactory,
                           final PluginSetting pluginSetting,
                           final HttpClientBuilder httpClientBuilder,
                           final PluginMetrics pluginMetrics){
        this.httpSinkConfiguration = httpSinkConfiguration;
        this.bufferFactory = bufferFactory;
        this.pluginSetting = pluginSetting;
        this.reentrantLock = new ReentrantLock();
        this.bufferedEventHandles = new LinkedList<>();
        this.httpClientBuilder = httpClientBuilder;

        if (httpSinkConfiguration.isSsl() || httpSinkConfiguration.useAcmCertForSSL()) {
            this.certificateProviderFactory = new CertificateProviderFactory(httpSinkConfiguration);
            httpSinkConfiguration.validateAndInitializeCertAndKeyFileInS3();
            this.httpClientConnectionManager = new HttpClientSSLConnectionManager()
                    .createHttpClientConnectionManager(httpSinkConfiguration, certificateProviderFactory);
        }
        this.httpAuthOptions = buildAuthHttpSinkObjectsByConfig(httpSinkConfiguration);
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
                try {
                    currentBuffer.writeEvent(event.toJsonString().getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (event.getEventHandle() != null) {
                    this.bufferedEventHandles.add(event.getEventHandle());
                }
                final List<HttpEndPointResponse> failedHttpEndPointResponses = pushToEndPoint(getCurrentBufferData(currentBuffer));
                if (!failedHttpEndPointResponses.isEmpty()) {
                    //TODO send to DLQ and webhook
                } else {
                    LOG.info("data pushed to all the end points successfully");
                }
                currentBuffer = bufferFactory.getBuffer();
                releaseEventHandles(Boolean.TRUE);

            });
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
    private List<HttpEndPointResponse> pushToEndPoint(final byte[] currentBufferData) {
        List<HttpEndPointResponse> httpEndPointResponses = new ArrayList<>(httpSinkConfiguration.getUrlConfigurationOptions().size());
        httpSinkConfiguration.getUrlConfigurationOptions().forEach( urlConfOption -> {
            final ClassicRequestBuilder classicHttpRequestBuilder =
                    httpAuthOptions.get(urlConfOption.getUrl()).getClassicHttpRequestBuilder();
            classicHttpRequestBuilder.setEntity(new String(currentBufferData));
            try {
                httpAuthOptions.get(urlConfOption.getUrl()).getHttpClientBuilder().build()
                        .execute(classicHttpRequestBuilder.build(), HttpClientContext.create());
            } catch (IOException e) {
                LOG.info("No of Records failed to push endpoint {}",currentBuffer.getEventCount());
                LOG.error("Exception while pushing buffer data to end point. URL : {}, Exception : ", urlConfOption.getUrl(), e);
                httpEndPointResponses.add(new HttpEndPointResponse(urlConfOption.getUrl(), HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage()));
            }
        });
        LOG.info("No of Records successfully pushed to endpoint {}",currentBuffer.getEventCount());
        return httpEndPointResponses;
    }

    /**
     * * This method gets Auth Handler classes based on configuration
     *  @param authType AuthTypeOptions.
     *  @param authOptions HttpAuthOptions.Builder.
     */
    private HttpAuthOptions getAuthHandlerByConfig(final AuthTypeOptions authType,
                                                   final HttpAuthOptions.Builder authOptions){
        MultiAuthHttpSinkHandler multiAuthHttpSinkHandler = null;
        // TODO: AWS Sigv4 - check
        switch(authType) {
            case HTTP_BASIC:
                String username = httpSinkConfiguration.getAuthentication().getPluginSettings().get(USERNAME).toString();
                String password = httpSinkConfiguration.getAuthentication().getPluginSettings().get(PASSWORD).toString();
                multiAuthHttpSinkHandler = new BasicAuthHttpSinkHandler(username,password,httpClientConnectionManager);
                break;
            case BEARER_TOKEN:
                String token = httpSinkConfiguration.getAuthentication().getPluginSettings().get(TOKEN).toString();
                multiAuthHttpSinkHandler = new BearerTokenAuthHttpSinkHandler(BEARER + token,httpClientConnectionManager);
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
        final List<UrlConfigurationOption> urlConfigurationOptions = httpSinkConfiguration.getUrlConfigurationOptions();
        final Map<String,HttpAuthOptions> authMap = new HashMap<>(urlConfigurationOptions.size());
        urlConfigurationOptions.forEach( urlOption -> {
            final HTTPMethodOptions httpMethod = Objects.nonNull(urlOption.getHttpMethod()) ? urlOption.getHttpMethod() : httpSinkConfiguration.getHttpMethod();
            final AuthTypeOptions authType = Objects.nonNull(urlOption.getAuthType()) ? urlOption.getAuthType() : httpSinkConfiguration.getAuthType();
            final String proxyUrlString =  Objects.nonNull(urlOption.getProxy()) ? urlOption.getProxy() : httpSinkConfiguration.getProxy();
            final ClassicRequestBuilder classicRequestBuilder = buildRequestByHTTPMethodType(httpMethod).setUri(urlOption.getUrl());

            if(Objects.nonNull(httpSinkConfiguration.getCustomHeaderOptions())){
                //TODO:add custom headers
            }
            if(Objects.nonNull(proxyUrlString)) {
                httpClientBuilder.setProxy(HttpSinkUtil.getHttpHostByURL(HttpSinkUtil.getURLByUrlString(proxyUrlString)));
                LOG.info("Sending data via proxy {}",proxyUrlString);
            }

            final HttpAuthOptions.Builder authOptions = new HttpAuthOptions.Builder()
                    .setUrl(urlOption.getUrl())
                    .setClassicHttpRequestBuilder(classicRequestBuilder)
                    .setHttpClientBuilder(httpClientBuilder);

            authMap.put(urlOption.getUrl(),getAuthHandlerByConfig(authType,authOptions));
        });
        return authMap;
    }

    /**
     * * builds ClassicRequestBuilder based on configured HttpMethod
     *  @param httpMethodOptions Http Method.
     */
    private ClassicRequestBuilder buildRequestByHTTPMethodType(final HTTPMethodOptions httpMethodOptions) {
        final ClassicRequestBuilder classicRequestBuilder;
        switch(httpMethodOptions){
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
}