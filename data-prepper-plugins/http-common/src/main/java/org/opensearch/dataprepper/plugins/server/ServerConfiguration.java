/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.server;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

public abstract class ServerConfiguration {
    int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    boolean DEFAULT_ENABLED_UNFRAMED_REQUESTS = false;
    boolean DEFAULT_HEALTH_CHECK = false;
    boolean DEFAULT_PROTO_REFLECTION_SERVICE = false;
    boolean DEFAULT_SSL = true;
    boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
    int DEFAULT_THREAD_COUNT = 200;
    int DEFAULT_MAX_PENDING_REQUESTS = 1024;
    int DEFAULT_MAX_CONNECTION_COUNT = 500;
    double BUFFER_TIMEOUT_FRACTION = 0.8;

    @Setter
    @Getter
    String path;

    @Setter
    boolean healthCheck = DEFAULT_HEALTH_CHECK;

    @Setter
    boolean protoReflectionService = DEFAULT_PROTO_REFLECTION_SERVICE;

    @Getter
    @Setter
    int requestTimeoutInMillis = DEFAULT_REQUEST_TIMEOUT_MS;

    @Setter
    boolean enableUnframedRequests = DEFAULT_ENABLED_UNFRAMED_REQUESTS;

    @Setter
    @Getter
    CompressionOption compression = CompressionOption.NONE;

    @Setter
    @Getter
    PluginModel authentication;

    @Setter
    @Getter
    boolean ssl = DEFAULT_SSL;

    @Setter
    @Getter
    boolean unauthenticatedHealthCheck = false;

    @Getter
    @Setter
    boolean useAcmCertForSSL = DEFAULT_USE_ACM_CERT_FOR_SSL;

    @Getter
    @Setter
    ByteCount maxRequestLength;

    @Getter
    @Setter
    Integer port;

    @Getter
    @Setter
    RetryInfoConfig retryInfo;

    @Getter
    @Setter
    int threadCount = DEFAULT_THREAD_COUNT;

    @Getter
    @Setter
    int maxConnectionCount = DEFAULT_MAX_CONNECTION_COUNT;

    @Getter
    @Setter
    int maxPendingRequests = DEFAULT_MAX_PENDING_REQUESTS;

    @Getter
    @Setter
    int bufferTimeoutInMillis;

//    public boolean hasHealthCheck();
//
//    public boolean enableHttpHealthCheck();
//
//    public boolean hasProtoReflectionService();
//
//    public boolean enableUnframedRequests();
//
//    public boolean useAcmCertForSSL();

    public boolean hasHealthCheck() {
        return healthCheck;
    }

    public boolean enableHttpHealthCheck() {
        return enableUnframedRequests() && hasHealthCheck();
    }

    public boolean hasProtoReflectionService() {
        return protoReflectionService;
    }

    public boolean enableUnframedRequests() {
        return enableUnframedRequests;
    }

    public boolean useAcmCertForSSL() {
        return useAcmCertForSSL;
    }
}
