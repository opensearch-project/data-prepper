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

public class ServerConfiguration {
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    static final boolean DEFAULT_ENABLED_UNFRAMED_REQUESTS = false;
    static final boolean DEFAULT_HEALTH_CHECK = false;
    static final boolean DEFAULT_PROTO_REFLECTION_SERVICE = false;
    static final boolean DEFAULT_SSL = true;
    static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
    static final int DEFAULT_THREAD_COUNT = 200;
    static final int DEFAULT_MAX_PENDING_REQUESTS = 1024;
    static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
    static final double BUFFER_TIMEOUT_FRACTION = 0.8;

    @Setter
    @Getter
    private String path;

    @Setter
    private boolean healthCheck = DEFAULT_HEALTH_CHECK;

    @Setter
    private boolean protoReflectionService = DEFAULT_PROTO_REFLECTION_SERVICE;

    @Getter
    @Setter
    private int requestTimeoutInMillis = DEFAULT_REQUEST_TIMEOUT_MS;

    @Setter
    private boolean enableUnframedRequests = DEFAULT_ENABLED_UNFRAMED_REQUESTS;

    @Setter
    @Getter
    private CompressionOption compression = CompressionOption.NONE;

    @Setter
    @Getter
    private PluginModel authentication;

    @Setter
    @Getter
    private boolean ssl = DEFAULT_SSL;

    @Setter
    @Getter
    private boolean unauthenticatedHealthCheck = false;

    @Getter
    @Setter
    private boolean useAcmCertForSSL = DEFAULT_USE_ACM_CERT_FOR_SSL;

    @Getter
    @Setter
    private ByteCount maxRequestLength;

    @Getter
    @Setter
    private Integer port;

    @Getter
    @Setter
    private RetryInfoConfig retryInfo;

    @Getter
    @Setter
    private int threadCount = DEFAULT_THREAD_COUNT;

    @Getter
    @Setter
    private int maxConnectionCount = DEFAULT_MAX_CONNECTION_COUNT;

    @Getter
    @Setter
    private int maxPendingRequests = DEFAULT_MAX_PENDING_REQUESTS;

    @Getter
    @Setter
    private int bufferTimeoutInMillis;

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
