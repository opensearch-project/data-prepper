package org.opensearch.dataprepper.plugins.source.otlp;

import org.opensearch.dataprepper.plugins.server.ServerConfiguration;

public class ConvertConfiguration {

    public static ServerConfiguration convertConfiguration(final OTLPSourceConfig otlpSourceConfig) {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setHealthCheck(otlpSourceConfig.hasHealthCheck());
        serverConfiguration.setProtoReflectionService(otlpSourceConfig.hasProtoReflectionService());
        serverConfiguration.setRequestTimeoutInMillis(otlpSourceConfig.getRequestTimeoutInMillis());
        serverConfiguration.setEnableUnframedRequests(otlpSourceConfig.enableUnframedRequests());
        serverConfiguration.setCompression(otlpSourceConfig.getCompression());
        serverConfiguration.setAuthentication(otlpSourceConfig.getAuthentication());
        serverConfiguration.setSsl(otlpSourceConfig.isSsl());
        serverConfiguration.setUnauthenticatedHealthCheck(otlpSourceConfig.isUnauthenticatedHealthCheck());
        serverConfiguration.setUseAcmCertForSSL(otlpSourceConfig.useAcmCertForSSL());
        serverConfiguration.setMaxRequestLength(otlpSourceConfig.getMaxRequestLength());
        serverConfiguration.setPort(otlpSourceConfig.getPort());
        serverConfiguration.setRetryInfo(otlpSourceConfig.getRetryInfo());
        serverConfiguration.setThreadCount(otlpSourceConfig.getThreadCount());
        serverConfiguration.setMaxConnectionCount(otlpSourceConfig.getMaxConnectionCount());

        return serverConfiguration;
    }
}