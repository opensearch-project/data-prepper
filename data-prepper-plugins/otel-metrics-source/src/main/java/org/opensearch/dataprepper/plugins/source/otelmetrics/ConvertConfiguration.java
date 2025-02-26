package org.opensearch.dataprepper.plugins.source.otelmetrics;

import org.opensearch.dataprepper.plugins.server.ServerConfiguration;

public class ConvertConfiguration {

    public static ServerConfiguration convertConfiguration(final OTelMetricsSourceConfig oTelMetricsSourceConfig) {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
            serverConfiguration.setPath(oTelMetricsSourceConfig.getPath());
            serverConfiguration.setHealthCheck(oTelMetricsSourceConfig.hasHealthCheck());
            serverConfiguration.setProtoReflectionService(oTelMetricsSourceConfig.hasProtoReflectionService());
            serverConfiguration.setRequestTimeoutInMillis(oTelMetricsSourceConfig.getRequestTimeoutInMillis());
            serverConfiguration.setEnableUnframedRequests(oTelMetricsSourceConfig.enableUnframedRequests());
            serverConfiguration.setCompression(oTelMetricsSourceConfig.getCompression());
            serverConfiguration.setAuthentication(oTelMetricsSourceConfig.getAuthentication());
            serverConfiguration.setSsl(oTelMetricsSourceConfig.isSsl());
            serverConfiguration.setUnauthenticatedHealthCheck(oTelMetricsSourceConfig.isUnauthenticatedHealthCheck());
            serverConfiguration.setUseAcmCertForSSL(oTelMetricsSourceConfig.useAcmCertForSSL());
            serverConfiguration.setMaxRequestLength(oTelMetricsSourceConfig.getMaxRequestLength());
            serverConfiguration.setPort(oTelMetricsSourceConfig.getPort());
            serverConfiguration.setRetryInfo(oTelMetricsSourceConfig.getRetryInfo());
            serverConfiguration.setThreadCount(oTelMetricsSourceConfig.getThreadCount());
            serverConfiguration.setMaxConnectionCount(oTelMetricsSourceConfig.getMaxConnectionCount());

        return serverConfiguration;
    }
}
