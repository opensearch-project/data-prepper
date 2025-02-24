package org.opensearch.dataprepper.plugins.source.oteltrace;

import org.opensearch.dataprepper.plugins.server.ServerConfiguration;

public class ConvertConfiguration {

    public static ServerConfiguration convertConfiguration(final OTelTraceSourceConfig oTelTraceSourceConfig) {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setPath(oTelTraceSourceConfig.getPath());
        serverConfiguration.setHealthCheck(oTelTraceSourceConfig.hasHealthCheck());
        serverConfiguration.setProtoReflectionService(oTelTraceSourceConfig.hasProtoReflectionService());
        serverConfiguration.setRequestTimeoutInMillis(oTelTraceSourceConfig.getRequestTimeoutInMillis());
        serverConfiguration.setEnableUnframedRequests(oTelTraceSourceConfig.enableUnframedRequests());
        serverConfiguration.setCompression(oTelTraceSourceConfig.getCompression());
        serverConfiguration.setAuthentication(oTelTraceSourceConfig.getAuthentication());
        serverConfiguration.setSsl(oTelTraceSourceConfig.isSsl());
        serverConfiguration.setUnauthenticatedHealthCheck(oTelTraceSourceConfig.isUnauthenticatedHealthCheck());
        serverConfiguration.setUseAcmCertForSSL(oTelTraceSourceConfig.useAcmCertForSSL());
        serverConfiguration.setMaxRequestLength(oTelTraceSourceConfig.getMaxRequestLength());
        serverConfiguration.setPort(oTelTraceSourceConfig.getPort());
        serverConfiguration.setRetryInfo(oTelTraceSourceConfig.getRetryInfo());
        serverConfiguration.setThreadCount(oTelTraceSourceConfig.getThreadCount());
        serverConfiguration.setMaxConnectionCount(oTelTraceSourceConfig.getMaxConnectionCount());

        return serverConfiguration;
    }
}
