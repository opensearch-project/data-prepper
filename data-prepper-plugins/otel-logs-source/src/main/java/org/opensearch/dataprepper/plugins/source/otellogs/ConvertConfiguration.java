package org.opensearch.dataprepper.plugins.source.otellogs;

import org.opensearch.dataprepper.plugins.server.ServerConfiguration;

public class ConvertConfiguration {

    public static ServerConfiguration convertConfiguration(final OTelLogsSourceConfig oTelLogsSourceConfig) {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setPath(oTelLogsSourceConfig.getPath());
        serverConfiguration.setHealthCheck(oTelLogsSourceConfig.hasHealthCheck());
        serverConfiguration.setProtoReflectionService(oTelLogsSourceConfig.hasProtoReflectionService());
        serverConfiguration.setRequestTimeoutInMillis(oTelLogsSourceConfig.getRequestTimeoutInMillis());
        serverConfiguration.setEnableUnframedRequests(oTelLogsSourceConfig.enableUnframedRequests());
        serverConfiguration.setCompression(oTelLogsSourceConfig.getCompression());
        serverConfiguration.setAuthentication(oTelLogsSourceConfig.getAuthentication());
        serverConfiguration.setSsl(oTelLogsSourceConfig.isSsl());
        serverConfiguration.setUseAcmCertForSSL(oTelLogsSourceConfig.useAcmCertForSSL());
        serverConfiguration.setMaxRequestLength(oTelLogsSourceConfig.getMaxRequestLength());
        serverConfiguration.setPort(oTelLogsSourceConfig.getPort());
        serverConfiguration.setRetryInfo(oTelLogsSourceConfig.getRetryInfo());
        serverConfiguration.setThreadCount(oTelLogsSourceConfig.getThreadCount());
        serverConfiguration.setMaxConnectionCount(oTelLogsSourceConfig.getMaxConnectionCount());

        return serverConfiguration;
    }
}