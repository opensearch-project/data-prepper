package org.opensearch.dataprepper.plugins.source.loghttp;

import org.opensearch.dataprepper.http.HttpServerConfig;
import org.opensearch.dataprepper.plugins.server.ServerConfiguration;

public class ConvertConfiguration {

    public static ServerConfiguration convertConfiguration(final HttpServerConfig sourceConfig) {
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setPath(sourceConfig.getPath());
        serverConfiguration.setHealthCheck(sourceConfig.hasHealthCheckService());
        serverConfiguration.setRequestTimeoutInMillis(sourceConfig.getRequestTimeoutInMillis());
        serverConfiguration.setCompression(sourceConfig.getCompression());
        serverConfiguration.setAuthentication(sourceConfig.getAuthentication());
        serverConfiguration.setSsl(sourceConfig.isSsl());
        serverConfiguration.setUnauthenticatedHealthCheck(sourceConfig.isUnauthenticatedHealthCheck());
        serverConfiguration.setMaxRequestLength(sourceConfig.getMaxRequestLength());
        serverConfiguration.setPort(sourceConfig.getPort());
        serverConfiguration.setThreadCount(sourceConfig.getThreadCount());
        serverConfiguration.setMaxPendingRequests(sourceConfig.getMaxPendingRequests());
        serverConfiguration.setMaxConnectionCount(sourceConfig.getMaxConnectionCount());
        serverConfiguration.setBufferTimeoutInMillis(sourceConfig.getBufferTimeoutInMillis());
        return serverConfiguration;
    }
}
