package org.opensearch.dataprepper.plugins.common.opensearch;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ServerlessOptions;

import java.util.Optional;

public class ServerlessOptionsFactory {

    public static Optional<ServerlessOptions> create(final ConnectionConfiguration connectionConfiguration) {
        if (!connectionConfiguration.isServerless() ||
            StringUtils.isBlank(connectionConfiguration.getServerlessNetworkPolicyName()) ||
            StringUtils.isBlank(connectionConfiguration.getServerlessCollectionName()) ||
            StringUtils.isBlank(connectionConfiguration.getServerlessVpceId())
        ) {
            return Optional.empty();
        }

        return Optional.of(new ServerlessOptions(
            connectionConfiguration.getServerlessNetworkPolicyName(),
            connectionConfiguration.getServerlessCollectionName(),
            connectionConfiguration.getServerlessVpceId()));
    }

    public static Optional<ServerlessOptions> create(final AwsAuthenticationConfiguration awsConfig) {
        if (awsConfig == null || !awsConfig.isServerlessCollection()) {
            return Optional.empty();
        }

        final ServerlessOptions serverlessOptions = awsConfig.getServerlessOptions();
        if (serverlessOptions == null ||
            StringUtils.isBlank(serverlessOptions.getNetworkPolicyName()) ||
            StringUtils.isBlank(serverlessOptions.getCollectionName()) ||
            StringUtils.isBlank(serverlessOptions.getVpceId())
        ) {
            return Optional.empty();
        }

        return Optional.of(serverlessOptions);
    }

}
