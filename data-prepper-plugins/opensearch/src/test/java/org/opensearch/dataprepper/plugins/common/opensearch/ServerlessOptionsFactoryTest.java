package org.opensearch.dataprepper.plugins.common.opensearch;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ServerlessOptions;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServerlessOptionsFactoryTest {

    @Test
    void getOptionsShouldReturnEmptyForNonServerlessConnection() {
        ConnectionConfiguration connectionConfiguration = mock(ConnectionConfiguration.class);
        when(connectionConfiguration.isServerless()).thenReturn(false);

        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create(connectionConfiguration);

        assertFalse(result.isPresent());
    }

    @Test
    void getOptionsShouldReturnEmptyForBlankValuesInConnection() {
        ConnectionConfiguration connectionConfiguration = mock(ConnectionConfiguration.class);
        when(connectionConfiguration.isServerless()).thenReturn(true);
        when(connectionConfiguration.getServerlessNetworkPolicyName()).thenReturn(" ");
        when(connectionConfiguration.getServerlessCollectionName()).thenReturn(" ");
        when(connectionConfiguration.getServerlessVpceId()).thenReturn(" ");

        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create(connectionConfiguration);

        assertFalse(result.isPresent());
    }

    @Test
    void getOptionsShouldReturnNonEmptyForValidConnectionConfiguration() {
        ConnectionConfiguration connectionConfiguration = mock(ConnectionConfiguration.class);
        when(connectionConfiguration.isServerless()).thenReturn(true);
        when(connectionConfiguration.getServerlessNetworkPolicyName()).thenReturn("policyName");
        when(connectionConfiguration.getServerlessCollectionName()).thenReturn("collectionName");
        when(connectionConfiguration.getServerlessVpceId()).thenReturn("vpceId");

        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create(connectionConfiguration);

        assertTrue(result.isPresent());
        result.ifPresent(options -> {
            assertEquals("policyName", options.getNetworkPolicyName());
            assertEquals("collectionName", options.getCollectionName());
            assertEquals("vpceId", options.getVpceId());
        });
    }

    @Test
    void getOptionsShouldReturnEmptyForNullAwsConfig() {
        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create((AwsAuthenticationConfiguration) null);

        assertFalse(result.isPresent());
    }

    @Test
    void getOptionsShouldReturnEmptyForNonServerlessAwsConfig() {
        AwsAuthenticationConfiguration awsConfig = mock(AwsAuthenticationConfiguration.class);
        when(awsConfig.isServerlessCollection()).thenReturn(false);

        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create(awsConfig);

        assertFalse(result.isPresent());
    }

    @Test
    void getOptionsShouldReturnEmptyForNullServerlessOptionsInAwsConfig() {
        AwsAuthenticationConfiguration awsConfig = mock(AwsAuthenticationConfiguration.class);
        when(awsConfig.isServerlessCollection()).thenReturn(true);
        when(awsConfig.getServerlessOptions()).thenReturn(null);

        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create(awsConfig);

        assertFalse(result.isPresent());
    }

    @Test
    void getOptionsShouldReturnEmptyForBlankValuesInServerlessOptions() {
        AwsAuthenticationConfiguration awsConfig = mock(AwsAuthenticationConfiguration.class);
        ServerlessOptions serverlessOptions = new ServerlessOptions(" ", " ", " ");

        when(awsConfig.isServerlessCollection()).thenReturn(true);
        when(awsConfig.getServerlessOptions()).thenReturn(serverlessOptions);

        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create(awsConfig);

        assertFalse(result.isPresent());
    }

    @Test
    void getOptionsShouldReturnNonEmptyForValidAwsConfig() {
        AwsAuthenticationConfiguration awsConfig = mock(AwsAuthenticationConfiguration.class);
        ServerlessOptions serverlessOptions = new ServerlessOptions("policyName", "collectionName", "vpceId");

        when(awsConfig.isServerlessCollection()).thenReturn(true);
        when(awsConfig.getServerlessOptions()).thenReturn(serverlessOptions);

        Optional<ServerlessOptions> result = ServerlessOptionsFactory.create(awsConfig);

        assertTrue(result.isPresent());
        result.ifPresent(options -> {
            assertEquals("policyName", options.getNetworkPolicyName());
            assertEquals("collectionName", options.getCollectionName());
            assertEquals("vpceId", options.getVpceId());
        });
    }
}
