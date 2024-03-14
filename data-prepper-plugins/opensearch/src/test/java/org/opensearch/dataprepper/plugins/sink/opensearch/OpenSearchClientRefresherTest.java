package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.function.BiFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenSearchClientRefresherTest {
    private static final String TEST_USERNAME = "test_user";
    private static final String TEST_PASSWORD = "test_password";
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private BiFunction<AwsCredentialsSupplier, ConnectionConfiguration, OpenSearchClient> clientBiFunction;

    @Mock
    private ConnectionConfiguration connectionConfiguration;

    @Mock
    private OpenSearchClient openSearchClient;

    private OpenSearchClientRefresher createObjectUnderTest() {
        return new OpenSearchClientRefresher(
                awsCredentialsSupplier, openSearchClient, connectionConfiguration, clientBiFunction);
    }

    @Test
    void testGet() {
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
    }

    @Test
    void testGetAfterUpdateWithBasicAuthUnchanged() {
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        when(connectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(connectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final PluginSetting newConfig = mock(PluginSetting.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        when(newConnectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(newConnectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verifyNoInteractions(clientBiFunction);
    }

    @Test
    void testGetAfterUpdateWithUsernameChanged() {
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        when(connectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        final PluginSetting newConfig = mock(PluginSetting.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        when(newConnectionConfiguration.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientBiFunction.apply(eq(awsCredentialsSupplier), eq(newConnectionConfiguration)))
                .thenReturn(newClient);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(newClient));
    }

    @Test
    void testGetAfterUpdateWithPasswordChanged() {
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        when(connectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(connectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final PluginSetting newConfig = mock(PluginSetting.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        when(newConnectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(newConnectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientBiFunction.apply(eq(awsCredentialsSupplier), eq(newConnectionConfiguration)))
                .thenReturn(newClient);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(newClient));
    }
}