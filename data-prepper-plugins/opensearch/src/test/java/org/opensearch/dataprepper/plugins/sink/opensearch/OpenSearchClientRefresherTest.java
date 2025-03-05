package org.opensearch.dataprepper.plugins.sink.opensearch;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;

import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchClientRefresher.CLIENT_REFRESH_ERRORS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchClientRefresher.CREDENTIALS_CHANGED;

@ExtendWith(MockitoExtension.class)
class OpenSearchClientRefresherTest {
    private static final String TEST_USERNAME = "test_user";
    private static final String TEST_PASSWORD = "test_password";

    @Mock
    private Function<ConnectionConfiguration, OpenSearchClient> clientFunction;

    @Mock
    private ConnectionConfiguration connectionConfiguration;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private AuthConfig authConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter credentialsChangeCounter;

    @Mock
    private Counter clientRefreshErrorsCounter;

    private OpenSearchClientRefresher createObjectUnderTest() {
        return new OpenSearchClientRefresher(pluginMetrics, connectionConfiguration, clientFunction);
    }

    @BeforeEach
    void setUp() {
        when(clientFunction.apply(eq(connectionConfiguration))).thenReturn(openSearchClient);
    }

    @Test
    void testGet() {
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
    }

    @Test
    void testGetAfterUpdateWithDeprecatedBasicAuthUnchanged() {
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(clientFunction, times(1)).apply(any());
        when(connectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(connectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSinkConfig newConfig = mock(OpenSearchSinkConfig.class);
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
        verifyNoMoreInteractions(clientFunction);
    }

    @Test
    void testGetAfterUpdateWithBasicAuthUnchanged() {
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(clientFunction, times(1)).apply(any());
        when(connectionConfiguration.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(authConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSinkConfig newConfig = mock(OpenSearchSinkConfig.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        when(newConnectionConfiguration.getAuthConfig()).thenReturn(newAuthConfig);
        when(newAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verifyNoMoreInteractions(clientFunction);
    }

    @Test
    void testGetAfterUpdateWithDeprecatedUsernameChanged() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(clientFunction, times(1)).apply(any());
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        when(connectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        final OpenSearchSinkConfig newConfig = mock(OpenSearchSinkConfig.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        when(newConnectionConfiguration.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientFunction.apply(eq(newConnectionConfiguration))).thenReturn(newClient);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(newClient));
        verify(credentialsChangeCounter).increment();
        verify(clientFunction, times(2)).apply(any());
    }

    @Test
    void testGetAfterUpdateWithUsernameChanged() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(clientFunction, times(1)).apply(any());
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        when(connectionConfiguration.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(authConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSinkConfig newConfig = mock(OpenSearchSinkConfig.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        when(newConnectionConfiguration.getAuthConfig()).thenReturn(newAuthConfig);
        when(newAuthConfig.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientFunction.apply(eq(newConnectionConfiguration))).thenReturn(newClient);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(newClient));
        verify(credentialsChangeCounter).increment();
        verify(clientFunction, times(2)).apply(any());
    }

    @Test
    void testGetAfterUpdateWithDeprecatedPasswordChanged() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(clientFunction, times(1)).apply(any());
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        when(connectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(connectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSinkConfig newConfig = mock(OpenSearchSinkConfig.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        when(newConnectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(newConnectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientFunction.apply(eq(newConnectionConfiguration))).thenReturn(newClient);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(newClient));
        verify(credentialsChangeCounter).increment();
        verify(clientFunction, times(2)).apply(any());
    }

    @Test
    void testGetAfterUpdateWithPasswordChanged() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(clientFunction, times(1)).apply(any());
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        when(connectionConfiguration.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(authConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSinkConfig newConfig = mock(OpenSearchSinkConfig.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        when(newConnectionConfiguration.getAuthConfig()).thenReturn(newAuthConfig);
        when(newAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newAuthConfig.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientFunction.apply(eq(newConnectionConfiguration))).thenReturn(newClient);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(newClient));
        verify(credentialsChangeCounter).increment();
        verify(clientFunction, times(2)).apply(any());
    }

    @Test
    void testGetAfterUpdateClientFailure() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        when(pluginMetrics.counter(CLIENT_REFRESH_ERRORS)).thenReturn(clientRefreshErrorsCounter);
        final OpenSearchClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(clientFunction, times(1)).apply(any());
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        when(connectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(connectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSinkConfig newConfig = mock(OpenSearchSinkConfig.class);
        final ConnectionConfiguration newConnectionConfiguration = mock(ConnectionConfiguration.class);
        when(newConnectionConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(newConnectionConfiguration.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientFunction.apply(eq(newConnectionConfiguration))).thenThrow(RuntimeException.class);
        try (MockedStatic<ConnectionConfiguration> configurationMockedStatic = mockStatic(
                ConnectionConfiguration.class)) {
            configurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(eq(newConfig)))
                    .thenReturn(newConnectionConfiguration);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), equalTo(openSearchClient));
        verify(credentialsChangeCounter).increment();
        verify(clientRefreshErrorsCounter).increment();
        verify(clientFunction, times(2)).apply(any());
    }
}