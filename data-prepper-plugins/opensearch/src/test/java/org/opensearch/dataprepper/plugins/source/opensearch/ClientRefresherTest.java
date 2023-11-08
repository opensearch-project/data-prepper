package org.opensearch.dataprepper.plugins.source.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClientRefresherTest {
    private static final String TEST_USERNAME = "test_user";
    private static final String TEST_PASSWORD = "test_password";

    @Mock
    private Function<OpenSearchSourceConfiguration, Object> clientFunction;

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private Object client;

    private ClientRefresher createObjectUnderTest() {
        return new ClientRefresher(Object.class, clientFunction, openSearchSourceConfiguration);
    }

    @BeforeEach
    void setup() {
        when(clientFunction.apply(eq(openSearchSourceConfiguration))).thenReturn(client);
    }

    @Test
    void testGet() {
        final ClientRefresher objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.get(), equalTo(client));
    }

    @Test
    void testGetAfterUpdateWithBasicAuthUnchanged() {
        final ClientRefresher objectUnderTest = createObjectUnderTest();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSourceConfiguration newConfig = mock(OpenSearchSourceConfiguration.class);
        when(newConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newConfig.getPassword()).thenReturn(TEST_PASSWORD);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(client));
    }

    @Test
    void testGetAfterUpdateWithUsernameChanged() {
        final ClientRefresher objectUnderTest = createObjectUnderTest();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        final OpenSearchSourceConfiguration newConfig = mock(OpenSearchSourceConfiguration.class);
        when(newConfig.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientFunction.apply(eq(newConfig))).thenReturn(newClient);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(newClient));
    }

    @Test
    void testGetAfterUpdateWithPasswordChanged() {
        final ClientRefresher objectUnderTest = createObjectUnderTest();
        when(openSearchSourceConfiguration.getUsername()).thenReturn(TEST_USERNAME);
        when(openSearchSourceConfiguration.getPassword()).thenReturn(TEST_PASSWORD);
        final OpenSearchSourceConfiguration newConfig = mock(OpenSearchSourceConfiguration.class);
        when(newConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newConfig.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final OpenSearchClient newClient = mock(OpenSearchClient.class);
        when(clientFunction.apply(eq(newConfig))).thenReturn(newClient);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(newClient));
    }
}