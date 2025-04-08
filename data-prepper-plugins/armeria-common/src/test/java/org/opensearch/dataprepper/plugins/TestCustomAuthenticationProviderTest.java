package org.opensearch.dataprepper.plugins;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.testcustomauth.TestCustomAuthenticationConfig;
import org.opensearch.dataprepper.plugins.testcustomauth.TestCustomGrpcAuthenticationProvider;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestCustomAuthenticationProviderTest {

    private static final String TOKEN = "test-token";
    private static final String HEADER = "authentication";

    @Mock
    private TestCustomAuthenticationConfig config;

    private TestCustomGrpcAuthenticationProvider provider;

    @BeforeEach
    void setUp() {
        when(config.customToken()).thenReturn(TOKEN);
        when(config.header()).thenReturn(HEADER);

        provider = new TestCustomGrpcAuthenticationProvider(config);
    }

    @Test
    void testGetHttpAuthenticationService_shouldReturnValidOptional() {
        var optionalService = provider.getHttpAuthenticationService();
        Assertions.assertTrue(optionalService.isPresent());
    }

    @Test
    void testGetAuthenticationInterceptor_shouldReturnNonNull() {
        var interceptor = provider.getAuthenticationInterceptor();
        Assertions.assertNotNull(interceptor);
    }
}

