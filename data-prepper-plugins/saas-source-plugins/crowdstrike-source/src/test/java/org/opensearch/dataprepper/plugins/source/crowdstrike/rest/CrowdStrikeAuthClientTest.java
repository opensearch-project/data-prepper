package org.opensearch.dataprepper.plugins.source.crowdstrike.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.plugins.source.crowdstrike.CrowdStrikeSourceConfig;
import org.opensearch.dataprepper.plugins.source.crowdstrike.configuration.AuthenticationConfig;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CrowdStrikeAuthClientTest {

    private CrowdStrikeSourceConfig mockSourceConfig;
    private WebClient.Builder mockWebClientBuilder;
    private WebClient.ResponseSpec responseSpec;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        mockSourceConfig = mock(CrowdStrikeSourceConfig.class);
        AuthenticationConfig mockAuthConfig = mock(AuthenticationConfig.class);

        when(mockSourceConfig.getAuthenticationConfig()).thenReturn(mockAuthConfig);
        when(mockAuthConfig.getClientId()).thenReturn("test-client-id");
        when(mockAuthConfig.getClientSecret()).thenReturn("test-client-secret");

        // Mock the full WebClient chain
        mockWebClientBuilder = mock(WebClient.Builder.class);
        WebClient mockWebClient = mock(WebClient.class);
        WebClient.RequestBodyUriSpec uriSpec = mock(WebClient.RequestBodyUriSpec.class);
        WebClient.RequestBodySpec bodySpec = mock(WebClient.RequestBodySpec.class);
        WebClient.RequestHeadersSpec headersSpec = mock(WebClient.RequestHeadersSpec.class);
        responseSpec = mock(WebClient.ResponseSpec.class);

        when(mockWebClientBuilder.build()).thenReturn(mockWebClient);
        when(mockWebClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri(anyString())).thenReturn(bodySpec);
        when(bodySpec.contentType(any())).thenReturn(bodySpec);
        when(bodySpec.bodyValue(any())).thenReturn(headersSpec);
        when(headersSpec.retrieve()).thenReturn(responseSpec);
    }

    @Test
    void testInitCredentials_shouldSetBearerTokenAndExpiry() {
        Map<String, Object> mockResponse = Map.of(
                "access_token", "test-access-token",
                "expires_in", 3600
        );

        when(responseSpec.bodyToMono(Map.class)).thenReturn(Mono.just(mockResponse));

        try (MockedStatic<WebClient> webClientStatic = mockStatic(WebClient.class)) {
            webClientStatic.when(WebClient::builder).thenReturn(mockWebClientBuilder);

            CrowdStrikeAuthClient client = new CrowdStrikeAuthClient(mockSourceConfig);
            Instant beforeCall = Instant.now();
            client.initCredentials();

            assertEquals("test-access-token", client.getBearerToken());
            assertNotNull(client.getExpireTime());
            assertTrue(client.getExpireTime().isAfter(beforeCall));
            assertFalse(client.isTokenExpired());
        }
    }

    @Test
    void testInitCredentials_whenTokenMissing_shouldThrowException() {
        Map<String, Object> badResponse = Map.of(); // No access_token

        when(responseSpec.bodyToMono(Map.class)).thenReturn(Mono.just(badResponse));

        try (MockedStatic<WebClient> webClientStatic = mockStatic(WebClient.class)) {
            webClientStatic.when(WebClient::builder).thenReturn(mockWebClientBuilder);

            CrowdStrikeAuthClient client = new CrowdStrikeAuthClient(mockSourceConfig);

            RuntimeException ex = assertThrows(RuntimeException.class, client::initCredentials);
            assertTrue(ex.getMessage().contains("access_token"));
        }
    }
}