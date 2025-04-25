package org.opensearch.dataprepper.plugins.source.crowdstrike.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.crowdstrike.CrowdStrikeSourceConfig;
import org.opensearch.dataprepper.plugins.source.crowdstrike.configuration.AuthenticationConfig;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CrowdStrikeAuthClientTest {

    @Mock
    private RestTemplate restTemplateMock;
    private CrowdStrikeAuthClient authClient;
    @Mock
    private CrowdStrikeSourceConfig mockSourceConfig;


    @BeforeEach
    void setUp() {
        AuthenticationConfig authConfig = mock(AuthenticationConfig.class);
        when(mockSourceConfig.getAuthenticationConfig()).thenReturn(authConfig);
        when(authConfig.getClientId()).thenReturn("test-id");
        when(authConfig.getClientSecret()).thenReturn("test-secret");
        authClient = new CrowdStrikeAuthClient(mockSourceConfig);
        authClient.restTemplate = restTemplateMock;
    }

    @Test
    void testInitCredentials_success() {
        Map<String, Object> mockResponse = Map.of(
                "access_token", "mock-token",
                "expires_in", 3600
        );
        when(restTemplateMock.postForEntity(anyString(), any(), eq(Map.class)))
                .thenReturn(ResponseEntity.ok(mockResponse));
        authClient.initCredentials();
        assertEquals("mock-token", authClient.getBearerToken());
        assertNotNull(authClient.getExpireTime());
    }

    @Test
    void testHttpClientErrorExceptionHandled() {
        HttpClientErrorException forbidden = HttpClientErrorException.create(
                HttpStatus.FORBIDDEN, "Forbidden", HttpHeaders.EMPTY, null, null);

        when(restTemplateMock.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(forbidden);

        RuntimeException ex = assertThrows(RuntimeException.class, authClient::initCredentials);
        assertTrue(ex.getMessage().contains("Failed to acquire access token even after 6 retry attempts"));
        assertEquals(Instant.ofEpochMilli(0), authClient.getExpireTime());
    }

    @Test
    void testTooManyRequestsBacksOffAndRetries() {
        Map<String, Object> tokenMap = new HashMap<>();
        tokenMap.put("access_token", "mock-token");
        tokenMap.put("expires_in", 3600);

        HttpClientErrorException tooManyRequestsException =
                HttpClientErrorException.create(HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests",
                        null, null, null);

        when(restTemplateMock.postForEntity(any(String.class), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(tooManyRequestsException)
                .thenReturn(new ResponseEntity<>(tokenMap, HttpStatus.OK)); // 2nd succeeds

        authClient.initCredentials();

        assertEquals("mock-token", authClient.getBearerToken());
        assertTrue(authClient.getExpireTime().isAfter(Instant.now()));
    }

    @Test
    void testRefreshToken_skipsIfTokenIsValidInitially() {
        CrowdStrikeAuthClient client = spy(new CrowdStrikeAuthClient(mockSourceConfig) {
            @Override
            protected boolean isTokenValid() {
                return true;
            }

            @Override
            protected void getAuthToken() {
                fail("getAuthToken should not be called when token is already valid.");
            }
        });

        client.refreshToken();
        verify(client, times(1)).isTokenValid();
        verify(client, never()).getAuthToken();
    }
}
