package org.opensearch.dataprepper.plugins.source.microsoft_office365.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Office365AuthenticationProviderTest {

    @Mock
    private Office365SourceConfig config;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private AuthenticationConfiguration authConfig;

    @Mock
    private AuthenticationConfiguration.OAuth2Credentials oAuth2Credentials;

    private Office365AuthenticationProvider authProvider;

    @BeforeEach
    void setUp() {
        when(config.getAuthenticationConfiguration()).thenReturn(authConfig);
        when(authConfig.getOauth2()).thenReturn(oAuth2Credentials);
        when(oAuth2Credentials.getClientId()).thenReturn("testClientId");
        when(oAuth2Credentials.getClientSecret()).thenReturn("testClientSecret");
        when(config.getTenantId()).thenReturn("testTenantId");

        authProvider = new Office365AuthenticationProvider(config);
        ReflectionTestUtils.setField(authProvider, "restTemplate", restTemplate);
    }

    @Test
    void testCredentialRetrieval() {
        mockSuccessfulTokenResponse();

        // Test init
        authProvider.initCredentials();
        assertEquals("testAccessToken", authProvider.getAccessToken());

        // Clear the token and test renew directly
        ReflectionTestUtils.setField(authProvider, "accessToken", null);
        authProvider.renewCredentials();
        assertEquals("testAccessToken", authProvider.getAccessToken());
    }

    @Test
    void testRenewCredentialsWithRetry() {
        // Mock a failure followed by a success
        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR))
                .thenReturn(createSuccessfulTokenResponse());

        authProvider.renewCredentials();

        assertEquals("testAccessToken", authProvider.getAccessToken());
        verify(restTemplate, times(2)).postForEntity(anyString(), any(HttpEntity.class), eq(Map.class));
    }

    @Test
    void testRenewCredentialsWithPermanentFailure() {
        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.UNAUTHORIZED));

        assertThrows(RuntimeException.class, () -> authProvider.renewCredentials());
    }

    @Test
    void testRenewCredentialsWithInvalidResponse() {
        // Mock response without access_token
        Map<String, Object> tokenResponse = new HashMap<>();
        tokenResponse.put("expires_in", 3600);
        ResponseEntity<Map> responseEntity = new ResponseEntity<>(tokenResponse, HttpStatus.OK);

        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(responseEntity);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> authProvider.renewCredentials());
        assertEquals("Invalid token response: missing access_token", exception.getMessage());
    }

    private void mockSuccessfulTokenResponse() {
        ResponseEntity<Map> responseEntity = createSuccessfulTokenResponse();
        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(responseEntity);
    }

    private ResponseEntity<Map> createSuccessfulTokenResponse() {
        Map<String, Object> tokenResponse = new HashMap<>();
        tokenResponse.put("access_token", "testAccessToken");
        tokenResponse.put("expires_in", 3600);
        return new ResponseEntity<>(tokenResponse, HttpStatus.OK);
    }
}