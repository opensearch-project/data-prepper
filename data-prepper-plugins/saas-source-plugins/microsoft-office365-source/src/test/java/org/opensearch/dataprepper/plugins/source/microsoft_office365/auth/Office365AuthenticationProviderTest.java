package org.opensearch.dataprepper.plugins.source.microsoft_office365.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
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
    private Oauth2Config oAuth2Config;

    @Mock
    private PluginConfigVariable clientIdVariable;

    @Mock
    private PluginConfigVariable clientSecretVariable;

    private Office365AuthenticationProvider authProvider;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        // Use lenient mocks to avoid UnnecessaryStubbingException
        lenient().when(config.getAuthenticationConfiguration()).thenReturn(authConfig);
        lenient().when(authConfig.getOauth2()).thenReturn(oAuth2Config);
        lenient().when(oAuth2Config.getClientId()).thenReturn(clientIdVariable);
        lenient().when(oAuth2Config.getClientSecret()).thenReturn(clientSecretVariable);
        lenient().when(clientIdVariable.getValue()).thenReturn("testClientId");
        lenient().when(clientSecretVariable.getValue()).thenReturn("testClientSecret");
        lenient().when(config.getTenantId()).thenReturn("testTenantId");

        authProvider = new Office365AuthenticationProvider(config);
        ReflectivelySetField.setField(Office365AuthenticationProvider.class, authProvider, "restTemplate", restTemplate);
    }

    @Test
    void testCredentialRetrieval() throws NoSuchFieldException, IllegalAccessException {
        mockSuccessfulTokenResponse();

        // Test init
        authProvider.initCredentials();
        assertEquals("testAccessToken", authProvider.getAccessToken());

        // Verify that refreshAndRetrieveValue was called on both config variables
        verify(clientIdVariable).refreshAndRetrieveValue();
        verify(clientSecretVariable).refreshAndRetrieveValue();

        // Clear the token and test renew directly
        ReflectivelySetField.setField(Office365AuthenticationProvider.class, authProvider, "accessToken", null);
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

    @Test
    void testGetAccessTokenWithLazyInitialization() throws NoSuchFieldException, IllegalAccessException {
        mockSuccessfulTokenResponse();

        // Ensure access token is null initially
        ReflectivelySetField.setField(Office365AuthenticationProvider.class, authProvider, "accessToken", null);

        // Call getAccessToken which should trigger initialization
        String token = authProvider.getAccessToken();

        assertEquals("testAccessToken", token);
        verify(clientIdVariable).refreshAndRetrieveValue();
        verify(clientSecretVariable).refreshAndRetrieveValue();
    }

    @Test
    void testConcurrentAccessTokenInitialization() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        mockSuccessfulTokenResponse();

        // Ensure access token is null initially
        ReflectivelySetField.setField(Office365AuthenticationProvider.class, authProvider, "accessToken", null);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        String[] results = new String[threadCount];

        // Start multiple threads trying to get access token simultaneously
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    results[index] = authProvider.getAccessToken();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all threads to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        // Verify all threads got the same token
        for (String result : results) {
            assertEquals("testAccessToken", result);
        }

        // Verify initCredentials was called only once due to double-checked locking
        verify(clientIdVariable, times(1)).refreshAndRetrieveValue();
        verify(clientSecretVariable, times(1)).refreshAndRetrieveValue();
    }

    @Test
    void testIsCredentialsInitializedReturnsFalseInitially() {
        // Credentials should not be initialized initially
        assertFalse(authProvider.isCredentialsInitialized());
    }

    @Test
    void testIsCredentialsInitializedReturnsTrueAfterSuccessfulInit() {
        mockSuccessfulTokenResponse();

        // Initialize credentials
        authProvider.initCredentials();

        // Should return true after successful initialization
        assertTrue(authProvider.isCredentialsInitialized());
    }

    @Test
    void testInitCredentialsBlocksUntilSuccessfulInitialization() throws InterruptedException {
        // Mock multiple failures followed by success
        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.BAD_REQUEST))
                .thenThrow(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR))
                .thenReturn(createSuccessfulTokenResponse());

        // Use a separate thread to test the retry behavior
        AtomicReference<Exception> thrownException = new AtomicReference<>();
        
        Thread initThread = new Thread(() -> {
            try {
                authProvider.initCredentials();
            } catch (Exception e) {
                thrownException.set(e);
            }
        });

        initThread.start();
        
        // Wait a bit for the first attempt to fail and the retry to start
        Thread.sleep(500);
        
        // Interrupt the thread to stop the long wait
        initThread.interrupt();
        initThread.join(2000); // Wait for thread to finish

        // Verify that the REST call was made at least once (the first failure)
        verify(restTemplate, times(1)).postForEntity(anyString(), any(HttpEntity.class), eq(Map.class));
    }

    @Test
    void testInitCredentialsHandlesInterruptedException() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        // Mock to always fail to trigger retry logic
        when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.BAD_REQUEST));

        AtomicReference<Exception> thrownException = new AtomicReference<>();
        
        Thread initThread = new Thread(() -> {
            try {
                authProvider.initCredentials();
            } catch (Exception e) {
                thrownException.set(e);
            }
        });

        initThread.start();
        
        // Give the thread a moment to start and enter the retry loop
        Thread.sleep(100);
        
        // Interrupt the thread while it's sleeping
        initThread.interrupt();
        initThread.join(1000); // Wait for thread to finish

        // Verify that a RuntimeException was thrown due to interruption
        assertNotNull(thrownException.get());
        assertTrue(thrownException.get() instanceof RuntimeException);
        assertTrue(thrownException.get().getMessage().contains("Interrupted while waiting to retry"));
        
        // Verify credentials were not initialized due to interruption
        assertFalse(authProvider.isCredentialsInitialized());
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
