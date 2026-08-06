package org.opensearch.dataprepper.plugins.source.microsoft_office365.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.plugins.source.source_crawler.metrics.VendorAPIMetricsRecorder;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.atLeast;
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

    @Mock
    private VendorAPIMetricsRecorder metricsRecorder;

    private Office365AuthenticationProvider authProvider;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        authProvider = new Office365AuthenticationProvider(config, metricsRecorder);
        ReflectivelySetField.setField(Office365AuthenticationProvider.class, authProvider, "restTemplate", restTemplate);
    }

    @Nested
    class WithAuthMocks {
        
        @BeforeEach
        void setupAuthMocks() {
            when(config.getAuthenticationConfiguration()).thenReturn(authConfig);
            when(authConfig.getOauth2()).thenReturn(oAuth2Config);
            when(oAuth2Config.getClientId()).thenReturn(clientIdVariable);
            when(oAuth2Config.getClientSecret()).thenReturn(clientSecretVariable);
            when(clientIdVariable.getValue()).thenReturn("testClientId");
            when(clientSecretVariable.getValue()).thenReturn("testClientSecret");
            when(config.getTenantId()).thenReturn("testTenantId");
            
            // Mock the metrics recorder to execute the supplier properly for authentication tests
            when(metricsRecorder.recordAuthLatency(any(Supplier.class))).thenAnswer(invocation -> {
                Supplier<Object> supplier = invocation.getArgument(0);
                return supplier.get();
            });
        }

        @Test
        void testCredentialRetrieval() throws NoSuchFieldException, IllegalAccessException {
            mockSuccessfulTokenResponse();

            // Test init
            authProvider.initCredentials();
            assertEquals("testAccessToken", authProvider.getAccessToken());

            verify(clientIdVariable).refresh();
            verify(clientSecretVariable).refresh();

            ReflectivelySetField.setField(Office365AuthenticationProvider.class, authProvider, "accessToken", null);
            authProvider.renewCredentials();
            assertEquals("testAccessToken", authProvider.getAccessToken());
        }

        @Test
        void testGetAccessTokenWithLazyInitialization() throws NoSuchFieldException, IllegalAccessException {
            mockSuccessfulTokenResponse();

            // Ensure access token is null initially
            ReflectivelySetField.setField(Office365AuthenticationProvider.class, authProvider, "accessToken", null);

            // Call getAccessToken which should trigger initialization
            String token = authProvider.getAccessToken();

            assertEquals("testAccessToken", token);
            verify(clientIdVariable).refresh();
            verify(clientSecretVariable).refresh();
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

            // Verify we have the expected number of results
            assertEquals(threadCount, results.length);
            
            // Verify all threads got the same token
            for (String result : results) {
                assertEquals("testAccessToken", result);
            }

            // Verify initCredentials was called only once due to double-checked locking
            verify(clientIdVariable, times(1)).refresh();
            verify(clientSecretVariable, times(1)).refresh();
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
        void testRenewCredentialsRecordsMetricsOnSuccess() {
            mockSuccessfulTokenResponse();

            authProvider.renewCredentials();

            // Verify metrics are recorded for successful authentication
            verify(metricsRecorder, times(1)).recordAuthLatency(any(Supplier.class));
            verify(metricsRecorder, times(1)).recordAuthSuccess();
            verify(metricsRecorder, times(0)).recordAuthFailure();
            verify(metricsRecorder, times(0)).recordError(any());
        }

        @Test
        void testRenewCredentialsRecordsMetricsOnFailure() {
            when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                    .thenThrow(new HttpClientErrorException(HttpStatus.UNAUTHORIZED));

            assertThrows(SaaSCrawlerException.class, () -> authProvider.renewCredentials());

            // Verify metrics are recorded for failed authentication
            verify(metricsRecorder, times(1)).recordAuthLatency(any(Supplier.class));
            verify(metricsRecorder, times(1)).recordAuthFailure();
            verify(metricsRecorder, times(1)).recordError(any(SaaSCrawlerException.class));
            verify(metricsRecorder, times(0)).recordAuthSuccess();
        }

        @Test
        void testRenewCredentialsRecordsMetricsOnInvalidResponse() {
            // Mock response without access_token
            Map<String, Object> tokenResponse = new HashMap<>();
            tokenResponse.put("expires_in", 3600);
            ResponseEntity<Map> responseEntity = new ResponseEntity<>(tokenResponse, HttpStatus.OK);

            when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                    .thenReturn(responseEntity);

            assertThrows(SaaSCrawlerException.class, () -> authProvider.renewCredentials());

            // Verify metrics are recorded for invalid response
            verify(metricsRecorder, times(1)).recordAuthLatency(any(Supplier.class));
            verify(metricsRecorder, times(1)).recordAuthFailure();
            verify(metricsRecorder, times(1)).recordError(any(SaaSCrawlerException.class));
            verify(metricsRecorder, times(0)).recordAuthSuccess();
        }

        @Test
        void testRenewCredentialsWithPermanentFailure() {
            when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                    .thenThrow(new HttpClientErrorException(HttpStatus.UNAUTHORIZED));

            assertThrows(SaaSCrawlerException.class, () -> authProvider.renewCredentials());
        }

        @Test
        void testRenewCredentialsWithInvalidResponse() {
            // Mock response without access_token
            Map<String, Object> tokenResponse = new HashMap<>();
            tokenResponse.put("expires_in", 3600);
            ResponseEntity<Map> responseEntity = new ResponseEntity<>(tokenResponse, HttpStatus.OK);

            when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                    .thenReturn(responseEntity);

            SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                    () -> authProvider.renewCredentials());
            assertEquals("Invalid token response: missing access_token", exception.getMessage());
        }

        @Test
        void testInitCredentialsBlocksUntilSuccessfulInitialization() throws InterruptedException {
            // Mock failures within RetryHandler followed by success in next renewCredentials call
            when(restTemplate.postForEntity(anyString(), any(HttpEntity.class), eq(Map.class)))
                    .thenThrow(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR))
                    .thenThrow(new RuntimeException())
                    .thenReturn(createSuccessfulTokenResponse());

            // Start initialization in a separate thread
            Thread initThread = new Thread(() -> authProvider.initCredentials());
            initThread.start();

            await().atMost(java.time.Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        // Verify that multiple REST calls were made
                        verify(restTemplate, atLeast(2)).postForEntity(anyString(), any(HttpEntity.class), eq(Map.class));
                    });

            // Wait for thread to complete
            initThread.join(2000);

            assertEquals("testAccessToken", authProvider.getAccessToken());
            assertTrue(authProvider.isCredentialsInitialized());
        }

        @Test
        void testInitCredentialsHandlesInterruptedException() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
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

            // Verify that a SaaSCrawlerException was thrown due to interruption
            assertNotNull(thrownException.get());
            assertThat(thrownException.get(), instanceOf(SaaSCrawlerException.class));
            
            // Verify credentials were not initialized due to interruption
            assertFalse(authProvider.isCredentialsInitialized());
        }
    }

    @Test
    void testIsCredentialsInitializedReturnsFalseInitially() {
        // Credentials should not be initialized initially
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
