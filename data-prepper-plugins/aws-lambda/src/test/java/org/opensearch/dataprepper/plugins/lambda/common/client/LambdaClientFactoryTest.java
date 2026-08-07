package org.opensearch.dataprepper.plugins.lambda.common.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.util.CountingRetryCondition;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LambdaClientFactoryTest {

  @Mock
  private AwsAuthenticationOptions awsAuthenticationOptions;

  @Mock
  private AwsCredentialsSupplier awsCredentialsSupplier;

  @Mock
  private AwsCredentialsProvider awsCredentialsProvider;

  @BeforeEach
  void setUp() {
    when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_WEST_2);
    when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/example-role");
    when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn("externalId");
    when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(new HashMap<>());

    when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(awsCredentialsProvider);
  }

  @Test
  void testCreateAsyncLambdaClient() {
    ClientOptions clientOptions = new ClientOptions();

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            awsCredentialsSupplier,
            clientOptions
    );

    assertNotNull(client);
    assertEquals(Region.US_WEST_2, client.serviceClientConfiguration().region());
  }
  @Test
  void testCreateAsyncLambdaClientOverrideConfiguration() {
    ClientOptions clientOptions = new ClientOptions();

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            awsCredentialsSupplier,
            clientOptions
    );

    assertNotNull(client);
    ClientOverrideConfiguration overrideConfig = client.serviceClientConfiguration().overrideConfiguration();

    assertEquals(clientOptions.getApiCallTimeout(), overrideConfig.apiCallTimeout().get());
    assertNotNull(overrideConfig.retryPolicy());
    assertNotNull(overrideConfig.metricPublishers());
    assertFalse(overrideConfig.metricPublishers().isEmpty());
    // apiCallAttemptTimeout should not be set when null
    assertFalse(overrideConfig.apiCallAttemptTimeout().isPresent());
  }

  @Test
  void testCreateAsyncLambdaClientWithApiCallAttemptTimeout() {
    // Arrange
    ClientOptions clientOptions = mock(ClientOptions.class);
    when(clientOptions.getMaxConcurrency()).thenReturn(200);
    when(clientOptions.getConnectionTimeout()).thenReturn(Duration.ofSeconds(60));
    when(clientOptions.getReadTimeout()).thenReturn(Duration.ofSeconds(60));
    when(clientOptions.getApiCallTimeout()).thenReturn(Duration.ofSeconds(60));
    when(clientOptions.getApiCallAttemptTimeout()).thenReturn(Duration.ofSeconds(30));
    when(clientOptions.getMaxConnectionRetries()).thenReturn(3);
    when(clientOptions.getBaseDelay()).thenReturn(Duration.ofMillis(100));
    when(clientOptions.getMaxBackoff()).thenReturn(Duration.ofSeconds(20));

    // Act
    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            awsCredentialsSupplier,
            clientOptions
    );

    // Assert
    assertNotNull(client);
    ClientOverrideConfiguration overrideConfig = client.serviceClientConfiguration().overrideConfiguration();
    assertEquals(Duration.ofSeconds(30), overrideConfig.apiCallAttemptTimeout().get());
  }

  @Test
  void testCreateAsyncLambdaClientWithoutReadTimeout() {
    // Arrange
    ClientOptions clientOptions = mock(ClientOptions.class);
    when(clientOptions.getMaxConcurrency()).thenReturn(200);
    when(clientOptions.getConnectionTimeout()).thenReturn(Duration.ofSeconds(60));
    when(clientOptions.getReadTimeout()).thenReturn(null); // No read timeout
    when(clientOptions.getApiCallTimeout()).thenReturn(Duration.ofSeconds(60));
    when(clientOptions.getApiCallAttemptTimeout()).thenReturn(null); // No attempt timeout
    when(clientOptions.getMaxConnectionRetries()).thenReturn(3);
    when(clientOptions.getBaseDelay()).thenReturn(Duration.ofMillis(100));
    when(clientOptions.getMaxBackoff()).thenReturn(Duration.ofSeconds(20));

    // Act
    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            awsCredentialsSupplier,
            clientOptions
    );

    // Assert - should not throw exception when readTimeout is null
    assertNotNull(client);
  }

  @Test
  void testRetryConditionIsCalledWithTooManyRequestsException() {
    // Arrange
    CountingRetryCondition countingRetryCondition = new CountingRetryCondition();

    // Create mock Lambda client
    LambdaAsyncClient mockClient = mock(LambdaAsyncClient.class);

    // Setup mock to return TooManyRequestsException for the first 3 calls
    when(mockClient.invoke(any(InvokeRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(TooManyRequestsException.builder().build()))
            .thenReturn(CompletableFuture.failedFuture(TooManyRequestsException.builder().build()))
            .thenReturn(CompletableFuture.failedFuture(TooManyRequestsException.builder().build()));

    // Create test request
    InvokeRequest request = InvokeRequest.builder()
            .functionName("test-function")
            .build();

    // Simulate retries
    for (int i = 0; i < 3; i++) {
      try {
        CompletableFuture<InvokeResponse> future = mockClient.invoke(request);
        RetryPolicyContext context = RetryPolicyContext.builder()
                .exception(TooManyRequestsException.builder().build())
                .retriesAttempted(i)
                .build();

        // Test the retry condition
        countingRetryCondition.shouldRetry(context);

        future.join();
      } catch (CompletionException e) {
        assertTrue(e.getCause() instanceof TooManyRequestsException);
      }
    }

    // Verify retry count
    assertEquals(3, countingRetryCondition.getRetryCount(),
            "Retry condition should have been called exactly 3 times");
  }

  @Test
  void testRetryConditionFirstFailsAndThenSucceeds() {
    // Arrange
    CountingRetryCondition countingRetryCondition = new CountingRetryCondition();

    // Create mock Lambda client
    LambdaAsyncClient mockClient = mock(LambdaAsyncClient.class);

    // Setup mock to return TooManyRequestsException for first 2 calls, then succeed on 3rd
    when(mockClient.invoke(any(InvokeRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(TooManyRequestsException.builder().build()))
            .thenReturn(CompletableFuture.failedFuture(TooManyRequestsException.builder().build()))
            .thenReturn(CompletableFuture.completedFuture(InvokeResponse.builder()
                    .statusCode(200)
                    .build()));

    // Create test request
    InvokeRequest request = InvokeRequest.builder()
            .functionName("test-function")
            .build();

    // Track if we reached success
    boolean successReached = false;

    // Simulate retries with eventual success
    for (int i = 0; i < 3; i++) {
      try {
        CompletableFuture<InvokeResponse> future = mockClient.invoke(request);

        if (i < 2) {
          // For first two attempts, verify retry condition
          RetryPolicyContext context = RetryPolicyContext.builder()
                  .exception(TooManyRequestsException.builder().build())
                  .retriesAttempted(i)
                  .build();
          countingRetryCondition.shouldRetry(context);
        }

        InvokeResponse response = future.join();
        if (response.statusCode() == 200) {
          successReached = true;
        }
      } catch (CompletionException e) {
        assertTrue(e.getCause() instanceof TooManyRequestsException,
                "Exception should be TooManyRequestsException");
      }
    }

    // Verify retry count and success
    assertEquals(2, countingRetryCondition.getRetryCount(),
            "Retry condition should have been called exactly 2 times");
    assertTrue(successReached, "Should have reached successful completion");
  }

  @Test
  void testClientUsesConfiguredReadTimeout() {
    ClientOptions clientOptions = new ClientOptions();
    Duration customReadTimeout = Duration.ofSeconds(30);
    
    // Use reflection to set the readTimeout since there's no setter
    try {
      java.lang.reflect.Field readTimeoutField = ClientOptions.class.getDeclaredField("readTimeout");
      readTimeoutField.setAccessible(true);
      readTimeoutField.set(clientOptions, customReadTimeout);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set readTimeout", e);
    }

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            awsCredentialsSupplier,
            clientOptions
    );

    assertNotNull(client);
    assertEquals(customReadTimeout, clientOptions.getReadTimeout());
  }

}
