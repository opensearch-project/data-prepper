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
import org.opensearch.dataprepper.plugins.lambda.common.util.CustomLambdaRetryCondition;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

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
  }

  @Test
  void testCustomRetryConditionWorks_withSpyOrRetryCondition() {
    // Arrange
    CustomLambdaRetryCondition customRetryCondition = new CustomLambdaRetryCondition();
    RetryCondition spyRetryCondition = spy(customRetryCondition);

    LambdaAsyncClient lambdaClient = LambdaAsyncClient.builder()
            .httpClient(NettyNioAsyncHttpClient.builder().build())
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder()
                            // Even though we set numRetries=3,
                            // the SDK may only call our custom condition once
                            .numRetries(3)
                            .retryCondition(spyRetryCondition)
                            .build())
                    .build())
            .region(Region.US_EAST_1)
            .build();

    // Simulate a retryable exception
    InvokeRequest request = InvokeRequest.builder()
            .functionName("test-function")
            .build();

    // Act
    try {
      CompletableFuture<InvokeResponse> futureResponse = lambdaClient.invoke(request);
      futureResponse.join(); // Force completion
    } catch (Exception e) {
    }

    // Assert
    // The AWS SDK's internal 'OrRetryCondition' may only call our condition once
    verify(spyRetryCondition, atLeastOnce())
            .shouldRetry(any(RetryPolicyContext.class));
  }

}
