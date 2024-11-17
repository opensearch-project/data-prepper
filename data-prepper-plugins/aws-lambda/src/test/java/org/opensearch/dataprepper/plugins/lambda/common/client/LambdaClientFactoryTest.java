package org.opensearch.dataprepper.plugins.lambda.common.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.time.Duration;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
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
    int maxConnectionRetries = 3;
    Duration sdkTimeout = Duration.ofSeconds(120);

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            maxConnectionRetries,
            awsCredentialsSupplier,
            sdkTimeout
    );

    assertNotNull(client);
    assertEquals(Region.US_WEST_2, client.serviceClientConfiguration().region());
  }

  @Test
  void testCreateAsyncLambdaClientWithDifferentRegion() {
    when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.EU_CENTRAL_1);

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            3,
            awsCredentialsSupplier,
            Duration.ofSeconds(60)
    );

    assertNotNull(client);
    assertEquals(Region.EU_CENTRAL_1, client.serviceClientConfiguration().region());
  }

  @Test
  void testCreateAsyncLambdaClientWithCustomSdkTimeout() {
    Duration customTimeout = Duration.ofMinutes(5);

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            3,
            awsCredentialsSupplier,
            customTimeout
    );

    assertNotNull(client);
    assertEquals(customTimeout, client.serviceClientConfiguration().overrideConfiguration().apiCallTimeout().get());
  }

  @Test
  void testCreateAsyncLambdaClientWithMaxRetries() {
    int maxRetries = 5;

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            maxRetries,
            awsCredentialsSupplier,
            Duration.ofSeconds(60)
    );

    assertNotNull(client);
  }

  @Test
  void testCreateAsyncLambdaClientOverrideConfiguration() {
    Duration sdkTimeout = Duration.ofSeconds(90);
    int maxRetries = 4;

    LambdaAsyncClient client = LambdaClientFactory.createAsyncLambdaClient(
            awsAuthenticationOptions,
            maxRetries,
            awsCredentialsSupplier,
            sdkTimeout
    );

    assertNotNull(client);
    ClientOverrideConfiguration overrideConfig = client.serviceClientConfiguration().overrideConfiguration();

    assertEquals(sdkTimeout, overrideConfig.apiCallTimeout().get());
    assertEquals(maxRetries, overrideConfig.retryPolicy().get().numRetries());
    assertNotNull(overrideConfig.metricPublishers());
    assertFalse(overrideConfig.metricPublishers().isEmpty());
  }
}