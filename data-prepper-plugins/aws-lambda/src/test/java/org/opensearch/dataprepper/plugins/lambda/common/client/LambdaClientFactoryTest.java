package org.opensearch.dataprepper.plugins.lambda.common.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.util.HashMap;

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
}
