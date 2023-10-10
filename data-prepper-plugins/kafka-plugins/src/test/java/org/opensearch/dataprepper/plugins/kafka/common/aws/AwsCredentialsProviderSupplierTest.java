package org.opensearch.dataprepper.plugins.kafka.common.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsCredentialsProviderSupplierTest {
    @Mock
    private KafkaConnectionConfig connectionConfig;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private AwsCredentialsProviderSupplier createObjectUnderTest() {
        return new AwsCredentialsProviderSupplier(connectionConfig, awsCredentialsSupplier);
    }

    @Test
    void get_uses_defaultOptions_when_awsConfig_is_null() {
        AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(AwsCredentialsOptions.defaultOptions()))
                .thenReturn(awsCredentialsProvider);

        assertThat(createObjectUnderTest().get(), equalTo(awsCredentialsProvider));
    }

    @Test
    void get_uses_uses_awsConfig_to() {
        AwsCredentialsOptions awsCredentialsOptions = mock(AwsCredentialsOptions.class);
        AwsConfig awsConfig = mock(AwsConfig.class);
        when(connectionConfig.getAwsConfig()).thenReturn(awsConfig);
        when(awsConfig.toCredentialsOptions()).thenReturn(awsCredentialsOptions);

        AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptions))
                .thenReturn(awsCredentialsProvider);

        assertThat(createObjectUnderTest().get(), equalTo(awsCredentialsProvider));
    }
}