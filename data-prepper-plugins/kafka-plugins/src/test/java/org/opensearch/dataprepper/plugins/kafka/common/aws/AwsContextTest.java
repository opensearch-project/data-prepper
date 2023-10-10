package org.opensearch.dataprepper.plugins.kafka.common.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsContextTest {
    @Mock
    private KafkaConnectionConfig connectionConfig;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private AwsContext createObjectUnderTest() {
        return new AwsContext(connectionConfig, awsCredentialsSupplier);
    }

    @Test
    void get_uses_defaultOptions_when_awsConfig_is_null() {
        AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(AwsCredentialsOptions.defaultOptions()))
                .thenReturn(awsCredentialsProvider);

        assertThat(createObjectUnderTest().get(), equalTo(awsCredentialsProvider));
    }

    @Nested
    class WithAwsConfig {

        @Mock
        private AwsConfig awsConfig;
        @Mock
        private AwsCredentialsOptions awsCredentialsOptions;

        @BeforeEach
        void setUp() {
            when(connectionConfig.getAwsConfig()).thenReturn(awsConfig);
        }

        @Test
        void get_uses_uses_awsConfig_to() {
            when(awsConfig.toCredentialsOptions()).thenReturn(awsCredentialsOptions);
            AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
            when(awsCredentialsSupplier.getProvider(awsCredentialsOptions))
                    .thenReturn(awsCredentialsProvider);

            assertThat(createObjectUnderTest().get(), equalTo(awsCredentialsProvider));
        }

        @Test
        void getRegion_returns_null_if_AwsConfig_region_is_null() {
            assertThat(createObjectUnderTest().getRegion(), nullValue());
        }

        @ParameterizedTest
        @ValueSource(strings = {"us-east-2", "eu-west-3", "ap-northeast-1"})
        void getRegion_returns_Region_of(String regionString) {
            when(awsConfig.getRegion()).thenReturn(regionString);

            Region region = Region.of(regionString);
            assertThat(createObjectUnderTest().getRegion(), equalTo(region));
        }
    }

    @Test
    void getRegion_returns_null_if_no_AwsConfig() {
        assertThat(createObjectUnderTest().getRegion(), nullValue());
    }
}