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
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsCredentialsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
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
    void getOrDefault_uses_defaultOptions_when_awsConfig_is_null() {
        AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(AwsCredentialsOptions.defaultOptions()))
                .thenReturn(awsCredentialsProvider);

        assertThat(createObjectUnderTest().getOrDefault(null), equalTo(awsCredentialsProvider));
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
        void getOrDefault_uses_uses_awsConfig_to() {
            when(awsConfig.toCredentialsOptions()).thenReturn(awsCredentialsOptions);
            AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
            when(awsCredentialsSupplier.getProvider(awsCredentialsOptions))
                    .thenReturn(awsCredentialsProvider);

            assertThat(createObjectUnderTest().getOrDefault(null), equalTo(awsCredentialsProvider));
        }

        @Test
        void getRegionOrDefault_returns_null_if_AwsConfig_region_is_null() {
            assertThat(createObjectUnderTest().getRegionOrDefault(null), nullValue());
        }

        @ParameterizedTest
        @ValueSource(strings = {"us-east-2", "eu-west-3", "ap-northeast-1"})
        void getRegionOrDefault_returns_Region_of(String regionString) {
            when(awsConfig.getRegion()).thenReturn(regionString);

            Region region = Region.of(regionString);
            assertThat(createObjectUnderTest().getRegionOrDefault(null), equalTo(region));
        }
    }

    @Test
    void getRegionOrDefault_returns_null_if_no_AwsConfig() {
        assertThat(createObjectUnderTest().getRegionOrDefault(null), nullValue());
    }

    @Nested
    class WithPassedAwsCredentialsConfig {
        @Mock
        private AwsCredentialsConfig awsCredentialsConfig;

        @Test
        void getRegionOrDefault_returns_null_if_no_region_in_config_or_no_AwsConfig() {
            assertThat(createObjectUnderTest().getRegionOrDefault(awsCredentialsConfig), nullValue());
        }

        @ParameterizedTest
        @ValueSource(strings = {"us-east-2", "eu-west-3", "ap-northeast-1"})
        void getRegionOrDefault_returns_region_from_AwsCredentialsConfig_and_no_AwsConfig(String regionString) {
            AwsConfig awsConfig = mock(AwsConfig.class);
            when(connectionConfig.getAwsConfig()).thenReturn(awsConfig);
            when(awsCredentialsConfig.getRegion()).thenReturn(regionString);

            assertThat(createObjectUnderTest().getRegionOrDefault(awsCredentialsConfig),
                    equalTo(Region.of(regionString)));

            verifyNoInteractions(awsConfig);
        }

        @Test
        void getOrDefault_uses_defaultOptions_when_AwsCredentialsConfig_sts_role_ARN_is_null() {
            AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
            when(awsCredentialsSupplier.getProvider(AwsCredentialsOptions.defaultOptions()))
                    .thenReturn(awsCredentialsProvider);

            assertThat(createObjectUnderTest().getOrDefault(awsCredentialsConfig), equalTo(awsCredentialsProvider));
        }

        @Test
        void getOrDefault_uses_AwsCredentialsConfig_to_when_sts_role_ARN_is_present() {
            AwsCredentialsOptions awsCredentialsOptions = mock(AwsCredentialsOptions.class);
            when(awsCredentialsConfig.toCredentialsOptions()).thenReturn(awsCredentialsOptions);
            AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
            when(awsCredentialsConfig.getStsRoleArn()).thenReturn(UUID.randomUUID().toString());
            when(awsCredentialsSupplier.getProvider(awsCredentialsOptions))
                    .thenReturn(awsCredentialsProvider);

            assertThat(createObjectUnderTest().getOrDefault(awsCredentialsConfig), equalTo(awsCredentialsProvider));
        }
    }
}