package org.opensearch.dataprepper.plugins.sink.personalize;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.PersonalizeSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.personalizeevents.PersonalizeEventsClient;
import software.amazon.awssdk.services.personalizeevents.PersonalizeEventsClientBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClientFactoryTest {
    @Mock
    private PersonalizeSinkConfiguration personalizeSinkConfig;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() {
        when(personalizeSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    }

    @Test
    void createPersonalizeEventsClient_with_real_PersonalizeEventsClient() {
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Optional.of(Region.US_EAST_1));
        final PersonalizeEventsClient personalizeEventsClient = ClientFactory.createPersonalizeEventsClient(personalizeSinkConfig, awsCredentialsSupplier);

        assertThat(personalizeEventsClient, notNullValue());
    }

    @Test
    void createPersonalizeEventsClient_provides_correct_inputs_for_null_awsAuthenticationOptions() {
        when(personalizeSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        final AwsCredentialsProvider expectedCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(expectedCredentialsProvider);

        final PersonalizeEventsClientBuilder personalizeEventsClientBuilder = mock(PersonalizeEventsClientBuilder.class);
        when(personalizeEventsClientBuilder.region(any())).thenReturn(personalizeEventsClientBuilder);
        when(personalizeEventsClientBuilder.credentialsProvider(any())).thenReturn(personalizeEventsClientBuilder);
        when(personalizeEventsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(personalizeEventsClientBuilder);
        try(final MockedStatic<PersonalizeEventsClient> personalizeEventsClientMockedStatic = mockStatic(PersonalizeEventsClient.class)) {
            personalizeEventsClientMockedStatic.when(PersonalizeEventsClient::builder)
                    .thenReturn(personalizeEventsClientBuilder);
            ClientFactory.createPersonalizeEventsClient(personalizeSinkConfig, awsCredentialsSupplier);
        }

        final ArgumentCaptor<AwsCredentialsProvider> credentialsProviderArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(personalizeEventsClientBuilder).credentialsProvider(credentialsProviderArgumentCaptor.capture());

        final AwsCredentialsProvider actualCredentialsProvider = credentialsProviderArgumentCaptor.getValue();

        assertThat(actualCredentialsProvider, equalTo(expectedCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(optionsArgumentCaptor.capture());

        final AwsCredentialsOptions actualCredentialsOptions = optionsArgumentCaptor.getValue();
        assertThat(actualCredentialsOptions, is(notNullValue()));
        assertThat(actualCredentialsOptions.getRegion(), equalTo(null));
        assertThat(actualCredentialsOptions.getStsRoleArn(), equalTo(null));
        assertThat(actualCredentialsOptions.getStsExternalId(), equalTo(null));
        assertThat(actualCredentialsOptions.getStsHeaderOverrides(), equalTo(Collections.emptyMap()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void createPersonalizeEventsClient_provides_correct_inputs(final String regionString) {
        final Region region = Region.of(regionString);
        final String stsRoleArn = UUID.randomUUID().toString();
        final String externalId = UUID.randomUUID().toString();
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Optional.of(region));
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(externalId);
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);

        final AwsCredentialsProvider expectedCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(expectedCredentialsProvider);

        final PersonalizeEventsClientBuilder personalizeEventsClientBuilder = mock(PersonalizeEventsClientBuilder.class);
        when(personalizeEventsClientBuilder.region(region)).thenReturn(personalizeEventsClientBuilder);
        when(personalizeEventsClientBuilder.credentialsProvider(any())).thenReturn(personalizeEventsClientBuilder);
        when(personalizeEventsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(personalizeEventsClientBuilder);
        try(final MockedStatic<PersonalizeEventsClient> personalizeEventsClientMockedStatic = mockStatic(PersonalizeEventsClient.class)) {
            personalizeEventsClientMockedStatic.when(PersonalizeEventsClient::builder)
                    .thenReturn(personalizeEventsClientBuilder);
            ClientFactory.createPersonalizeEventsClient(personalizeSinkConfig, awsCredentialsSupplier);
        }

        final ArgumentCaptor<AwsCredentialsProvider> credentialsProviderArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(personalizeEventsClientBuilder).credentialsProvider(credentialsProviderArgumentCaptor.capture());

        final AwsCredentialsProvider actualCredentialsProvider = credentialsProviderArgumentCaptor.getValue();

        assertThat(actualCredentialsProvider, equalTo(expectedCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(optionsArgumentCaptor.capture());

        final AwsCredentialsOptions actualCredentialsOptions = optionsArgumentCaptor.getValue();
        assertThat(actualCredentialsOptions.getRegion(), equalTo(region));
        assertThat(actualCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
        assertThat(actualCredentialsOptions.getStsExternalId(), equalTo(externalId));
        assertThat(actualCredentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }
}