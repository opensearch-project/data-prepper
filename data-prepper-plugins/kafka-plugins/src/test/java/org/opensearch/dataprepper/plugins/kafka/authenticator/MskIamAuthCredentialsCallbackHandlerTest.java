/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.authenticator;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

import javax.security.auth.callback.Callback;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MskIamAuthCredentialsCallbackHandlerTest {

    @Mock
    private AwsCredentialsProvider mskCredentialsProvider;

    private final MskIamAuthCredentialsCallbackHandler objectUnderTest = new MskIamAuthCredentialsCallbackHandler();

    @Test
    void handleResolvesSessionCredentialsAndConvertsToV1() throws Exception {
        final AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
                "accessKeyId", "secretAccessKey", "sessionToken");
        when(mskCredentialsProvider.resolveCredentials()).thenReturn(sessionCredentials);
        final AWSCredentialsCallback callback = new AWSCredentialsCallback();

        try (final MockedStatic<KafkaSecurityConfigurer> mockedConfigurer = mockStatic(KafkaSecurityConfigurer.class)) {
            mockedConfigurer.when(KafkaSecurityConfigurer::getMskCredentialsProvider).thenReturn(mskCredentialsProvider);
            objectUnderTest.handle(new Callback[]{callback});
        }

        assertThat(callback.isSuccessful(), is(true));
        final AWSCredentials credentials = callback.getAwsCredentials();
        assertThat(credentials, instanceOf(BasicSessionCredentials.class));
        assertThat(credentials.getAWSAccessKeyId(), is("accessKeyId"));
        assertThat(credentials.getAWSSecretKey(), is("secretAccessKey"));
        assertThat(((BasicSessionCredentials) credentials).getSessionToken(), is("sessionToken"));
    }

    @Test
    void handleResolvesNonSessionCredentialsAndConvertsToV1() throws Exception {
        final AwsBasicCredentials basicCredentials = AwsBasicCredentials.create("accessKeyId", "secretAccessKey");
        when(mskCredentialsProvider.resolveCredentials()).thenReturn(basicCredentials);
        final AWSCredentialsCallback callback = new AWSCredentialsCallback();

        try (final MockedStatic<KafkaSecurityConfigurer> mockedConfigurer = mockStatic(KafkaSecurityConfigurer.class)) {
            mockedConfigurer.when(KafkaSecurityConfigurer::getMskCredentialsProvider).thenReturn(mskCredentialsProvider);
            objectUnderTest.handle(new Callback[]{callback});
        }

        assertThat(callback.isSuccessful(), is(true));
        final AWSCredentials credentials = callback.getAwsCredentials();
        assertThat(credentials, instanceOf(BasicAWSCredentials.class));
        assertThat(credentials.getAWSAccessKeyId(), is("accessKeyId"));
        assertThat(credentials.getAWSSecretKey(), is("secretAccessKey"));
    }

    @Test
    void handleSetsLoadingExceptionWhenProviderThrows() throws Exception {
        final RuntimeException resolveException = new RuntimeException("Access denied");
        when(mskCredentialsProvider.resolveCredentials()).thenThrow(resolveException);
        final AWSCredentialsCallback callback = new AWSCredentialsCallback();

        try (final MockedStatic<KafkaSecurityConfigurer> mockedConfigurer = mockStatic(KafkaSecurityConfigurer.class)) {
            mockedConfigurer.when(KafkaSecurityConfigurer::getMskCredentialsProvider).thenReturn(mskCredentialsProvider);
            objectUnderTest.handle(new Callback[]{callback});
        }

        assertThat(callback.isSuccessful(), is(false));
        assertThat(callback.getLoadingException(), is(sameInstance(resolveException)));
    }

    @Test
    void handleSetsLoadingExceptionWhenProviderIsNull() throws Exception {
        final AWSCredentialsCallback callback = new AWSCredentialsCallback();

        try (final MockedStatic<KafkaSecurityConfigurer> mockedConfigurer = mockStatic(KafkaSecurityConfigurer.class)) {
            mockedConfigurer.when(KafkaSecurityConfigurer::getMskCredentialsProvider).thenReturn(null);
            objectUnderTest.handle(new Callback[]{callback});
        }

        assertThat(callback.isSuccessful(), is(false));
        assertThat(callback.getLoadingException(), instanceOf(NullPointerException.class));
    }
}
