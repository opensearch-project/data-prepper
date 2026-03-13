/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BackoffCredentialsProviderTest {

    private static final Instant BASE_TIME = Instant.parse("2025-01-01T00:00:00Z");

    @Mock
    private AwsCredentialsProvider delegate;

    @Mock
    private AwsCredentials credentials;

    @Mock
    private Clock clock;

    private BackoffCredentialsProvider createObjectUnderTest() {
        return new BackoffCredentialsProvider(delegate, clock);
    }

    @BeforeEach
    void setUp() {
        lenient().when(clock.instant()).thenReturn(BASE_TIME);
    }

    @Test
    void constructor_with_delegate_only_creates_provider() {
        final BackoffCredentialsProvider provider = new BackoffCredentialsProvider(delegate);
        when(delegate.resolveCredentials()).thenReturn(credentials);

        final AwsCredentials result = provider.resolveCredentials();

        assertThat(result, sameInstance(credentials));
    }

    @Test
    void resolveCredentials_delegates_on_success() {
        when(delegate.resolveCredentials()).thenReturn(credentials);

        final AwsCredentials result = createObjectUnderTest().resolveCredentials();

        assertThat(result, sameInstance(credentials));
        verify(delegate).resolveCredentials();
    }

    @Test
    void resolveCredentials_on_failure_throws_and_applies_backoff() {
        final SdkException exception = SdkClientException.create("access denied");
        when(delegate.resolveCredentials()).thenThrow(exception);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();

        final SdkException thrown = assertThrows(SdkException.class, objectUnderTest::resolveCredentials);
        assertThat(thrown, sameInstance(exception));
    }

    @Test
    void resolveCredentials_during_backoff_throws_cached_exception_without_calling_delegate() {
        final SdkException exception = SdkClientException.create("access denied");
        when(delegate.resolveCredentials()).thenThrow(exception);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();

        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);

        when(clock.instant()).thenReturn(BASE_TIME.plusSeconds(5));

        final SdkException thrown = assertThrows(SdkException.class, objectUnderTest::resolveCredentials);
        assertThat(thrown, sameInstance(exception));
        verify(delegate, times(1)).resolveCredentials();
    }

    @Test
    void resolveCredentials_after_backoff_expires_calls_delegate_again() {
        final SdkException exception = SdkClientException.create("access denied");
        when(delegate.resolveCredentials()).thenThrow(exception).thenReturn(credentials);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();

        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);

        when(clock.instant()).thenReturn(BASE_TIME.plusSeconds(11));

        final AwsCredentials result = objectUnderTest.resolveCredentials();
        assertThat(result, sameInstance(credentials));
        verify(delegate, times(2)).resolveCredentials();
    }

    @Test
    void resolveCredentials_success_after_failure_resets_backoff() {
        final SdkException exception = SdkClientException.create("access denied");
        when(delegate.resolveCredentials())
                .thenThrow(exception)
                .thenReturn(credentials)
                .thenThrow(exception);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();

        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);

        when(clock.instant()).thenReturn(BASE_TIME.plusSeconds(11));
        objectUnderTest.resolveCredentials();

        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);

        when(clock.instant()).thenReturn(BASE_TIME.plusSeconds(16));

        final SdkException thrown = assertThrows(SdkException.class, objectUnderTest::resolveCredentials);
        assertThat(thrown, sameInstance(exception));
        verify(delegate, times(3)).resolveCredentials();
    }

    @Test
    void resolveCredentials_exponential_backoff_progression() {
        final SdkException exception = SdkClientException.create("access denied");
        when(delegate.resolveCredentials()).thenThrow(exception);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();

        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);

        when(clock.instant()).thenReturn(BASE_TIME.plusSeconds(9));
        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);
        verify(delegate, times(1)).resolveCredentials();

        when(clock.instant()).thenReturn(BASE_TIME.plus(BackoffCredentialsProvider.INITIAL_BACKOFF).plusMillis(1));
        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);
        verify(delegate, times(2)).resolveCredentials();

        when(clock.instant()).thenReturn(BASE_TIME.plus(BackoffCredentialsProvider.INITIAL_BACKOFF).plusSeconds(21));
        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);
        verify(delegate, times(3)).resolveCredentials();

        when(clock.instant()).thenReturn(BASE_TIME.plus(BackoffCredentialsProvider.INITIAL_BACKOFF).plusSeconds(61));
        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);
        verify(delegate, times(4)).resolveCredentials();
    }

    @Test
    void resolveCredentials_backoff_caps_at_max() {
        final SdkException exception = SdkClientException.create("access denied");
        when(delegate.resolveCredentials()).thenThrow(exception);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();

        Instant currentTime = BASE_TIME;
        for (int i = 0; i < 10; i++) {
            long expectedBackoffMs = BackoffCredentialsProvider.INITIAL_BACKOFF.toMillis()
                    * (long) Math.pow(BackoffCredentialsProvider.BACKOFF_MULTIPLIER, i);
            expectedBackoffMs = Math.min(expectedBackoffMs, BackoffCredentialsProvider.MAX_BACKOFF.toMillis());

            when(clock.instant()).thenReturn(currentTime);
            assertThrows(SdkException.class, objectUnderTest::resolveCredentials);

            currentTime = currentTime.plus(Duration.ofMillis(expectedBackoffMs)).plusMillis(1);
        }

        verify(delegate, times(10)).resolveCredentials();
    }

    @Test
    void resolveCredentials_when_now_equals_backoffUntil_calls_delegate() {
        final SdkException exception = SdkClientException.create("access denied");
        when(delegate.resolveCredentials()).thenThrow(exception).thenReturn(credentials);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();

        assertThrows(SdkException.class, objectUnderTest::resolveCredentials);

        when(clock.instant()).thenReturn(BASE_TIME.plus(BackoffCredentialsProvider.INITIAL_BACKOFF));

        final AwsCredentials result = objectUnderTest.resolveCredentials();
        assertThat(result, sameInstance(credentials));
        verify(delegate, times(2)).resolveCredentials();
    }

    @Test
    void resolveCredentials_first_success_does_not_affect_state() {
        when(delegate.resolveCredentials()).thenReturn(credentials);

        final BackoffCredentialsProvider objectUnderTest = createObjectUnderTest();
        objectUnderTest.resolveCredentials();
        objectUnderTest.resolveCredentials();

        verify(delegate, times(2)).resolveCredentials();
    }

    @Test
    void resolveCredentials_constants_have_expected_values() {
        assertThat(BackoffCredentialsProvider.INITIAL_BACKOFF, equalTo(Duration.ofSeconds(10)));
        assertThat(BackoffCredentialsProvider.MAX_BACKOFF, equalTo(Duration.ofMinutes(10)));
        assertThat(BackoffCredentialsProvider.BACKOFF_MULTIPLIER, equalTo(2));
    }
}
