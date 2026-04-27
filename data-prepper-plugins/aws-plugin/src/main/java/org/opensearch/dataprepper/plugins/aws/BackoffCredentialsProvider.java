/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * A credentials provider wrapper that applies exponential backoff when the
 * delegate provider fails to resolve credentials. During the backoff window,
 * this provider throws the cached exception without calling the delegate,
 * preventing excessive STS AssumeRole calls when a role is deleted or
 * misconfigured.
 *
 * <p>On successful credential resolution, the backoff state is reset.</p>
 */
class BackoffCredentialsProvider implements AwsCredentialsProvider {
    private static final Logger LOG = LoggerFactory.getLogger(BackoffCredentialsProvider.class);

    static final Duration INITIAL_BACKOFF = Duration.ofSeconds(10);
    static final Duration MAX_BACKOFF = Duration.ofMinutes(10);
    static final int BACKOFF_MULTIPLIER = 2;

    private final AwsCredentialsProvider delegate;
    private final Clock clock;

    private volatile SdkException lastException;
    private volatile Instant backoffUntil;
    private volatile int failureCount;

    BackoffCredentialsProvider(final AwsCredentialsProvider delegate) {
        this(delegate, Clock.systemUTC());
    }

    BackoffCredentialsProvider(final AwsCredentialsProvider delegate, final Clock clock) {
        this.delegate = delegate;
        this.clock = clock;
        this.backoffUntil = Instant.MIN;
        this.failureCount = 0;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        final Instant now = clock.instant();

        final SdkException cachedException = lastException;
        if (cachedException != null && now.isBefore(backoffUntil)) {
            LOG.debug("Credentials resolution in backoff period until {}. Throwing cached exception.", backoffUntil);
            throw cachedException;
        }

        try {
            final AwsCredentials credentials = delegate.resolveCredentials();
            resetBackoff();
            return credentials;
        } catch (final SdkException e) {
            applyBackoff(e);
            throw e;
        }
    }

    private synchronized void resetBackoff() {
        if (failureCount > 0) {
            LOG.info("Credentials resolution succeeded after {} failures. Resetting backoff.", failureCount);
        }
        failureCount = 0;
        lastException = null;
        backoffUntil = Instant.MIN;
    }

    private synchronized void applyBackoff(final SdkException exception) {
        failureCount++;
        lastException = exception;

        long backoffMillis = INITIAL_BACKOFF.toMillis() * (long) Math.pow(BACKOFF_MULTIPLIER, failureCount - 1);
        backoffMillis = Math.min(backoffMillis, MAX_BACKOFF.toMillis());

        backoffUntil = clock.instant().plus(Duration.ofMillis(backoffMillis));
        LOG.warn("Credentials resolution failed (attempt {}). Backing off for {}ms until {}.",
                failureCount, backoffMillis, backoffUntil, exception);
    }
}
