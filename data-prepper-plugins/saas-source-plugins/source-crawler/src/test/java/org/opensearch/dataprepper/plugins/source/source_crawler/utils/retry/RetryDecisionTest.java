/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

class RetryDecisionTest {

    @Test
    void retry_ReturnsRetryDecision() {
        final RetryDecision decision = RetryDecision.retry();

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(false));
        assertThat(decision.getException(), equalTo(Optional.empty()));
    }

    @Test
    void stop_ReturnsStopDecision() {
        final RetryDecision decision = RetryDecision.stop();

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), equalTo(Optional.empty()));
    }

    @Test
    void stopWithException_ReturnsStopDecisionWithException() {
        final RuntimeException exception = new RuntimeException("Test exception");

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException().get(), equalTo(exception));
        assertThat(decision.getException().get().getMessage(), equalTo("Test exception"));
    }

    @Test
    void stopWithException_WithSecurityException_PreservesExceptionType() {
        final SecurityException exception = new SecurityException("Access denied");

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException().get(), equalTo(exception));
        assertThat(decision.getException().get().getMessage(), equalTo("Access denied"));
    }

    @Test
    void stopWithException_WithNullException_AcceptsNull() {
        final RetryDecision decision = RetryDecision.stopWithException(null);

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), equalTo(Optional.ofNullable(null)));
    }

    @Test
    void retry_CreatesIndependentInstances() {
        final RetryDecision decision1 = RetryDecision.retry();
        final RetryDecision decision2 = RetryDecision.retry();

        assertThat(decision1, notNullValue());
        assertThat(decision2, notNullValue());
        assertThat(decision1 == decision2, equalTo(false));
    }

    @Test
    void stop_CreatesIndependentInstances() {
        final RetryDecision decision1 = RetryDecision.stop();
        final RetryDecision decision2 = RetryDecision.stop();

        assertThat(decision1, notNullValue());
        assertThat(decision2, notNullValue());
        assertThat(decision1 == decision2, equalTo(false));
    }

    @Test
    void stopWithException_WithDifferentExceptions_MaintainsDistinctState() {
        final RuntimeException exception1 = new RuntimeException("Exception 1");
        final RuntimeException exception2 = new RuntimeException("Exception 2");

        final RetryDecision decision1 = RetryDecision.stopWithException(exception1);
        final RetryDecision decision2 = RetryDecision.stopWithException(exception2);

        assertThat(decision1.getException().get(), equalTo(exception1));
        assertThat(decision2.getException().get(), equalTo(exception2));
        assertThat(decision1.getException().get() == decision2.getException().get(), equalTo(false));
    }

    @Test
    void getShouldStop_ReturnsCorrectValue() {
        final RetryDecision retryDecision = RetryDecision.retry();
        final RetryDecision stopDecision = RetryDecision.stop();

        assertThat(retryDecision.isShouldStop(), equalTo(false));
        assertThat(stopDecision.isShouldStop(), equalTo(true));
    }

    @Test
    void getException_ReturnsCorrectValue() {
        final RuntimeException exception = new RuntimeException("Test");
        final RetryDecision decisionWithException = RetryDecision.stopWithException(exception);
        final RetryDecision decisionWithoutException = RetryDecision.stop();

        assertThat(decisionWithException.getException().get(), equalTo(exception));
        assertThat(decisionWithoutException.getException(), equalTo(Optional.empty()));
    }

    @Test
    void stopWithException_WithIllegalArgumentException_PreservesExceptionType() {
        final IllegalArgumentException exception = new IllegalArgumentException(
                "Invalid argument");

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException().get(), equalTo(exception));
        assertThat(decision.getException().get().getMessage(), equalTo("Invalid argument"));
    }

    @Test
    void stopWithException_WithNestedCause_PreservesFullException() {
        final Exception cause = new Exception("Root cause");
        final RuntimeException exception = new RuntimeException("Wrapper exception", cause);

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException().get(), equalTo(exception));
        assertThat(decision.getException().get().getCause(), equalTo(cause));
        assertThat(decision.getException().get().getCause().getMessage(), equalTo("Root cause"));
    }
}
