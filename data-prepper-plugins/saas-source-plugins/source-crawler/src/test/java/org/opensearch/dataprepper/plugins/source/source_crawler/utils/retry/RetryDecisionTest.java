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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class RetryDecisionTest {

    @Test
    void retry_ReturnsRetryDecision() {
        final RetryDecision decision = RetryDecision.retry();

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(false));
        assertThat(decision.getException(), nullValue());
    }

    @Test
    void stop_ReturnsStopDecision() {
        final RetryDecision decision = RetryDecision.stop();

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), nullValue());
    }

    @Test
    void stopWithException_ReturnsStopDecisionWithException() {
        final RuntimeException exception = new RuntimeException("Test exception");

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), equalTo(exception));
        assertThat(decision.getException().getMessage(), equalTo("Test exception"));
    }

    @Test
    void stopWithException_WithSecurityException_PreservesExceptionType() {
        final SecurityException exception = new SecurityException("Access denied");

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), equalTo(exception));
        assertThat(decision.getException().getMessage(), equalTo("Access denied"));
    }

    @Test
    void stopWithException_WithNullException_AcceptsNull() {
        final RetryDecision decision = RetryDecision.stopWithException(null);

        assertThat(decision, notNullValue());
        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), nullValue());
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

        assertThat(decision1.getException(), equalTo(exception1));
        assertThat(decision2.getException(), equalTo(exception2));
        assertThat(decision1.getException() == decision2.getException(), equalTo(false));
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

        assertThat(decisionWithException.getException(), equalTo(exception));
        assertThat(decisionWithoutException.getException(), nullValue());
    }

    @Test
    void stopWithException_WithIllegalArgumentException_PreservesExceptionType() {
        final IllegalArgumentException exception = new IllegalArgumentException(
                "Invalid argument");

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), equalTo(exception));
        assertThat(decision.getException().getMessage(), equalTo("Invalid argument"));
    }

    @Test
    void stopWithException_WithNestedCause_PreservesFullException() {
        final Exception cause = new Exception("Root cause");
        final RuntimeException exception = new RuntimeException("Wrapper exception", cause);

        final RetryDecision decision = RetryDecision.stopWithException(exception);

        assertThat(decision.isShouldStop(), equalTo(true));
        assertThat(decision.getException(), equalTo(exception));
        assertThat(decision.getException().getCause(), equalTo(cause));
        assertThat(decision.getException().getCause().getMessage(), equalTo("Root cause"));
    }
}
