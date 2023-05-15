/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
public class DdbClientCustomRetryConditionTest {

    @Mock
    private RetryPolicyContext retryPolicyContext;

    @Mock
    private RetryCondition defaultRetryCondition;

    private DdbClientCustomRetryCondition createObjectUnderTest() {
        try (final MockedStatic<RetryCondition> retryConditionMockedStatic = mockStatic(RetryCondition.class)) {
            retryConditionMockedStatic.when(RetryCondition::defaultRetryCondition).thenReturn(defaultRetryCondition);
            return new DdbClientCustomRetryCondition();
        }
    }

    @Test
    void shouldRetry_with_ProvisionedThroughPutExceededException_returns_true() {
        final ProvisionedThroughputExceededException provisionedThroughputExceededException = mock(ProvisionedThroughputExceededException.class);

        given(retryPolicyContext.exception()).willReturn(provisionedThroughputExceededException);
        given(retryPolicyContext.retriesAttempted()).willReturn(10);

        final boolean result = createObjectUnderTest().shouldRetry(retryPolicyContext);

        assertThat(result, equalTo(true));
    }

    @Test
    void shouldRetry_with_other_exception_calls_defaultRetryCondition() {
        final SdkException exception = mock(SdkException.class);

        given(retryPolicyContext.exception()).willReturn(exception);

        final DdbClientCustomRetryCondition objectUnderTest = createObjectUnderTest();
        given(defaultRetryCondition.shouldRetry(retryPolicyContext)).willReturn(false);

        final boolean result = objectUnderTest.shouldRetry(retryPolicyContext);

        assertThat(result, equalTo(false));
    }
}
