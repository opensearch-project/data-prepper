/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.ErrorCause;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ErrorCauseStringCreatorTest {
    @Test
    void toSingleLineDisplayString_returns_empty_string_with_null_ErrorCause() {
        assertThat(ErrorCauseStringCreator.toSingleLineDisplayString(null),
                equalTo(""));
    }

    @Test
    void toSingleLineDisplayString_returns_empty_when_reason_is_null() {
        ErrorCause errorCause = mock(ErrorCause.class);

        assertThat(ErrorCauseStringCreator.toSingleLineDisplayString(errorCause),
                equalTo("unknown"));
    }

    @Test
    void toSingleLineDisplayString_returns_string_with_reason_when_no_nested_cause() {
        String reason = UUID.randomUUID().toString();
        ErrorCause errorCause = mock(ErrorCause.class);
        when(errorCause.reason()).thenReturn(reason);

        assertThat(ErrorCauseStringCreator.toSingleLineDisplayString(errorCause),
                equalTo(reason));
    }

    @Test
    void toSingleLineDisplayString_returns_string_of_reasons_with_a_single_nested_cause() {
        String innerReason = UUID.randomUUID().toString();
        ErrorCause innerErrorCause = mock(ErrorCause.class);
        when(innerErrorCause.reason()).thenReturn(innerReason);

        String outerReason = UUID.randomUUID().toString();
        ErrorCause outerErrorCause = mock(ErrorCause.class);
        when(outerErrorCause.reason()).thenReturn(outerReason);
        when(outerErrorCause.causedBy()).thenReturn(innerErrorCause);

        assertThat(ErrorCauseStringCreator.toSingleLineDisplayString(outerErrorCause),
                equalTo(outerReason + " caused by " + innerReason));
    }

    @Test
    void toSingleLineDisplayString_returns_string_of_reasons_with_multiple_nested_causes() {
        String innerReason = UUID.randomUUID().toString();
        ErrorCause innerErrorCause = mock(ErrorCause.class);
        when(innerErrorCause.reason()).thenReturn(innerReason);

        String middleReason = UUID.randomUUID().toString();
        ErrorCause middleErrorCause = mock(ErrorCause.class);
        when(middleErrorCause.reason()).thenReturn(middleReason);
        when(middleErrorCause.causedBy()).thenReturn(innerErrorCause);

        String outerReason = UUID.randomUUID().toString();
        ErrorCause outerErrorCause = mock(ErrorCause.class);
        when(outerErrorCause.reason()).thenReturn(outerReason);
        when(outerErrorCause.causedBy()).thenReturn(middleErrorCause);

        assertThat(ErrorCauseStringCreator.toSingleLineDisplayString(outerErrorCause),
                equalTo(outerReason + " caused by " + middleReason + " caused by " + innerReason));
    }
}