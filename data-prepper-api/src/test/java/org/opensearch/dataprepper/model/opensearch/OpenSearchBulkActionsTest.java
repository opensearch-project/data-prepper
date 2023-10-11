/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.opensearch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class OpenSearchBulkActionsTest {
    @ParameterizedTest
    @EnumSource(OpenSearchBulkActions.class)
    void fromOptionValue(final OpenSearchBulkActions action) {
        assertThat(OpenSearchBulkActions.fromOptionValue(action.name()), is(action));
        assertThat(action, instanceOf(OpenSearchBulkActions.class));
    }
}
