/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class BulkActionTest {

    @ParameterizedTest
    @EnumSource(BulkAction.class)
    void fromOptionValue(final BulkAction action) {
        assertThat(BulkAction.fromOptionValue(action.name()), is(action));
        assertThat(action, instanceOf(BulkAction.class));
    }

}
