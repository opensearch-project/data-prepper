/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class OutputFormatTest {

    @ParameterizedTest
    @EnumSource(OutputFormat.class)
    void fromOptionValue(final OutputFormat value) {
        assertThat(OutputFormat.fromOptionValue(value.name()), is(value));
        assertThat(value, instanceOf(OutputFormat.class));
    }

}
