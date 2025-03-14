/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class S3DataSelectionTest {
    @ParameterizedTest
    @EnumSource(S3DataSelection.class)
    void fromOptionValue(final S3DataSelection option) {
        assertThat(S3DataSelection.fromOptionValue(option.name().toLowerCase()), is(option));
    }

}

