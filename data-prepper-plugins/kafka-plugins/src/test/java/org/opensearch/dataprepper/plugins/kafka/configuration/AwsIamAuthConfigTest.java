/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsIamAuthConfigTest {
    @ParameterizedTest
    @EnumSource(AwsIamAuthConfig.class)
    void fromTypeValue(final AwsIamAuthConfig type) {
        assertThat(AwsIamAuthConfig.fromOptionValue(type.name()), is(type));
    }
}
