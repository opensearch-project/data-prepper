/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AwsPluginConfigTest {

    @Test
    void testDefault() {
        final AwsPluginConfig objectUnderTest = new AwsPluginConfig();

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getDefaultStsConfiguration(), notNullValue());
        assertThat(objectUnderTest.getDefaultStsConfiguration().getAwsRegion(), nullValue());
        assertThat(objectUnderTest.getDefaultStsConfiguration().getAwsStsRoleArn(), nullValue());
    }
}
