/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
