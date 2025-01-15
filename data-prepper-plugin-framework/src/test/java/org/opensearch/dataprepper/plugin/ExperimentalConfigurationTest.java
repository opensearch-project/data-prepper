/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class ExperimentalConfigurationTest {
    @Test
    void defaultConfiguration_should_return_config_with_isEnableAll_false() {
        final ExperimentalConfiguration objectUnderTest = ExperimentalConfiguration.defaultConfiguration();
        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.isEnableAll(), equalTo(false));
    }
}