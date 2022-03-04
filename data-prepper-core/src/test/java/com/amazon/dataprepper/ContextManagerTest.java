/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class ContextManagerTest {

    @Test
    public void testContextManagerGet() {
        final ContextManager contextManager = new ContextManager(
                TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE,
                TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE
        );
        final DataPrepper dataPrepper = contextManager.getDataPrepperBean();

        assertThat(dataPrepper, is(instanceOf(DataPrepper.class)));
    }
}