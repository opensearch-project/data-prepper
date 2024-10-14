/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.DataPrepper;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class ContextManagerTest {

    @Test
    void testContextManagerGet() {
        final ContextManager contextManager = new ContextManager(
                "src/test/resources/single_pipeline_valid_empty_source_plugin_settings.yml",
                "src/test/resources/valid_data_prepper_config.yml"
        );
        final DataPrepper dataPrepper = contextManager.getDataPrepperBean();

        assertThat(dataPrepper, is(instanceOf(DataPrepper.class)));
    }
}