/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flatten;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class FlattenProcessorConfigTest {
    @Test
    void testDefaultConfig() {
        final FlattenProcessorConfig FlattenProcessorConfig = new FlattenProcessorConfig();

        assertThat(FlattenProcessorConfig.getSource(), equalTo(null));
        assertThat(FlattenProcessorConfig.getTarget(), equalTo(null));
        assertThat(FlattenProcessorConfig.isRemoveListIndices(), equalTo(false));
        assertThat(FlattenProcessorConfig.isRemoveListIndices(), equalTo(false));
        assertThat(FlattenProcessorConfig.getFlattenWhen(), equalTo(null));
        assertThat(FlattenProcessorConfig.getTagsOnFailure(), equalTo(null));
    }
}
