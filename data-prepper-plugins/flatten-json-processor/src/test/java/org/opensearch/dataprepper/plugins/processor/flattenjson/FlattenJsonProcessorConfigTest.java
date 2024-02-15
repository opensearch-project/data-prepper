/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flattenjson;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class FlattenJsonProcessorConfigTest {
    @Test
    void testDefaultConfig() {
        final FlattenJsonProcessorConfig FlattenJsonProcessorConfig = new FlattenJsonProcessorConfig();

        assertThat(FlattenJsonProcessorConfig.getSource(), equalTo(null));
        assertThat(FlattenJsonProcessorConfig.getTarget(), equalTo(null));
        assertThat(FlattenJsonProcessorConfig.isRemoveListIndices(), equalTo(false));
        assertThat(FlattenJsonProcessorConfig.isRemoveListIndices(), equalTo(false));
        assertThat(FlattenJsonProcessorConfig.getFlattenWhen(), equalTo(null));
        assertThat(FlattenJsonProcessorConfig.getTagsOnFailure(), equalTo(null));
    }
}
