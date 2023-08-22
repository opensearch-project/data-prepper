/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.loadtype;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class LoadTypeOptionsTest {

    @Test
    void notNull_test() {
        assertNotNull(LoadTypeOptions.INMEMORY);
    }

    @Test
    void fromOptionValue_test() {
        LoadTypeOptions loadTypeOptions = LoadTypeOptions.fromOptionValue("memory_map");
        assertNotNull(loadTypeOptions);
        assertThat(loadTypeOptions.toString(), equalTo("INMEMORY"));
    }
}
