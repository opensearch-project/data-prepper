/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.apache.commons.lang3.RandomStringUtils;



public class SinkContextTest {
    private SinkContext sinkContext;

    @Test
    public void testSinkContextBasic() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testRoutes = Collections.emptyList();
        sinkContext = new SinkContext(testTagsTargetKey, testRoutes);
        assertThat(sinkContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(sinkContext.getRoutes(), equalTo(testRoutes));
        
    }
    
}

