/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


public class SinkContextTest {
    private SinkContext sinkContext;

    @Test
    public void testSinkContextBasic() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testRoutes = Collections.emptyList();
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();
        sinkContext = new SinkContext(testTagsTargetKey, testRoutes, testIncludeKeys, testExcludeKeys);
        assertThat(sinkContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(sinkContext.getRoutes(), equalTo(testRoutes));
        assertThat(sinkContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(sinkContext.getExcludeKeys(), equalTo(testExcludeKeys));

    }

    @Test
    public void testSinkContextWithTagsOnly() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        sinkContext = new SinkContext(testTagsTargetKey);
        assertThat(sinkContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(sinkContext.getRoutes(), equalTo(null));
        assertThat(sinkContext.getIncludeKeys(), equalTo(null));
        assertThat(sinkContext.getExcludeKeys(), equalTo(null));

    }

}

