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
import static org.junit.jupiter.api.Assertions.assertNull;

public class OutputCodecContextTest {


    @Test
    public void testOutputCodecContextBasic() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();
        OutputCodecContext codecContext = new OutputCodecContext(testTagsTargetKey, testIncludeKeys, testExcludeKeys);
        assertThat(codecContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(codecContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(codecContext.getExcludeKeys(), equalTo(testExcludeKeys));

        OutputCodecContext emptyContext = new OutputCodecContext();
        assertNull(emptyContext.getTagsTargetKey());
        assertThat(emptyContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(emptyContext.getExcludeKeys(), equalTo(testExcludeKeys));


    }

    @Test
    public void testOutputCodecContextAdapter() {
        final String testTagsTargetKey = RandomStringUtils.randomAlphabetic(6);
        final List<String> testIncludeKeys = Collections.emptyList();
        final List<String> testExcludeKeys = Collections.emptyList();

        SinkContext sinkContext = new SinkContext(testTagsTargetKey, null, testIncludeKeys, testExcludeKeys);

        OutputCodecContext codecContext = OutputCodecContext.fromSinkContext(sinkContext);
        assertThat(codecContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(codecContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(codecContext.getExcludeKeys(), equalTo(testExcludeKeys));

        OutputCodecContext emptyContext = OutputCodecContext.fromSinkContext(null);
        assertNull(emptyContext.getTagsTargetKey());
        assertThat(emptyContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(emptyContext.getExcludeKeys(), equalTo(testExcludeKeys));


    }
}
