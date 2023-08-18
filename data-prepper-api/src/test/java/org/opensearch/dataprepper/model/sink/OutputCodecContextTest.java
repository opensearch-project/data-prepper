/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

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

    @Test
    void shouldIncludeKey_returns_expected_when_no_include_exclude() {
        OutputCodecContext objectUnderTest = new OutputCodecContext(null, null, null);
        assertThat(objectUnderTest.shouldIncludeKey(UUID.randomUUID().toString()), equalTo(true));
    }

    @Test
    void shouldIncludeKey_returns_expected_when_empty_lists_for_include_exclude() {
        OutputCodecContext objectUnderTest = new OutputCodecContext(null, Collections.emptyList(), Collections.emptyList());
        assertThat(objectUnderTest.shouldIncludeKey(UUID.randomUUID().toString()), equalTo(true));
    }

    @Test
    void shouldIncludeKey_returns_expected_when_includeKey() {
        String includeKey1 = UUID.randomUUID().toString();
        String includeKey2 = UUID.randomUUID().toString();
        final List<String> includeKeys = List.of(includeKey1, includeKey2);

        OutputCodecContext objectUnderTest = new OutputCodecContext(null, includeKeys, null);

        assertThat(objectUnderTest.shouldIncludeKey(includeKey1), equalTo(true));
        assertThat(objectUnderTest.shouldIncludeKey(includeKey2), equalTo(true));
        assertThat(objectUnderTest.shouldIncludeKey(UUID.randomUUID().toString()), equalTo(false));
    }

    @Test
    void shouldIncludeKey_returns_expected_when_excludeKey() {
        String excludeKey1 = UUID.randomUUID().toString();
        String excludeKey2 = UUID.randomUUID().toString();
        final List<String> excludeKeys = List.of(excludeKey1, excludeKey2);

        OutputCodecContext objectUnderTest = new OutputCodecContext(null, null, excludeKeys);

        assertThat(objectUnderTest.shouldIncludeKey(excludeKey1), equalTo(false));
        assertThat(objectUnderTest.shouldIncludeKey(excludeKey2), equalTo(false));
        assertThat(objectUnderTest.shouldIncludeKey(UUID.randomUUID().toString()), equalTo(true));
    }
}
