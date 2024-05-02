/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;

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
        final List<PluginModel> testResponseActions = Collections.emptyList();

        SinkContext sinkContext = new SinkContext(testTagsTargetKey, null, testIncludeKeys, testExcludeKeys, testResponseActions);

        OutputCodecContext codecContext = OutputCodecContext.fromSinkContext(sinkContext);
        assertThat(codecContext.getTagsTargetKey(), equalTo(testTagsTargetKey));
        assertThat(codecContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(codecContext.getExcludeKeys(), equalTo(testExcludeKeys));

        OutputCodecContext emptyContext = OutputCodecContext.fromSinkContext(null);
        assertNull(emptyContext.getTagsTargetKey());
        assertThat(emptyContext.getIncludeKeys(), equalTo(testIncludeKeys));
        assertThat(emptyContext.getExcludeKeys(), equalTo(testExcludeKeys));
    }

    @Nested
    class WithNullIncludeExclude {
        private OutputCodecContext createObjectUnderTest() {
            return new OutputCodecContext(null, null, null);
        }

        @Test
        void shouldIncludeKey_returns_true_when_no_include_exclude() {
            assertThat(createObjectUnderTest().shouldIncludeKey(UUID.randomUUID().toString()), equalTo(true));
        }

        @Test
        void shouldNotIncludeKey_returns_false_when_no_include_exclude() {
            assertThat(createObjectUnderTest().shouldNotIncludeKey(UUID.randomUUID().toString()), equalTo(false));
        }
    }

    @Nested
    class WithEmptyIncludeExclude {
        private OutputCodecContext createObjectUnderTest() {
            return new OutputCodecContext(null, Collections.emptyList(), Collections.emptyList());
        }

        @Test
        void shouldIncludeKey_returns_true_when_empty_lists_for_include_exclude() {
            assertThat(createObjectUnderTest().shouldIncludeKey(UUID.randomUUID().toString()), equalTo(true));
        }

        @Test
        void shouldNotIncludeKey_returns_false_when_empty_lists_for_include_exclude() {
            assertThat(createObjectUnderTest().shouldNotIncludeKey(UUID.randomUUID().toString()), equalTo(false));
        }
    }

    @Nested
    class WithIncludeKey {

        private String includeKey1;
        private String includeKey2;
        private List<String> includeKeys;

        @BeforeEach
        void setUp() {
            includeKey1 = UUID.randomUUID().toString();
            includeKey2 = UUID.randomUUID().toString();
            includeKeys = List.of(includeKey1, includeKey2);

        }

        private OutputCodecContext createObjectUnderTest() {
            return new OutputCodecContext(null, includeKeys, null);
        }

        @Test
        void shouldIncludeKey_returns_expected_when_includeKey() {
            OutputCodecContext objectUnderTest = createObjectUnderTest();

            assertThat(objectUnderTest.shouldIncludeKey(includeKey1), equalTo(true));
            assertThat(objectUnderTest.shouldIncludeKey(includeKey2), equalTo(true));
            assertThat(objectUnderTest.shouldIncludeKey(UUID.randomUUID().toString()), equalTo(false));
        }

        @Test
        void shouldNotIncludeKey_returns_expected_when_includeKey() {

            OutputCodecContext objectUnderTest = createObjectUnderTest();

            assertThat(objectUnderTest.shouldNotIncludeKey(includeKey1), equalTo(false));
            assertThat(objectUnderTest.shouldNotIncludeKey(includeKey2), equalTo(false));
            assertThat(objectUnderTest.shouldNotIncludeKey(UUID.randomUUID().toString()), equalTo(true));
        }
    }

    @Nested
    class WithExcludeKey {

        private String excludeKey1;
        private String excludeKey2;
        private List<String> excludeKeys;

        @BeforeEach
        void setUp() {
            excludeKey1 = UUID.randomUUID().toString();
            excludeKey2 = UUID.randomUUID().toString();
            excludeKeys = List.of(excludeKey1, excludeKey2);
        }

        private OutputCodecContext createObjectUnderTest() {
            return new OutputCodecContext(null, null, excludeKeys);
        }

        @Test
        void shouldIncludeKey_returns_expected_when_excludeKey() {
            OutputCodecContext objectUnderTest = createObjectUnderTest();

            assertThat(objectUnderTest.shouldIncludeKey(excludeKey1), equalTo(false));
            assertThat(objectUnderTest.shouldIncludeKey(excludeKey2), equalTo(false));
            assertThat(objectUnderTest.shouldIncludeKey(UUID.randomUUID().toString()), equalTo(true));
        }

        @Test
        void shouldNotIncludeKey_returns_expected_when_excludeKey() {
            OutputCodecContext objectUnderTest = createObjectUnderTest();

            assertThat(objectUnderTest.shouldNotIncludeKey(excludeKey1), equalTo(true));
            assertThat(objectUnderTest.shouldNotIncludeKey(excludeKey2), equalTo(true));
            assertThat(objectUnderTest.shouldNotIncludeKey(UUID.randomUUID().toString()), equalTo(false));
        }
    }
}
