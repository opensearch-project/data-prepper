/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.commons.lang3.RandomStringUtils;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

import java.util.List;
import java.util.Random;

class TruncateProcessorConfigTests {
    TruncateProcessorConfig truncateProcessorConfig;

    Random random;

	@BeforeEach
	void setUp() {
        truncateProcessorConfig = new TruncateProcessorConfig();
        random = new Random();
    }

    @Test
    void testDefaults() {
        assertThat(truncateProcessorConfig.getEntries(), equalTo(null));
    }

    @Test
    void testEntryDefaults() {
        TruncateProcessorConfig.Entry entry = new TruncateProcessorConfig.Entry();
        assertThat(entry.getStartAt(), equalTo(null));
        assertThat(entry.getLength(), equalTo(null));
        assertThat(entry.getTruncateWhen(), equalTo(null));
    }

    @Test
    void testValidConfiguration_withStartAt() throws NoSuchFieldException, IllegalAccessException {
        TruncateProcessorConfig.Entry entry = new TruncateProcessorConfig.Entry();
        String source = RandomStringUtils.randomAlphabetic(10);
        List<String> sourceKeys = List.of(source);
        setField(TruncateProcessorConfig.Entry.class, entry, "sourceKeys", sourceKeys);
        int startAt = random.nextInt(100);
        setField(TruncateProcessorConfig.Entry.class, entry, "startAt", startAt);
        assertThat(entry.getSourceKeys(), equalTo(sourceKeys));
        assertThat(entry.getStartAt(), equalTo(startAt));
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "entries", List.of(entry));
        assertTrue(entry.isValidConfig());
    }

    @Test
    void testValidConfiguration_withLength() throws NoSuchFieldException, IllegalAccessException {
        TruncateProcessorConfig.Entry entry = new TruncateProcessorConfig.Entry();
        String source1 = RandomStringUtils.randomAlphabetic(10);
        String source2 = RandomStringUtils.randomAlphabetic(10);
        List<String> sourceKeys = List.of(source1, source2);
        setField(TruncateProcessorConfig.Entry.class, entry, "sourceKeys", sourceKeys);
        int length = random.nextInt(100);
        setField(TruncateProcessorConfig.Entry.class, entry, "length", length);
        assertThat(entry.getSourceKeys(), equalTo(sourceKeys));
        assertThat(entry.getLength(), equalTo(length));
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "entries", List.of(entry));
        assertTrue(entry.isValidConfig());
    }

    @Test
    void testValidConfiguration_withLength_withTruncateWhen() throws NoSuchFieldException, IllegalAccessException {
        TruncateProcessorConfig.Entry entry = new TruncateProcessorConfig.Entry();
        String source = RandomStringUtils.randomAlphabetic(10);
        String condition = RandomStringUtils.randomAlphabetic(10);
        List<String> sourceKeys = List.of(source);
        setField(TruncateProcessorConfig.Entry.class, entry, "sourceKeys", sourceKeys);
        int length = random.nextInt(100);
        int startAt = random.nextInt(100);
        setField(TruncateProcessorConfig.Entry.class, entry, "length", length);
        setField(TruncateProcessorConfig.Entry.class, entry, "startAt", startAt);
        setField(TruncateProcessorConfig.Entry.class, entry, "truncateWhen", condition);
        assertThat(entry.getSourceKeys(), equalTo(sourceKeys));
        assertThat(entry.getLength(), equalTo(length));
        assertThat(entry.getStartAt(), equalTo(startAt));
        assertThat(entry.getTruncateWhen(), equalTo(condition));
        assertTrue(entry.isValidConfig());
    }

    @Nested
    class InvalidConfigurationWithValidSourceKeys {
        private TruncateProcessorConfig.Entry entryUnderTest;
        private int validLength;
        private int validStartAt;

        @BeforeEach
        void setUp() throws NoSuchFieldException, IllegalAccessException {
            entryUnderTest = new TruncateProcessorConfig.Entry();
            String source = RandomStringUtils.randomAlphabetic(10);
            setField(TruncateProcessorConfig.Entry.class, entryUnderTest, "sourceKeys", List.of(source));

            validLength = random.nextInt(100) + 1;
            validStartAt = random.nextInt(100) + 1;
        }

        @Test
        void testInvalidConfiguration_StartAt_Length_BothNull() {
            assertFalse(entryUnderTest.isValidConfig());
        }

        @Test
        void testInvalidConfiguration_StartAt_Negative() throws NoSuchFieldException, IllegalAccessException {
            setField(TruncateProcessorConfig.Entry.class, entryUnderTest, "startAt", -validStartAt);
            assertFalse(entryUnderTest.isValidConfig());
        }

        @Test
        void testInvalidConfiguration_Length_Negative() throws NoSuchFieldException, IllegalAccessException {
            setField(TruncateProcessorConfig.Entry.class, entryUnderTest, "startAt", validStartAt);
            setField(TruncateProcessorConfig.Entry.class, entryUnderTest, "length", -validLength);
            assertFalse(entryUnderTest.isValidConfig());
        }
    }
}
