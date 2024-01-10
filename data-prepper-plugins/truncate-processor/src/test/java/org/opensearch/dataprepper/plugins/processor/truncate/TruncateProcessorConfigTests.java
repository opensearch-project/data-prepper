/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

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
        assertThat(truncateProcessorConfig.getSourceKeys(), equalTo(null));
        assertThat(truncateProcessorConfig.getStartAt(), equalTo(null));
        assertThat(truncateProcessorConfig.getLength(), equalTo(null));
        assertThat(truncateProcessorConfig.getTruncateWhen(), equalTo(null));
    }
    
    @Test
    void testValidConfiguration_withStartAt() throws NoSuchFieldException, IllegalAccessException {
        String source = RandomStringUtils.randomAlphabetic(10);
        List<String> sourceKeys = List.of(source);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "sourceKeys", sourceKeys);
        int startAt = random.nextInt(100);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "startAt", startAt);
        assertThat(truncateProcessorConfig.getSourceKeys(), equalTo(sourceKeys));
        assertThat(truncateProcessorConfig.getStartAt(), equalTo(startAt));
        assertTrue(truncateProcessorConfig.isValidConfig());
    }
    
    @Test
    void testValidConfiguration_withLength() throws NoSuchFieldException, IllegalAccessException {
        String source1 = RandomStringUtils.randomAlphabetic(10);
        String source2 = RandomStringUtils.randomAlphabetic(10);
        List<String> sourceKeys = List.of(source1, source2);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "sourceKeys", sourceKeys);
        int length = random.nextInt(100);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "length", length);
        assertThat(truncateProcessorConfig.getSourceKeys(), equalTo(sourceKeys));
        assertThat(truncateProcessorConfig.getLength(), equalTo(length));
        assertTrue(truncateProcessorConfig.isValidConfig());
    }

    @Test
    void testValidConfiguration_withLength_withTruncateWhen() throws NoSuchFieldException, IllegalAccessException {
        String source = RandomStringUtils.randomAlphabetic(10);
        String condition = RandomStringUtils.randomAlphabetic(10);
        List<String> sourceKeys = List.of(source);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "sourceKeys", sourceKeys);
        int length = random.nextInt(100);
        int startAt = random.nextInt(100);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "length", length);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "startAt", startAt);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "truncateWhen", condition);
        assertThat(truncateProcessorConfig.getSourceKeys(), equalTo(sourceKeys));
        assertThat(truncateProcessorConfig.getLength(), equalTo(length));
        assertThat(truncateProcessorConfig.getStartAt(), equalTo(startAt));
        assertThat(truncateProcessorConfig.getTruncateWhen(), equalTo(condition));
        assertTrue(truncateProcessorConfig.isValidConfig());
    }

    @Test
    void testInvalidConfiguration_StartAt_Length_BothNull() throws NoSuchFieldException, IllegalAccessException { 
        String source = RandomStringUtils.randomAlphabetic(10);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "sourceKeys", List.of(source));
        assertFalse(truncateProcessorConfig.isValidConfig());
    }

    @Test
    void testInvalidConfiguration_StartAt_Length_Negative() throws NoSuchFieldException, IllegalAccessException { 
        String source = RandomStringUtils.randomAlphabetic(10);
        int length = random.nextInt(100);
        int startAt = random.nextInt(100);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "sourceKeys", List.of(source));
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "startAt", -startAt);
        assertFalse(truncateProcessorConfig.isValidConfig());
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "startAt", startAt);
        setField(TruncateProcessorConfig.class, truncateProcessorConfig, "length", -length);
        assertFalse(truncateProcessorConfig.isValidConfig());
    }
}
