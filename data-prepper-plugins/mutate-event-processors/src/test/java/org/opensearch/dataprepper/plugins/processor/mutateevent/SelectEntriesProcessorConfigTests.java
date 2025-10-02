package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SelectEntriesProcessorConfigTests {

    @Test
    void testIsValidKeysRegexPatterns_with_valid_pattern() throws NoSuchFieldException, IllegalAccessException {
        final SelectEntriesProcessorConfig objectUnderTest = new SelectEntriesProcessorConfig();
        final List<String> validPatterns = List.of("test.*");
        ReflectivelySetField.setField(SelectEntriesProcessorConfig.class, objectUnderTest, "includeKeysRegex", validPatterns);

        assertThat(objectUnderTest.isValidIncludeKeysRegex(), equalTo(true));

    }

    @Test
    void testIsValidKeysRegexPatterns_with_invalid_pattern() throws NoSuchFieldException, IllegalAccessException {
        final SelectEntriesProcessorConfig objectUnderTest = new SelectEntriesProcessorConfig();
        final List<String> validPatterns = List.of("(abc");
        ReflectivelySetField.setField(SelectEntriesProcessorConfig.class, objectUnderTest, "includeKeysRegex", validPatterns);

        assertThat(objectUnderTest.isValidIncludeKeysRegex(), equalTo(false));

    }
}
