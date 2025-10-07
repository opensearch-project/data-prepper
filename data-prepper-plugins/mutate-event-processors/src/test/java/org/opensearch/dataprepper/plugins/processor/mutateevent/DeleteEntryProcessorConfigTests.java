package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class DeleteEntryProcessorConfigTests {

    @Test
    void testIsValidKeysRegexPatterns_with_valid_pattern() throws NoSuchFieldException, IllegalAccessException {
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();
        final List<String> validPatterns = List.of("test.*");
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", validPatterns);

        assertThat(objectUnderTest.isValidWithKeysRegexPattern(), equalTo(true));
    }

    @Test
    void testIsValidKeysRegexPatterns_with_invalid_pattern() throws NoSuchFieldException, IllegalAccessException {
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();
        final List<String> invalidPatterns = List.of("(abc");
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", invalidPatterns);

        assertThat(objectUnderTest.isValidWithKeysRegexPattern(), equalTo(false));
    }
}
