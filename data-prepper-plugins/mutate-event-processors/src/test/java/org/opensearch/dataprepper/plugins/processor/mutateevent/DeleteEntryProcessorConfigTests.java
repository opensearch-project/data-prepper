package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

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
    
    @Test
    void testisExcludeFromDeleteValid_with_valid_config() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();
        final List<String> regexKeys = List.of("test.*");
        final Set<EventKey> excludeKeys = Set.of(mock(EventKey.class));
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", regexKeys);
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", excludeKeys);

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void test_with_keys_valid_config() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();
        final List<String> regexKeys = List.of("test.*");
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeys", regexKeys);

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_invalid_config() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();
        final List<EventKey> testKeys = List.of(mock(EventKey.class));
        final Set<EventKey> excludeKeys = Set.of(mock(EventKey.class));
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeys", testKeys);
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", excludeKeys);

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(false));
    }
}
