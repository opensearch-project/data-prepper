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
    void testisExcludeFromDeleteValid_with_nonEmptyWithKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeys", List.of("test1"));

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_nonEmptyWithKeys_and_nonEmptyExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeys", List.of("test1"));
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", Set.of(mock(EventKey.class)));

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(false));
    }

    @Test
    void testisExcludeFromDeleteValid_with_nonEmptyWithKeysRegex_and_nonEmptyExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", List.of("test.*"));
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", Set.of(mock(EventKey.class)));

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_nullWithKeysRegex_and_nonEmptyExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", null);
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", Set.of(mock(EventKey.class)));

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(false));
    }

    @Test
    void testisExcludeFromDeleteValid_with_EmptyWithKeysRegex_and_nonEmptyExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", List.of());
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", Set.of(mock(EventKey.class)));

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(false));
    }

    @Test
    void testisExcludeFromDeleteValid_with_nullWithKeysRegex_and_emptyExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", null);
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", Set.of());

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_emptyWithKeysRegex_and_emptyExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", List.of());
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", Set.of());

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_nonEmptyWithKeysRegex_and_emptyExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", List.of("^tes.*"));
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", Set.of());

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_nullWithKeysRegex_and_nullExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", null);
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", null);

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_emptyWithKeysRegex_and_nullExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", List.of());
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", null);

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testisExcludeFromDeleteValid_with_nonEmptyWithKeysRegex_and_nullExcludeKeys() throws NoSuchFieldException, IllegalAccessException{
        final DeleteEntryProcessorConfig objectUnderTest = new DeleteEntryProcessorConfig();

        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "withKeysRegex", List.of("^tes.*"));
        ReflectivelySetField.setField(DeleteEntryProcessorConfig.class, objectUnderTest, "excludeFromDelete", null);

        assertThat(objectUnderTest.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nonEmptyWithKeysRegexEntry_and_nonEmptyExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, List.of("test.*"), Set.of(mock(EventKey.class)), null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nonEmptyWithKeysEntry() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                List.of(mock(EventKey.class)), null, null, null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nonEmptyWithKeysEntry_and_nonEmptyExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                List.of(mock(EventKey.class)), null, Set.of(mock(EventKey.class)), null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(false));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_emptyWithKeysRegexEntry_and_nonEmptyExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, List.of(), Set.of(mock(EventKey.class)), null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(false));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nullWithKeysRegexEntry_and_nonEmptyExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, null, Set.of(mock(EventKey.class)), null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(false));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nonEmptyWithKeysRegexEntry_and_emptyExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, List.of("test.*"), Set.of(), null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_emptyWithKeysRegexEntry_and_emptyExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, List.of(), Set.of(), null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nullWithKeysRegexEntry_and_emptyExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, null, Set.of(), null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nonEmptyWithKeysRegexEntry_and_nullExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, List.of("test.*"), null, null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_emptyWithKeysRegexEntry_and_nullExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, List.of(), null, null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }

    @Test
    void testIsExcludeFromDeleteValid_with_nullWithKeysRegexEntry_and_nullExcludeFromDelete() {
        final DeleteEntryProcessorConfig.Entry withKeysRegexEntry = new DeleteEntryProcessorConfig.Entry(
                null, null, null, null, null, null);

        assertThat(withKeysRegexEntry.isExcludeFromDeleteValid(), equalTo(true));
    }
}
