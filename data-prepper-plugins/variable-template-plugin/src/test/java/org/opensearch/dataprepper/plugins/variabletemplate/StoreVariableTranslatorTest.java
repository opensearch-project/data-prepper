/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.variabletemplate.StoreVariableTranslator.PREFIX;

@ExtendWith(MockitoExtension.class)
class StoreVariableTranslatorTest {

    @TempDir
    Path tempDir;

    private StoreVariableTranslator buildTranslator(final String content) throws IOException {
        final Path file = tempDir.resolve("store.env");
        Files.writeString(file, content);
        return new StoreVariableTranslator(List.of(file.toString()));
    }

    @Test
    void testGetPrefix_returnsStore() {
        final StoreVariableTranslator translator = new StoreVariableTranslator(Collections.emptyList());
        assertThat(translator.getPrefix(), equalTo(PREFIX));
    }

    @Test
    void testTranslate_simpleKeyValue() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("MY_KEY=my-value\n");
        assertThat(translator.translate("MY_KEY"), equalTo("my-value"));
    }

    @Test
    void testTranslate_integerValue() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("PORT=9200\n");
        assertThat(translator.translate("PORT"), equalTo("9200"));
    }

    @Test
    void testTranslate_specialCharactersInValue() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("SECRET=p@$$w0rd!%^&*()\n");
        assertThat(translator.translate("SECRET"), equalTo("p@$$w0rd!%^&*()"));
    }

    @Test
    void testTranslate_doubleQuotedValue() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY=\"quoted value\"\n");
        assertThat(translator.translate("KEY"), equalTo("quoted value"));
    }

    @Test
    void testTranslate_singleQuotedValue() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY='single quoted'\n");
        assertThat(translator.translate("KEY"), equalTo("single quoted"));
    }

    @Test
    void testTranslate_valueWithEqualsSign() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("JDBC_URL=jdbc:postgresql://host:5432/db?ssl=true\n");
        assertThat(translator.translate("JDBC_URL"), equalTo("jdbc:postgresql://host:5432/db?ssl=true"));
    }

    @Test
    void testTranslate_emptyValue() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("EMPTY=\n");
        assertThat(translator.translate("EMPTY"), equalTo(""));
    }

    @Test
    void testTranslate_commentsIgnored() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("# this is a comment\nKEY=value\n");
        assertThat(translator.translate("KEY"), equalTo("value"));
    }

    @Test
    void testTranslate_emptyLinesIgnored() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("\n\nKEY=value\n\n");
        assertThat(translator.translate("KEY"), equalTo("value"));
    }

    @Test
    void testTranslate_inlineComment_stripped() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY=value # inline comment\n");
        assertThat(translator.translate("KEY"), equalTo("value"));
    }

    @Test
    void testTranslate_missingKey_throwsIllegalArgumentException() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY=value\n");
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> translator.translate("MISSING"));
        assertThat(ex.getMessage().contains("MISSING"), equalTo(true));
    }

    @Test
    void testTranslate_multipleFiles_laterOverridesEarlier() throws IOException {
        final Path file1 = tempDir.resolve("base.env");
        final Path file2 = tempDir.resolve("override.env");
        Files.writeString(file1, "KEY=base-value\n");
        Files.writeString(file2, "KEY=override-value\n");
        final StoreVariableTranslator translator = new StoreVariableTranslator(
                List.of(file1.toString(), file2.toString()));
        assertThat(translator.translate("KEY"), equalTo("override-value"));
    }

    @Test
    void testTranslate_multipleFiles_uniqueKeysFromBoth() throws IOException {
        final Path file1 = tempDir.resolve("a.env");
        final Path file2 = tempDir.resolve("b.env");
        Files.writeString(file1, "KEY_A=value-a\n");
        Files.writeString(file2, "KEY_B=value-b\n");
        final StoreVariableTranslator translator = new StoreVariableTranslator(
                List.of(file1.toString(), file2.toString()));
        assertThat(translator.translate("KEY_A"), equalTo("value-a"));
        assertThat(translator.translate("KEY_B"), equalTo("value-b"));
    }

    @Test
    void testConstructor_missingSourceFile_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> new StoreVariableTranslator(List.of("/nonexistent/path/.env")));
    }

    @Test
    void testConstructor_missingEqualsSign_throwsIllegalArgumentException() throws IOException {
        assertThrows(IllegalArgumentException.class,
                () -> buildTranslator("INVALID_LINE_NO_EQUALS\n"));
    }

    @Test
    void testTranslate_inlineComment_atStartOfValue_stripped() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY=#startcomment\n");
        assertThat(translator.translate("KEY"), equalTo(""));
    }

    @Test
    void testConstructor_unreadableFile_throwsIllegalArgumentException() throws IOException {
        final Path file = tempDir.resolve("locked.env");
        Files.writeString(file, "KEY=value\n");
        assertThat(file.toFile().setReadable(false), is(true));
        assertThrows(IllegalArgumentException.class,
                () -> new StoreVariableTranslator(List.of(file.toString())));
    }

    @Test
    void testTranslateToPluginConfigVariable_returnsImmutableVariable() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY=val\n");
        final PluginConfigVariable variable = translator.translateToPluginConfigVariable("KEY");
        assertThat(variable, instanceOf(ImmutablePluginConfigVariable.class));
        assertThat(variable.getValue(), equalTo("val"));
        assertThat(variable.isUpdatable(), equalTo(false));
    }

    @Test
    void testTranslateToPluginConfigVariable_setValue_throwsException() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY=val\n");
        final PluginConfigVariable variable = translator.translateToPluginConfigVariable("KEY");
        assertThrows(Exception.class, () -> variable.setValue("new"));
    }

    @Test
    void testTranslateToPluginConfigVariable_refresh_isNoOp() throws IOException {
        final StoreVariableTranslator translator = buildTranslator("KEY=val\n");
        final PluginConfigVariable variable = translator.translateToPluginConfigVariable("KEY");
        variable.refresh();
        assertThat(variable.getValue(), equalTo("val"));
    }
}
