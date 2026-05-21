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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.variabletemplate.FileVariableTranslator.PREFIX;

@ExtendWith(MockitoExtension.class)
class FileVariableTranslatorTest {

    @TempDir
    Path tempDir;

    private FileVariableTranslator objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new FileVariableTranslator();
    }

    @Test
    void testGetPrefix_returnsFile() {
        assertThat(objectUnderTest.getPrefix(), equalTo(PREFIX));
    }

    @Test
    void testTranslate_stripsTrailingNewline() throws IOException {
        final Path file = tempDir.resolve("secret.txt");
        Files.writeString(file, "s3cr3t\n");
        assertThat(objectUnderTest.translate(file.toString()), equalTo("s3cr3t"));
    }

    @Test
    void testTranslate_stripsLeadingAndTrailingWhitespace() throws IOException {
        final Path file = tempDir.resolve("padded.txt");
        Files.writeString(file, "  value  ");
        assertThat(objectUnderTest.translate(file.toString()), equalTo("value"));
    }

    @Test
    void testTranslate_specialCharacters_returnedVerbatim() throws IOException {
        final Path file = tempDir.resolve("special.txt");
        final String value = "p@$$w0rd!#%^&*()-+=[]{}|;:,.<>?";
        Files.writeString(file, value);
        assertThat(objectUnderTest.translate(file.toString()), equalTo(value));
    }

    @Test
    void testTranslate_integerValue_returnedAsString() throws IOException {
        final Path file = tempDir.resolve("port.txt");
        Files.writeString(file, "9200");
        assertThat(objectUnderTest.translate(file.toString()), equalTo("9200"));
    }

    @Test
    void testTranslate_missingFile_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.translate(tempDir.resolve("ghost.txt").toString()));
    }

    @Test
    void testTranslate_unreadableFile_throwsIllegalArgumentException() throws IOException {
        final Path file = tempDir.resolve("locked.txt");
        Files.writeString(file, "value");
        assertThat(file.toFile().setReadable(false), is(true));
        assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.translate(file.toString()));
    }

    @Test
    void testTranslateToPluginConfigVariable_returnsImmutableVariable() throws IOException {
        final Path file = tempDir.resolve("token.txt");
        Files.writeString(file, "my-token");
        final PluginConfigVariable variable = objectUnderTest.translateToPluginConfigVariable(file.toString());
        assertThat(variable, instanceOf(ImmutablePluginConfigVariable.class));
        assertThat(variable.getValue(), equalTo("my-token"));
        assertThat(variable.isUpdatable(), equalTo(false));
    }

    @Test
    void testTranslateToPluginConfigVariable_setValue_throwsException() throws IOException {
        final Path file = tempDir.resolve("token2.txt");
        Files.writeString(file, "my-token");
        final PluginConfigVariable variable = objectUnderTest.translateToPluginConfigVariable(file.toString());
        assertThrows(Exception.class, () -> variable.setValue("new"));
    }

    @Test
    void testTranslateToPluginConfigVariable_refresh_isNoOp() throws IOException {
        final Path file = tempDir.resolve("token3.txt");
        Files.writeString(file, "my-token");
        final PluginConfigVariable variable = objectUnderTest.translateToPluginConfigVariable(file.toString());
        variable.refresh();
        assertThat(variable.getValue(), equalTo("my-token"));
    }
}
