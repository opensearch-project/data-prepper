/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class TransformersFactoryTest {

    private final String templatesDirectoryPath = "src/test/resources/transformation/templates";
    private final String rulesDirectoryPath = "src/test/resources/transformation/rules";
    private final String validPluginName = "testPlugin";
    private final String invalidPluginName = "";
    private TransformersFactory transformersFactory;

    @BeforeEach
    public void setUp() {
        transformersFactory = spy(new TransformersFactory(rulesDirectoryPath, templatesDirectoryPath));
    }

    @Test
    public void testGetPluginTemplateFileLocation_validPluginName() {
        String expectedPath = templatesDirectoryPath + "/" + validPluginName + "-template.yaml";
        assertEquals(expectedPath, transformersFactory.getPluginTemplateFileLocation(validPluginName));
    }

    @Test
    public void testGetPluginTemplateFileLocation_invalidPluginName() {
        Exception exception = assertThrows(RuntimeException.class, () -> {
            transformersFactory.getPluginTemplateFileLocation(invalidPluginName);
        });
        assertEquals("Transformation plugin not found", exception.getMessage());
    }

    @Test
    public void testGetTemplateModel_throwsRuntimeExceptionOnIOException() throws IOException {
        ObjectMapper mockedYamlMapper = Mockito.mock(ObjectMapper.class);
        String templatePath = templatesDirectoryPath + "/" + validPluginName + "-template.yaml";
        File expectedFile = new File(templatePath);

        Mockito.when(mockedYamlMapper.readValue(Mockito.eq(expectedFile), Mockito.eq(PipelineTemplateModel.class)))
                .thenThrow(new IOException("Test exception"));

        assertThrows(RuntimeException.class, () -> transformersFactory.getTemplateModel(validPluginName));
    }

    @Test
    public void testGetTemplateModel_invalidPluginNameThrowsRuntimeException() {
        assertThrows(RuntimeException.class, () -> transformersFactory.getTemplateModel(invalidPluginName),
                "Should throw a RuntimeException for empty plugin name.");
    }

    @Test
    public void testReadFile() throws IOException {
        // Mocking the getRuleFiles method
        List<Path> mockRuleFiles = Arrays.asList(
                Paths.get("src/test/resources/transformation/rules/documentdb-rule1.yaml"),
                Paths.get("src/test/resources/transformation/rules/documentdb-rule.yaml")
        );
        doReturn(mockRuleFiles).when(transformersFactory).getRuleFiles();

        List<Path> ruleFiles = transformersFactory.getRuleFiles();
        assertEquals(ruleFiles.size(), 2);
        Path firstRuleFile = ruleFiles.get(0);
        Path secondRuleFile = ruleFiles.get(1);

        assertEquals(firstRuleFile.getFileName().toString(), "documentdb-rule1.yaml");
    }

}

