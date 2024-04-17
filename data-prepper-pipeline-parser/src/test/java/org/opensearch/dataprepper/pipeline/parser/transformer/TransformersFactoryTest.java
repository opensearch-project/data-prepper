package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

public class TransformersFactoryTest {

    private TransformersFactory transformersFactory;
    private final String templatesDirectoryPath = "src/test/resources/templates";
    private final String rulesDirectoryPath = "src/test/resources/rules";
    private final String validPluginName = "testPlugin";
    private final String invalidPluginName = "";

    @BeforeEach
    public void setUp() {
        transformersFactory = new TransformersFactory(rulesDirectoryPath, templatesDirectoryPath);
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
    public void testGetPluginRuleFileLocation_validPluginName() {
        String expectedPath = rulesDirectoryPath + "/" + validPluginName + "-rule.yaml";
        assertEquals(expectedPath, transformersFactory.getPluginRuleFileLocation(validPluginName));
    }

    @Test
    public void testGetPluginRuleFileLocation_invalidPluginName() {
        Exception exception = assertThrows(RuntimeException.class, () -> {
            transformersFactory.getPluginRuleFileLocation(invalidPluginName);
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
}

