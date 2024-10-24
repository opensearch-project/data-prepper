package org.opensearch.dataprepper.pipeline.parser.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleStream;

import java.io.InputStream;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransformersFactoryTest {

    private TransformersFactory transformersFactory;

    @BeforeEach
    void setUp() {
        transformersFactory = new TransformersFactory();
    }

    @Test
    void testGetPluginTemplateFileStream_whenTemplateExists_shouldReturnInputStream() throws Exception {
        String pluginName = "test-plugin";

        // Load the actual resource
        InputStream inputStream = transformersFactory.getPluginTemplateFileStream(pluginName);

        assertNotNull(inputStream);
        inputStream.close();
    }

    @Test
    void testGetPluginTemplateFileStream_whenTemplateDoesNotExist_shouldThrowException() {
        String pluginName = "non-existent-plugin";

        Exception exception = assertThrows(RuntimeException.class, () -> {
            transformersFactory.getPluginTemplateFileStream(pluginName);
        });

        assertEquals("Template file not found for plugin: " + pluginName, exception.getMessage());
    }

    @Test
    void testLoadRules_whenRulesExist_shouldReturnRuleStreams() throws Exception {
        Collection<RuleStream> ruleStreams = transformersFactory.loadRules();

        assertNotNull(ruleStreams);
        assertFalse(ruleStreams.isEmpty());

        for (RuleStream ruleStream : ruleStreams) {
            assertNotNull(ruleStream.getRuleStream());
            assertNotNull(ruleStream.getName());
        }
    }

    @Test
    void testLoadRules_whenFilesExist_shouldReturnRuleStreams() throws Exception {
        // Ensure the rules directory has at least one file
        Collection<RuleStream> ruleStreams = transformersFactory.loadRules();

        assertNotNull(ruleStreams);
        assertFalse(ruleStreams.isEmpty());

        for (RuleStream ruleStream : ruleStreams) {
            assertNotNull(ruleStream.getRuleStream());
            assertNotNull(ruleStream.getName());
            assertTrue(ruleStream.getName().endsWith("-rule.yaml"));
        }
    }

    @Test
    void testGetPluginTemplateFileStream_whenPluginNameIsNull_shouldThrowException() {
        Exception exception = assertThrows(RuntimeException.class, () -> {
            transformersFactory.getPluginTemplateFileStream(null);
        });

        assertEquals("Transformation plugin not found", exception.getMessage());
    }

    @Test
    void testGetPluginTemplateFileStream_whenPluginNameIsEmpty_shouldThrowException() {
        Exception exception = assertThrows(RuntimeException.class, () -> {
            transformersFactory.getPluginTemplateFileStream("");
        });

        assertEquals("Transformation plugin not found", exception.getMessage());
    }

}
