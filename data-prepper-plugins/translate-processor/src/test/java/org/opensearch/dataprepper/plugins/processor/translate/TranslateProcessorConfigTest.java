package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;


class TranslateProcessorConfigTest {
    private TranslateProcessorConfig translateProcessorConfig;
    private RegexParameterConfiguration regexParameterConfiguration;
    private TranslateProcessorConfig createObjectUnderTest() {
        return new TranslateProcessorConfig();
    }

    @BeforeEach
    void setup() throws NoSuchFieldException, IllegalAccessException{
        translateProcessorConfig = createObjectUnderTest();
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "source", "sourceKey");
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "target", "targetKey");
    }

    @Test
    void test_no_map_patterns_filepath_options_present(){
        assertFalse(translateProcessorConfig.hasMappings());
    }

    @Test
    void test_only_map_option_present() throws NoSuchFieldException, IllegalAccessException{
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "map", Collections.singletonMap("key1", "val1"));
        assertTrue(translateProcessorConfig.hasMappings());
    }

    @Test
    void test_only_filepath_option_present() throws NoSuchFieldException, IllegalAccessException{
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "filePath", "/path/to/file.yaml");
        assertTrue(translateProcessorConfig.hasMappings());
    }

    @Test
    void test_only_patterns_option_present() throws NoSuchFieldException, IllegalAccessException{
        regexParameterConfiguration = new RegexParameterConfiguration();
        setField(RegexParameterConfiguration.class, regexParameterConfiguration, "patterns", Collections.singletonMap("patternKey1", "patternVal1"));
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "regexParameterConfiguration", regexParameterConfiguration);
        assertTrue(translateProcessorConfig.hasMappings());
    }

    @Test
    void test_no_patterns_under_regex() throws NoSuchFieldException, IllegalAccessException{
        regexParameterConfiguration = new RegexParameterConfiguration();
        setField(RegexParameterConfiguration.class, regexParameterConfiguration, "exact", true);
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "map", Collections.singletonMap("key1", "val1"));
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "regexParameterConfiguration", regexParameterConfiguration);
        assertFalse(translateProcessorConfig.hasMappings());
    }
}