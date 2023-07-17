package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;


class TranslateProcessorConfigTest {
    private TranslateProcessorConfig translateProcessorConfig;
    private TranslateProcessorConfig createObjectUnderTest() {
        return new TranslateProcessorConfig();
    }

    @BeforeEach
    void setup(){
        translateProcessorConfig = createObjectUnderTest();
    }

    @Test
    void test_no_mappings_filepath_options_present(){
        assertFalse(translateProcessorConfig.hasMappings());
    }

    @Test
    void test_only_mappings_option_present() throws NoSuchFieldException, IllegalAccessException{
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "mappingsParameterConfigs", List.of(new MappingsParameterConfig()));
        assertTrue(translateProcessorConfig.hasMappings());
    }

    @Test
    void test_only_filepath_option_present() throws NoSuchFieldException, IllegalAccessException{
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "filePath", "/path/to/file.yaml");
        assertTrue(translateProcessorConfig.hasMappings());
    }

    @Test
    void test_get_file_path()  throws NoSuchFieldException, IllegalAccessException{
        String filePath = "/path/to/file.yaml";
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "filePath", filePath);
        assertThat(translateProcessorConfig.getFilePath(), is(filePath));
    }

    @Test
    void test_get_mappings()  throws NoSuchFieldException, IllegalAccessException{
        List<MappingsParameterConfig> mappingsParameterConfigs = List.of(new MappingsParameterConfig());
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "mappingsParameterConfigs", mappingsParameterConfigs);
        assertThat(translateProcessorConfig.getMappingsParameterConfigs(), is(mappingsParameterConfigs));
    }

}