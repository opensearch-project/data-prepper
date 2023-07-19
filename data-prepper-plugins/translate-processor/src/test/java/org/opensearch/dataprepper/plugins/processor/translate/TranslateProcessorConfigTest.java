package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
    void test_no_mappings_present() throws NoSuchFieldException, IllegalAccessException {
        setField(TranslateProcessorConfig.class, translateProcessorConfig, "mappingsParameterConfigs", null);
        assertFalse(translateProcessorConfig.isMappingsValid());
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

    @Nested
    class FilePathTests{
        private File testMappingsFile;

        @BeforeEach
        void setup() throws IOException {
            testMappingsFile = File.createTempFile("test", ".yaml");
        }

        @AfterEach
        void cleanup() {
            testMappingsFile.delete();
        }

        @Test
        void test_is_file_valid_with_valid_file() throws NoSuchFieldException, IllegalAccessException, IOException {
            String fileContent = "mappings:\n" +
                                 "  - source: status\n" +
                                 "    targets:\n" +
                                 "      - target: test\n" +
                                 "        map:\n" +
                                 "          120: success";
            Files.write(testMappingsFile.toPath(), fileContent.getBytes());

            String filePath = testMappingsFile.getAbsolutePath();
            setField(TranslateProcessorConfig.class, translateProcessorConfig, "filePath", filePath);

            assertTrue(translateProcessorConfig.isFileValid());
        }

        @Test
        void test_is_file_valid_with_invalid_file() throws NoSuchFieldException, IllegalAccessException, IOException {
            String fileContent = "mappings:";
            Files.write(testMappingsFile.toPath(), fileContent.getBytes());

            String filePath = testMappingsFile.getAbsolutePath();
            setField(TranslateProcessorConfig.class, translateProcessorConfig, "filePath", filePath);

            assertFalse(translateProcessorConfig.isFileValid());
        }

        @Test
        void test_is_file_invalid_with_invalid_file_path() throws NoSuchFieldException, IllegalAccessException {
            String filePath = "/invalid/file/nofile.yaml";
            setField(TranslateProcessorConfig.class, translateProcessorConfig, "filePath", filePath);

            assertFalse(translateProcessorConfig.isFileValid());
        }
    }

}