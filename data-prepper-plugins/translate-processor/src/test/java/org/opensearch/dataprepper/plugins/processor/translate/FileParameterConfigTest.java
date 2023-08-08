package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class FileParameterConfigTest {

    private FileParameterConfig fileParameterConfig;

    @BeforeEach
    void setup(){
        fileParameterConfig = new FileParameterConfig();
    }

    @Test
    void get_file_name() throws NoSuchFieldException, IllegalAccessException {
        String fileName = "/path/to/file.yaml";
        setField(FileParameterConfig.class, fileParameterConfig, "fileName", fileName);
        assertThat(fileParameterConfig.getFileName(), is(fileName));
    }

    @Test
    void get_aws_config() throws NoSuchFieldException, IllegalAccessException {
        S3ObjectConfig s3ObjectConfig = new S3ObjectConfig();
        setField(FileParameterConfig.class, fileParameterConfig, "awsConfig", s3ObjectConfig);
        assertThat(fileParameterConfig.getAwsConfig(), is(s3ObjectConfig));
    }

    @Test
    void test_get_file_mappings() throws IOException, NoSuchFieldException, IllegalAccessException {
        String fileContent = "mappings:\n" +
                             "  - source: status\n" +
                             "    targets:\n" +
                             "      - target: test\n" +
                             "        map:\n" +
                             "          120: success";
        File testMappingsFile = File.createTempFile("test", ".yaml");
        Files.write(testMappingsFile.toPath(), fileContent.getBytes());

        String filePath = testMappingsFile.getAbsolutePath();
        setField(FileParameterConfig.class, fileParameterConfig, "fileName", filePath);

        TargetsParameterConfig targetsParameterConfig = fileParameterConfig.getFileMappings().get(0).getTargetsParameterConfigs().get(0);
        assertEquals(Map.of("120", "success"), targetsParameterConfig.getMap());
    }

}