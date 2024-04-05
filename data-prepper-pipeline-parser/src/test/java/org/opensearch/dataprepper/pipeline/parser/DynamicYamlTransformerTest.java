package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

class DynamicYamlTransformerTest {

    @InjectMocks
    private DynamicYamlTransformer yamlTransformer;

    @BeforeEach
    void setUp() {
        // Initialize the yamlTransformer before each test
        yamlTransformer = new DynamicYamlTransformer();
    }
    @Test
    void testSuccessfulTransformation() throws IOException {
        String originalYaml = "dodb-pipeline:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"database.example.com\"\n";

        String templateYaml = "docDB-docdb:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"{{dodb-pipeline.source.documentdb.hostname}}\"\n";

        String expectedYaml = "docDB-pipeline:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"database.example.com\"\n";

        String transformedYaml = yamlTransformer.transformYaml(originalYaml, templateYaml);

        assertEquals(expectedYaml.trim(), transformedYaml.trim(), "The transformed YAML should match the expected YAML.");
    }

    @Test
    void testPathNotFoundInTemplate() throws IOException {
        String originalYaml = "dodb-pipeline:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"database.example.com\"\n" +
                "      port: 5432\n";

        String templateYaml = "dodb-pipeline:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"{{dodb-pipeline.source.documentdb.hostname}}\"\n";

        String expectedYaml = "dodb-pipeline:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"database.example.com\"\n";

        String transformedYaml = yamlTransformer.transformYaml(originalYaml, templateYaml);

        assertEquals(expectedYaml.trim(), transformedYaml.trim(), "The transformed YAML should not include unspecified paths.");
    }

    @Test
    void testIOExceptionHandling() {
        String invalidYaml = "dodb-pipeline: [";

        assertThrows(RuntimeException.class, () -> {
            yamlTransformer.transformYaml(invalidYaml, invalidYaml);
        }, "A RuntimeException should be thrown when an IOException occurs.");
    }



}
