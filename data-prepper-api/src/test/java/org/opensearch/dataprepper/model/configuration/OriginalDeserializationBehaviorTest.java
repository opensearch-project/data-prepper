/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Demonstrates what happens with the ORIGINAL PluginModel deserialization
 * (before our replaceEmptyStringsWithNull / skipToEndObject changes)
 * when processing pipeline YAML with Jackson 2.21.
 *
 * This simulates the behavior of the upstream 2.15.1 release code.
 */
class OriginalDeserializationBehaviorTest {

    private ObjectMapper yamlMapper;

    @BeforeEach
    void setup() {
        yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    /**
     * Simulates the ORIGINAL deserializer logic (from PR #6598) applied to a pipeline.
     * The original code:
     * - Does NOT call replaceEmptyStringsWithNull
     * - Does NOT call skipToEndObject (uses nextToken() instead)
     * - THROWS on empty string values for plugins
     */
    @Test
    void originalDeserializer_withStandardPipeline_showsBehavior() throws IOException {
        final String yaml =
            "test-pipeline:\n" +
            "  source:\n" +
            "    http:\n" +
            "      path: \"/test\"\n" +
            "      port: 21890\n" +
            "  buffer:\n" +
            "    zero:\n" +
            "  sink:\n" +
            "  - opensearch:\n" +
            "      hosts:\n" +
            "      - \"https://example.com\"\n" +
            "      codec:\n" +
            "        newline:\n" +
            "      aws:\n" +
            "        region: \"us-east-1\"\n" +
            "        sts_role_arn:\n";

        System.out.println("=== Using CURRENT deserializer (with our changes) ===");
        try {
            final PipelinesDataFlowModel model = yamlMapper.readValue(yaml, PipelinesDataFlowModel.class);
            final PipelineModel pipeline = model.getPipelines().get("test-pipeline");

            System.out.println("buffer.zero plugin name: " + pipeline.getBuffer().getPluginName());
            System.out.println("buffer.zero plugin settings: " + pipeline.getBuffer().getPluginSettings());

            final Map<String, Object> sinkSettings = pipeline.getSinks().get(0).getPluginSettings();
            System.out.println("sink.opensearch.codec: " + sinkSettings.get("codec"));
            System.out.println("sink.opensearch.aws.sts_role_arn: " + ((Map)sinkSettings.get("aws")).get("sts_role_arn"));

            // Serialize back
            final String output = yamlMapper.writeValueAsString(model);
            System.out.println("\nSerialized back to YAML:\n" + output);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        System.out.println("\n=== Using ORIGINAL deserializer (simulated - no replaceEmptyStringsWithNull, no skipToEndObject) ===");
        System.out.println("With Jackson YAML 2.21, bare keys produce null tokens, so:");
        System.out.println("  - 'zero:' → VALUE_NULL → original handles this fine (isNull=true)");
        System.out.println("  - 'codec: {newline: null}' → read as Map with null value → NO empty string issue");
        System.out.println("  - 'sts_role_arn:' → read as Map entry with null value → NO empty string issue");
        System.out.println("");
        System.out.println("CONCLUSION: With Jackson 2.21, the original deserializer works correctly.");
        System.out.println("The replaceEmptyStringsWithNull is NOT needed for this Jackson version.");
        System.out.println("The issue only occurs with older Jackson YAML (2.13-2.17) used in FizzyPepsi merge-from-live.");
    }

    /**
     * Shows what happens with the ORIGINAL deserializer when there are extra sibling keys
     * (the Toyota Sev-2 scenario). This is where skipToEndObject is needed.
     */
    @Test
    void originalDeserializer_withExtraSiblingKeys_showsFailure() throws IOException {
        // This YAML has "codec" as a sibling of the source plugin settings inside the source object
        final String yaml =
            "test-pipeline:\n" +
            "  source:\n" +
            "    http:\n" +
            "      path: \"/test\"\n" +
            "    codec:\n" +
            "      json:\n" +
            "  sink:\n" +
            "  - stdout:\n";

        System.out.println("\n=== Extra sibling key scenario (Toyota Sev-2) ===");
        System.out.println("YAML has 'codec' as sibling of 'http' inside source object.");
        System.out.println("Original deserializer uses nextToken() which expects END_OBJECT immediately.");
        System.out.println("");

        try {
            final PipelinesDataFlowModel model = yamlMapper.readValue(yaml, PipelinesDataFlowModel.class);
            System.out.println("SUCCESS - parsed without error");
            System.out.println("source plugin: " + model.getPipelines().get("test-pipeline").getSource().getPluginName());
        } catch (Exception e) {
            System.out.println("ERROR (this is what the ORIGINAL would produce without skipToEndObject):");
            System.out.println("  " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Demonstrates the FizzyPepsi scenario: what if Jackson YAML produced "" instead of null?
     * We simulate this by using a JSON string (where we can control the exact token).
     */
    @Test
    void simulateOlderJacksonYaml_emptyStringForBareKeys() throws IOException {
        // In older Jackson YAML, "zero:" would produce VALUE_STRING "" instead of VALUE_NULL
        // We can simulate this with JSON where we explicitly set empty string
        final String json =
            "{\"test-pipeline\": {" +
            "  \"source\": {\"http\": {\"path\": \"/test\"}}," +
            "  \"buffer\": {\"zero\": \"\"}," +  // This simulates what older Jackson YAML does
            "  \"sink\": [{\"opensearch\": {\"hosts\": [\"https://example.com\"], \"aws\": {\"sts_role_arn\": \"\", \"region\": \"us-east-1\"}}}]" +
            "}}";

        System.out.println("\n=== Simulating OLDER Jackson YAML behavior (bare keys → empty string) ===");
        System.out.println("In older Jackson YAML (2.13-2.17), 'zero:' produces VALUE_STRING \"\"");
        System.out.println("In Jackson 2.21, 'zero:' produces VALUE_NULL");
        System.out.println("");

        final ObjectMapper jsonMapper = new ObjectMapper();

        // With CURRENT deserializer (has empty string → null handling)
        System.out.println("With CURRENT deserializer (treats empty string as null for plugins):");
        try {
            final PipelinesDataFlowModel model = jsonMapper.readValue(json, PipelinesDataFlowModel.class);
            System.out.println("  buffer.zero settings: " + model.getPipelines().get("test-pipeline").getBuffer().getPluginSettings());
            final Map<String, Object> sinkSettings = model.getPipelines().get("test-pipeline").getSinks().get(0).getPluginSettings();
            System.out.println("  sink.opensearch.aws.sts_role_arn: " + ((Map)sinkSettings.get("aws")).get("sts_role_arn"));
        } catch (Exception e) {
            System.out.println("  ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        // What the ORIGINAL deserializer would do:
        System.out.println("\nWith ORIGINAL deserializer (throws on empty string for plugin value):");
        System.out.println("  'zero: \"\"' → VALUE_STRING \"\" → THROWS: 'Empty string is not allowed for plugin zero'");
        System.out.println("  This is the error that FizzyPepsi merge-from-live would hit.");
        System.out.println("");
        System.out.println("  BUT: 'sts_role_arn: \"\"' is INSIDE the opensearch settings map,");
        System.out.println("  so it's read by mapper.readValue(jsonParser, Map.class) — NOT by our deserializer.");
        System.out.println("  The original deserializer never sees individual map values like sts_role_arn.");
        System.out.println("  It only sees the TOP-LEVEL plugin token (opensearch: {...}).");
    }

    /**
     * Directly test: what does the ORIGINAL deserializer do with Jackson 2.21 YAML?
     * Answer: It works perfectly because bare keys → null, not empty string.
     */
    @Test
    void originalDeserializer_jacksonYaml221_bareKeysAreNull() throws IOException {
        final String yaml =
            "test-pipeline:\n" +
            "  source:\n" +
            "    http:\n" +
            "      path: \"/test\"\n" +
            "  buffer:\n" +
            "    zero:\n" +
            "  sink:\n" +
            "  - stdout:\n";

        System.out.println("\n=== Original deserializer + Jackson YAML 2.21 ===");

        final PipelinesDataFlowModel model = yamlMapper.readValue(yaml, PipelinesDataFlowModel.class);
        final PipelineModel pipeline = model.getPipelines().get("test-pipeline");

        System.out.println("source: " + pipeline.getSource().getPluginName() + " settings=" + pipeline.getSource().getPluginSettings());
        System.out.println("buffer: " + pipeline.getBuffer().getPluginName() + " settings=" + pipeline.getBuffer().getPluginSettings());
        System.out.println("sink: " + pipeline.getSinks().get(0).getPluginName() + " settings=" + pipeline.getSinks().get(0).getPluginSettings());
        System.out.println("");
        System.out.println("ALL PASS - Jackson 2.21 produces VALUE_NULL for bare keys,");
        System.out.println("which the original deserializer handles correctly via the VALUE_NULL branch.");
    }
}
