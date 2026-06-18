/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * This test demonstrates what Jackson YAML 2.21 produces for various bare-key
 * configurations WITHOUT any custom PluginModel deserialization.
 *
 * Purpose: Show the raw Jackson behavior so we can understand what our custom
 * deserializer needs to handle vs what it should NOT touch.
 */
class RawJacksonYamlBehaviorTest {

    private ObjectMapper yamlMapper;

    @BeforeEach
    void setup() {
        yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    /**
     * Shows what Jackson YAML 2.21 produces for a bare key like "newline:" inside a map.
     * In older Jackson (2.12-2.16), this was null. In 2.17+, it may be "".
     */
    @Test
    void bareKey_inNestedMap_showsRawJacksonBehavior() throws IOException {
        final String yaml =
            "sink:\n" +
            "  opensearch:\n" +
            "    hosts:\n" +
            "    - \"https://example.com\"\n" +
            "    codec:\n" +
            "      newline:\n" +
            "    aws:\n" +
            "      region: \"us-east-1\"\n";

        final Map<String, Object> raw = yamlMapper.readValue(yaml, Map.class);
        final Map<String, Object> opensearch = (Map<String, Object>) raw.get("sink");
        // This is what Jackson gives us for "opensearch" at the top level
        System.out.println("=== Raw Jackson YAML parse (no custom deserializer) ===");
        System.out.println("Full parsed map: " + raw);

        final Map<String, Object> os = (Map<String, Object>) ((Map<String, Object>) raw.get("sink")).get("opensearch");
        final Map<String, Object> codec = (Map<String, Object>) os.get("codec");
        final Object newlineValue = codec.get("newline");

        System.out.println("codec.newline raw value: " + repr(newlineValue));
        System.out.println("codec.newline type: " + (newlineValue == null ? "null" : newlineValue.getClass().getName()));
        // This assertion documents the ACTUAL behavior - will show "" or null
    }

    /**
     * Shows what happens with a top-level bare key plugin like "stdout:" in a pipeline.
     * This is the PluginModel case - the key name is the plugin, value should be null/empty.
     */
    @Test
    void bareKey_topLevelPlugin_showsRawJacksonBehavior() throws IOException {
        final String yaml =
            "test-pipeline:\n" +
            "  source:\n" +
            "    http:\n" +
            "  sink:\n" +
            "  - stdout:\n";

        final Map<String, Object> raw = yamlMapper.readValue(yaml, Map.class);
        System.out.println("\n=== Bare key plugin (stdout:) ===");
        System.out.println("Full parsed map: " + raw);

        final Map<String, Object> pipeline = (Map<String, Object>) raw.get("test-pipeline");
        final Map<String, Object> source = (Map<String, Object>) pipeline.get("source");
        final Object httpValue = source.get("http");

        System.out.println("source.http raw value: " + repr(httpValue));
        System.out.println("source.http type: " + (httpValue == null ? "null" : httpValue.getClass().getName()));
    }

    /**
     * Shows what happens with sts_role_arn: (bare key that is NOT a PluginModel).
     * This is the case dlv flagged - empty string vs null matters here.
     */
    @Test
    void bareKey_nonPluginField_showsRawJacksonBehavior() throws IOException {
        final String yaml =
            "sink:\n" +
            "  opensearch:\n" +
            "    hosts:\n" +
            "    - \"https://example.com\"\n" +
            "    aws:\n" +
            "      region: \"us-east-1\"\n" +
            "      sts_role_arn:\n" +
            "    dlq:\n" +
            "      s3:\n" +
            "        bucket: \"my-bucket\"\n" +
            "        region: \"us-east-1\"\n" +
            "        sts_role_arn:\n";

        final Map<String, Object> raw = yamlMapper.readValue(yaml, Map.class);
        System.out.println("\n=== Bare key non-plugin field (sts_role_arn:) ===");

        final Map<String, Object> os = (Map<String, Object>) ((Map<String, Object>) raw.get("sink")).get("opensearch");
        final Map<String, Object> aws = (Map<String, Object>) os.get("aws");
        final Object stsValue = aws.get("sts_role_arn");

        System.out.println("aws.sts_role_arn raw value: " + repr(stsValue));
        System.out.println("aws.sts_role_arn type: " + (stsValue == null ? "null" : stsValue.getClass().getName()));

        final Map<String, Object> dlq = (Map<String, Object>) os.get("dlq");
        final Map<String, Object> s3 = (Map<String, Object>) dlq.get("s3");
        final Object dlqStsValue = s3.get("sts_role_arn");
        System.out.println("dlq.s3.sts_role_arn raw value: " + repr(dlqStsValue));
        System.out.println("dlq.s3.sts_role_arn type: " + (dlqStsValue == null ? "null" : dlqStsValue.getClass().getName()));
    }

    /**
     * Shows what happens with the zero buffer case: "zero:" as a plugin.
     */
    @Test
    void bareKey_zeroBuffer_showsRawJacksonBehavior() throws IOException {
        final String yaml =
            "test-pipeline:\n" +
            "  source:\n" +
            "    http:\n" +
            "      path: \"/test\"\n" +
            "  buffer:\n" +
            "    zero:\n" +
            "  sink:\n" +
            "  - stdout:\n";

        final Map<String, Object> raw = yamlMapper.readValue(yaml, Map.class);
        System.out.println("\n=== Zero buffer bare key ===");

        final Map<String, Object> pipeline = (Map<String, Object>) raw.get("test-pipeline");
        final Map<String, Object> buffer = (Map<String, Object>) pipeline.get("buffer");
        final Object zeroValue = buffer.get("zero");

        System.out.println("buffer.zero raw value: " + repr(zeroValue));
        System.out.println("buffer.zero type: " + (zeroValue == null ? "null" : zeroValue.getClass().getName()));
    }

    /**
     * Full pipeline transformation test: deserialize through PipelinesDataFlowModel
     * (which uses our custom PluginModel deserializer) and then serialize back.
     * Shows the end-to-end effect.
     */
    @Test
    void fullPipeline_deserializeAndSerialize_showsTransformation() throws IOException {
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

        System.out.println("\n=== Full pipeline: WITH custom PluginModel deserializer ===");
        final PipelinesDataFlowModel model = yamlMapper.readValue(yaml, PipelinesDataFlowModel.class);

        // Check buffer.zero
        final PluginModel bufferPlugin = model.getPipelines().get("test-pipeline").getBuffer();
        System.out.println("buffer plugin name: " + bufferPlugin.getPluginName());
        System.out.println("buffer plugin settings: " + bufferPlugin.getPluginSettings());

        // Check sink codec.newline
        final Map<String, Object> sinkSettings = model.getPipelines().get("test-pipeline").getSinks().get(0).getPluginSettings();
        final Map<String, Object> codec = (Map<String, Object>) sinkSettings.get("codec");
        System.out.println("sink.codec.newline: " + repr(codec.get("newline")));

        // Check sink aws.sts_role_arn
        final Map<String, Object> aws = (Map<String, Object>) sinkSettings.get("aws");
        System.out.println("sink.aws.sts_role_arn: " + repr(aws.get("sts_role_arn")));

        // Serialize back to YAML
        final String output = yamlMapper.writeValueAsString(model);
        System.out.println("\nSerialized output:\n" + output);
    }

    /**
     * Same pipeline but parsed as raw Map (NO custom deserializer) to compare.
     */
    @Test
    void fullPipeline_rawMapParse_showsWhatJacksonProducesNatively() throws IOException {
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

        System.out.println("\n=== Full pipeline: WITHOUT custom deserializer (raw Map) ===");
        final Map<String, Object> raw = yamlMapper.readValue(yaml, Map.class);

        final Map<String, Object> pipeline = (Map<String, Object>) raw.get("test-pipeline");
        final Map<String, Object> buffer = (Map<String, Object>) pipeline.get("buffer");
        System.out.println("buffer.zero: " + repr(buffer.get("zero")));

        final java.util.List<Map<String, Object>> sinks = (java.util.List<Map<String, Object>>) pipeline.get("sink");
        final Map<String, Object> opensearch = (Map<String, Object>) sinks.get(0).get("opensearch");
        final Map<String, Object> codec = (Map<String, Object>) opensearch.get("codec");
        System.out.println("sink.opensearch.codec.newline: " + repr(codec.get("newline")));

        final Map<String, Object> aws = (Map<String, Object>) opensearch.get("aws");
        System.out.println("sink.opensearch.aws.sts_role_arn: " + repr(aws.get("sts_role_arn")));

        // Now serialize back to YAML to see what the output looks like
        final String output = yamlMapper.writeValueAsString(raw);
        System.out.println("\nRe-serialized (raw, no custom):\n" + output);
    }

    private static String repr(Object value) {
        if (value == null) return "null";
        return "\"" + value + "\" (class=" + value.getClass().getSimpleName() + ")";
    }
}
