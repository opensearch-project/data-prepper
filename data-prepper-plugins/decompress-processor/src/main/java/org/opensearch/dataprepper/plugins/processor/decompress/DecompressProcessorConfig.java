/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.EncodingType;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.DecoderEngineFactory;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>decompress</code> processor decompresses any Base64-encoded " +
        "compressed fields inside of an event.")
public class DecompressProcessorConfig {

    @JsonPropertyDescription("The keys in the event that will be decompressed.")
    @JsonProperty("keys")
    @NotEmpty
    @NotNull
    private List<String> keys;

    @JsonPropertyDescription("The type of decompression to use for the keys in the event. Only <code>gzip</code> is supported.")
    @JsonProperty("type")
    @NotNull
    @ExampleValues({
            @ExampleValues.Example(value = "gzip", description = "Specifies gzip decompression")
    })
    private DecompressionType decompressionType;

    @JsonPropertyDescription("A list of strings with which to tag events when the processor fails to decompress the keys inside an event. Defaults to <code>_decompression_failure</code>.")
    @JsonProperty("tags_on_failure")
    private List<String> tagsOnFailure = List.of("_decompression_failure");

    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, such as <code>/is_compressed == true</code>, that determines when the decompress processor will run on certain events.")
    @JsonProperty("decompress_when")
    @ExampleValues({
            @ExampleValues.Example(value = "/some_key == null", description = "Only runs the decompress processor on the Event if the key some_key is null or does not exist.")
    })
    private String decompressWhen;

    @JsonIgnore
    private final EncodingType encodingType = EncodingType.BASE64;

    public List<String> getKeys() {
        return keys;
    }

    public DecompressionEngineFactory getDecompressionType() {
        return decompressionType;
    }

    public DecoderEngineFactory getEncodingType() { return encodingType; }

    public String getDecompressWhen() {
        return decompressWhen;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}
