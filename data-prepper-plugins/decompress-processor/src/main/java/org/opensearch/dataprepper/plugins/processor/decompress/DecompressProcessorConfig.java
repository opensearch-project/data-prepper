/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.EncodingType;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.DecoderEngineFactory;

import java.util.List;

public class DecompressProcessorConfig {

    @JsonPropertyDescription("The keys in the event that will be decompressed.")
    @JsonProperty("keys")
    @NotEmpty
    @NotNull
    private List<String> keys;

    @JsonPropertyDescription("The type of decompression to use for the keys in the event. Only gzip is supported.")
    @JsonProperty("type")
    @NotNull
    private DecompressionType decompressionType;

    @JsonPropertyDescription("A conditional expression that determines when the decompress processor will run on certain events.")
    @JsonProperty("decompress_when")
    private String decompressWhen;

    @JsonPropertyDescription("A list of strings with which to tag events when the processor fails to decompress the keys inside an event. Defaults to _decompression_failure.")
    @JsonProperty("tags_on_failure")
    private List<String> tagsOnFailure = List.of("_decompression_failure");

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
