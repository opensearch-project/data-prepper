/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.EncodingType;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.DecoderEngineFactory;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `decompress` processor decompresses any Base64-encoded " +
        "compressed fields inside of an event.")
public class DecompressProcessorConfig {

    @JsonProperty("keys")
    @NotEmpty
    private List<String> keys;

    @JsonProperty("type")
    @NotNull
    private DecompressionType decompressionType;

    @JsonProperty("decompress_when")
    private String decompressWhen;

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
