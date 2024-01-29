/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.EncodingType;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.IEncodingType;

import java.util.List;

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

    public IDecompressionType getDecompressionType() {
        return decompressionType;
    }

    public IEncodingType getEncodingType() { return encodingType; }

    public String getDecompressWhen() {
        return decompressWhen;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}
