/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum EncodingType implements DecoderEngineFactory {
    BASE64("base64");

    private final String option;

    private static final Map<String, EncodingType> OPTIONS_MAP = Arrays.stream(EncodingType.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private static final Map<String, DecoderEngine> DECODER_ENGINE_MAP = Map.of(
            "base64", new Base64DecoderEngine()
    );

    EncodingType(final String option) {
        this.option = option;
    }

    @JsonCreator
    static EncodingType fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }

    @Override
    public DecoderEngine getDecoderEngine() {
        return DECODER_ENGINE_MAP.get(this.option);
    }
}
