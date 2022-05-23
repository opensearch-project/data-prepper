/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import jakarta.validation.constraints.NotEmpty;

import java.util.Map;

public class CodecOption {
    @NotEmpty (message = "codec cannot be empty")
    private Map<String, Object> codec;

    public Map<String, Object> getCodec() {
        return codec;
    }
}
