/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

public class MaskActionConfig {

    @JsonProperty("mask_character")
    @Pattern(regexp = "[*#!%&@]", message = "Valid characters are *, #, $, %, &, ! and @")
    private String maskCharacter = "*";

    @JsonProperty("mask_character_length")
    @Min(1)
    @Max(10)
    private int maskCharacterLength = 3;

    public MaskActionConfig() {
    }

    public MaskActionConfig(String maskCharacter, int maskCharacterLength) {
        this.maskCharacter = maskCharacter;
        this.maskCharacterLength = maskCharacterLength;
    }

    public String getMaskCharacter() {
        return maskCharacter;
    }

    public int getMaskCharacterLength() {
        return maskCharacterLength;
    }
}
