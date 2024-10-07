/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

@JsonClassDescription("Obfuscates data by masking data. Without any configuration this will replace text with <code>***</code>")
@JsonPropertyOrder
public class MaskActionConfig {

    @JsonProperty("mask_character")
    @Pattern(regexp = "[*#!%&@]", message = "Valid characters are *, #, $, %, &, ! and @")
    @JsonPropertyDescription("The character to use to mask text. By default, this is <code>*</code>")
    private String maskCharacter = "*";

    @JsonProperty("mask_character_length")
    @Min(1)
    @Max(10)
    @JsonPropertyDescription("The length of the character mask to apply. By default, this is three characters.")
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
