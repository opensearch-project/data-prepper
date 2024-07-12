/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;

public class OneWayHashActionConfig {


    @JsonProperty("salt")
    @NotEmpty
    private byte[] salt;

    @JsonProperty("format")
    @Pattern(regexp = "SHA-512", message = "Valid values SHA-512")
    private String hashFormat = "SHA-512";

    public OneWayHashActionConfig() {
        byte [] saltBytes = new byte[64];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(saltBytes);
        this.salt = saltBytes;   
    }

    public OneWayHashActionConfig(String hashFormat, String salt) {

        this.hashFormat = hashFormat;
        
        if (salt == null || salt.isEmpty() ) {
            byte [] saltBytes = new byte[64];
            SecureRandom secureRandom = new SecureRandom();
            secureRandom.nextBytes(saltBytes);
            this.salt = saltBytes;
        } else {
            this.salt = salt.getBytes(StandardCharsets.UTF_8);;
        }
    }

    public OneWayHashActionConfig(String salt) {
        this.salt = salt.getBytes(StandardCharsets.UTF_8);
    }

    public byte [] getSalt (){
        return salt;
    }

    public String getHashFormat(){
        return hashFormat;
    }
}

