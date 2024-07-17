/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.Pattern;


public class OneWayHashActionConfig {


    @JsonProperty("salt")
    private byte[] salt;

    @JsonProperty("format")
    @Pattern(regexp = "SHA-512", message = "Valid values SHA-512")
    private String hashFormat = "SHA-512";

    @JsonProperty("salt_key")
    private String saltKey;

    public OneWayHashActionConfig() {        
        this.salt = generateSalt();   
    }

    public OneWayHashActionConfig(String hashFormat, String salt, String saltKey) {

        this.hashFormat = hashFormat;
        this.saltKey = saltKey;

        if (salt == null || salt.isEmpty() ) {
            this.salt = generateSalt();
        } else {
            this.salt = salt.getBytes(StandardCharsets.UTF_8);
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

    public String getSaltKey(){
        return saltKey;
    }

    private byte[] generateSalt(){
        byte [] saltBytes = new byte[64];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(saltBytes);        
        return saltBytes;
    }
}

