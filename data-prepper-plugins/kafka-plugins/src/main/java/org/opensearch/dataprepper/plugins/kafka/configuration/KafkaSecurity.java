/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaSecurity {

    public class KafkaSaslMechanism {
        @JsonProperty("ssl")
        private KafkaSaslSslMechanism saslSslMechanism;
    }

    public class KafkaSaslConfig {
        @JsonProperty("mechanism")
        private KafkaSaslMechanism saslMechanism;
        
    }

    public class KafkaSslConfig {
    }

    public class KafkaEncryptionConfig {
    }
    
    public class KafkaAuthenticationConfig {
        @JsonProperty("ssl")
        private KafkaSslConfig sslConfig;

        @JsonProperty("sasl")
        private KafkaSaslConfig saslConfig;
        
        public KafkaSslConfig getSslConfig() {
            return sslConfig;
        }

        public KafkaSaslConfig saslConfig() {
            return saslConfig;
        }
        
    }

    @JsonProperty("authentication")
    private KafkaAuthenticationConfig authenticationConfig;

    @JsonProperty("encryption")
    private KafkaEncryptionConfig encryptionConfig;

    public KafkaAuthenticationConfig getAuthenticationConfig() {
        return authenticationConfig;
    }
    
    public KafkaEncryptionConfig getEncryptionConfig() {
        return encryptionConfig;
    }

}


