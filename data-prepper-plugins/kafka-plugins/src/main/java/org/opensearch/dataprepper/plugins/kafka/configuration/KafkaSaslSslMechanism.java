/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.function.Function;                                                                                                                      

public enum KafkaSaslSslMechanism {
    MSK_IAM("msk_iam");

    private static final Map<String, KafkaSaslSslMechanism> MECHANISMS_MAP = Arrays.stream(KafkaSaslSslMechanism.values())
        .collect(Collectors.toMap(KafkaSaslSslMechanism::toString, Function.identity()));
    private final String mechanism;

    KafkaSaslSslMechanism(final String mechanism) {
        this.mechanism =  mechanism;
    }

    public String toString() {
        return this.mechanism;
    }

    @JsonCreator
    public static KafkaSaslSslMechanism getByName(final String mechanism) {
        return MECHANISMS_MAP.get(mechanism.toUpperCase());
    }
    
}

