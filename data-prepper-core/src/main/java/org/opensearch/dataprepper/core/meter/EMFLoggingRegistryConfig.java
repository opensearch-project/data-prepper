/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.meter;

import io.micrometer.core.instrument.step.StepRegistryConfig;

import java.util.Collections;
import java.util.Map;

public interface EMFLoggingRegistryConfig extends StepRegistryConfig {

    @Override
    default String prefix() {
        return "emf";
    }

    default Map<String, String> additionalProperties() {
        return Collections.emptyMap();
    }
}
