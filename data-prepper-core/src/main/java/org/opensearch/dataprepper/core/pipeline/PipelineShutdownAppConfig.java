/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.pipeline;

import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.function.Predicate;

@Configuration
public class PipelineShutdownAppConfig {
    @Bean("shouldShutdownOnPipelineFailurePredicate")
    Predicate<Map<String, Pipeline>> shouldShutdownOnPipelineFailurePredicate(final DataPrepperConfiguration dataPrepperConfiguration) {
        return dataPrepperConfiguration.getPipelineShutdown().getShouldShutdownOnPipelineFailurePredicate();
    }
}
