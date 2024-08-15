/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser;

import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PipelineTransformationConfiguration {

    @Bean
    TransformersFactory transformersFactory() {
        return new TransformersFactory();
    }

    @Bean
    public RuleEvaluator ruleEvaluator(TransformersFactory transformersFactory) {
        return new RuleEvaluator(transformersFactory);
    }
}
