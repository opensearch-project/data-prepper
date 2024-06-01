/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser;

import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Named;

@Configuration
public class PipelineTransformationConfiguration {
    public static final String TEMPLATES_DIRECTORY_PATH = "TEMPLATES_DIRECTORY_PATH";
    public static final String RULES_DIRECTORY_PATH = "RULES_DIRECTORY_PATH";

    @Bean
    @Named(RULES_DIRECTORY_PATH)
    static String provideRulesDirectoryPath() {
        ClassLoader classLoader = PipelineTransformationConfiguration.class.getClassLoader();
        String filePath = classLoader.getResource("rules").getFile();
        return filePath;
    }

    @Bean
    @Named(TEMPLATES_DIRECTORY_PATH)
    static String provideTemplateDirectoryPath() {
        ClassLoader classLoader = PipelineTransformationConfiguration.class.getClassLoader();
        String filePath = classLoader.getResource("templates").getFile();
        return filePath;
    }

    @Bean
    TransformersFactory transformersFactory(
            @Named(RULES_DIRECTORY_PATH) String rulesDirectoryPath,
            @Named(TEMPLATES_DIRECTORY_PATH) String templatesDirectoryPath
    ) {
        return new TransformersFactory(rulesDirectoryPath, templatesDirectoryPath);
    }

    @Bean
    public RuleEvaluator ruleEvaluator(TransformersFactory transformersFactory) {
        return new RuleEvaluator(transformersFactory);
    }
}
