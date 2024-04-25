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
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
public class PipelineTransformationConfiguration {
    public static final String TEMPLATES_DIRECTORY_PATH = "TEMPLATES_DIRECTORY_PATH";
    public static final String RULES_DIRECTORY_PATH = "RULES_DIRECTORY_PATH";
    private static final Path currentDir = Paths.get(System.getProperty("user.dir"));
    private static final String parserRelativePath = "/data-prepper-pipeline-parser/src";

    @Bean
    @Named(RULES_DIRECTORY_PATH)
    static String provideRulesDirectoryPath() {
        return currentDir.toString()+ parserRelativePath + "/resources/rules";
    }

    @Bean
    @Named(TEMPLATES_DIRECTORY_PATH)
    static String provideTemplateDirectoryPath() {
        return currentDir.toString() + parserRelativePath + "/resources/templates";
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
