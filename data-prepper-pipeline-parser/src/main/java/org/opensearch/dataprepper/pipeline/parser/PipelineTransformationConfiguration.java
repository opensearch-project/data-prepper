package org.opensearch.dataprepper.pipeline.parser;

import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.transformer.DynamicConfigTransformer;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Named;

@Configuration
public class PipelineTransformationConfiguration {
    public static final String TEMPLATES_DIRECTORY_PATH = "TEMPLATES_DIRECTORY_PATH";
    public static final String RULES_DIRECTORY_PATH = "VALIDATORS_DIRECTORY_PATH";

    @Bean
    @Named(RULES_DIRECTORY_PATH)
    static String provideRulesDirectoryPath() {
        return "resources/rules";
    }

    @Bean
    @Named(TEMPLATES_DIRECTORY_PATH)
    static String provideTemplateDirectoryPath() {
        return "resources/templates";
    }

    @Bean
    TransformersFactory transformersFactory(
            @Named(TEMPLATES_DIRECTORY_PATH) String provideTransformerDirectoryPath,
            @Named(RULES_DIRECTORY_PATH) String provideTemplateDirectoryPath
    ) {
        return new TransformersFactory(RULES_DIRECTORY_PATH, TEMPLATES_DIRECTORY_PATH);
    }

    @Bean
    public DynamicConfigTransformer pipelineConfigTransformer(
            RuleEvaluator ruleEvaluator) {
        return new DynamicConfigTransformer(ruleEvaluator);
    }

    @Bean
    public RuleEvaluator ruleEvaluator(TransformersFactory transformersFactory) {
        return new RuleEvaluator(transformersFactory);
    }
}
