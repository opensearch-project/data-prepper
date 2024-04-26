/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class RuleEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(RuleEvaluator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final TransformersFactory transformersFactory;
    private String PLUGIN_NAME = null;

    public RuleEvaluator(TransformersFactory transformersFactory) {
        this.transformersFactory = transformersFactory;
    }

    public RuleEvaluatorResult isTransformationNeeded(PipelinesDataFlowModel pipelineModel) {
        return isDocDBSource(pipelineModel);
    }

    /**
     * Evaluates model based on pre defined rules and
     * result contains the name of the pipeline that will need transformation,
     * evaluated boolean result and the corresponding template model
     * Assumption: only one pipeline can have transformation.
     * @param pipelinesModel
     * @return RuleEvaluatorResult
     */
    private RuleEvaluatorResult isDocDBSource(PipelinesDataFlowModel pipelinesModel) {
        PLUGIN_NAME = "documentdb";
//        String pluginRulesPath = transformersFactory.getPluginRuleFileLocation(PLUGIN_NAME);
//        File ruleFile = new File(pluginRulesPath);
//        LOG.info("Checking rule path {}",ruleFile.getAbsolutePath());

        Map<String, PipelineModel> pipelines = pipelinesModel.getPipelines();

        for (Map.Entry<String, PipelineModel> entry : pipelines.entrySet()) {
            try {
                String pipelineJson = OBJECT_MAPPER.writeValueAsString(entry);
                if (evaluate(pipelineJson, PLUGIN_NAME)) {
                    LOG.info("Rule for {} is evaluated true for pipelineJson {}", PLUGIN_NAME, pipelineJson);

//                    String templateFilePathString = transformersFactory.getPluginTemplateFileLocation(PLUGIN_NAME);
//                    File templateFile = new File(templateFilePathString);
//                    LOG.info("Absolute path of template file: {}",templateFile.getAbsolutePath());
                    InputStream templateStream = transformersFactory.getPluginTemplateFileStream(PLUGIN_NAME);
                    PipelineTemplateModel templateModel = yamlMapper.readValue(templateStream,
                            PipelineTemplateModel.class);
                    LOG.info("Template is chosen for {}", PLUGIN_NAME);

                    return RuleEvaluatorResult.builder()
                            .withEvaluatedResult(true)
                            .withPipelineTemplateModel(templateModel)
                            .withPipelineName(entry.getKey())
                            .build();
                }
            } catch (JsonProcessingException e) {
                LOG.error("Error processing json");
                throw new RuntimeException(e);
            } catch (IOException e) {
                LOG.error("Error reading file");
                throw new RuntimeException(e);
            }
        }
        return RuleEvaluatorResult.builder()
                .withEvaluatedResult(false)
                .withPipelineName(null)
                .withPipelineTemplateModel(null)
                .build();
    }

    private Boolean evaluate(String pipelinesJson,
                             String pluginName) {

        Configuration parseConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .options(Option.AS_PATH_LIST)
                .build();
        ParseContext parseContext = JsonPath.using(parseConfig);
        ReadContext readPathContext = parseContext.parse(pipelinesJson);

        try {
            //TODO
//            ClassLoader classLoader = RuleEvaluator.class.getClassLoader();
//            InputStream filestream = classLoader.getResourceAsStream("rules/documentdb-rule.yaml");
            InputStream ruleStream = transformersFactory.getPluginRuleFileStream(pluginName);
            RuleTransformerModel rulesModel = yamlMapper.readValue(ruleStream, RuleTransformerModel.class);
            List<String> rules = rulesModel.getApplyWhen();
            for (String rule : rules) {
                Object result = readPathContext.read(rule);
            }
        } catch (IOException e) {
            LOG.warn("Error reading {} rule",pluginName);
            return false;
        } catch (PathNotFoundException e) {
            LOG.warn("Json Path not found for {}", pluginName);
            return false;
        }
        return true;
    }
}

