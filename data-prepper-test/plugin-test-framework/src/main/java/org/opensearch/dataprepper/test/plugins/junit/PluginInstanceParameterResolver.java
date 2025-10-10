/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.pipeline.parser.ParseException;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

class PluginInstanceParameterResolver implements ParameterResolver {
    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        final DataPrepperPluginTest annotation = testClass.getAnnotation(DataPrepperPluginTest.class);
        if (annotation == null) {
            return false;
        }

        return Objects.equals(annotation.pluginType(), parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        final DataPrepperPluginTest annotation = testClass.getAnnotation(DataPrepperPluginTest.class);
        if (annotation == null) {
            throw new ParameterResolutionException("Missing @DataPrepperPluginTest annotation on class: " + testClass.getName());
        }

        final PluginConfigurationFile configurationFileAnnotation = parameterContext.findAnnotation(PluginConfigurationFile.class)
                .orElseThrow(() -> new ParameterResolutionException("Parameter resolver used without @PluginConfigurationFile."));

        final String configurationFile = configurationFileAnnotation.value();

        final PluginSetting pluginSetting = generatePluginSetting(testClass, configurationFile);

        final PluginFactory pluginFactory = getOrCreatePluginFactory(extensionContext);

        final Class<?> pluginType = annotation.pluginType();

        return pluginFactory.loadPlugin(pluginType, pluginSetting);
    }

    private PluginSetting generatePluginSetting(final Class<?> testClass, final String configurationFile) {
        final PipelinesDataFlowModel pipelinesDataFlowModel = readPipelinesDataFlowModel(testClass, configurationFile);
        String pipelineName = readSinglePipeline(pipelinesDataFlowModel, configurationFile);
        final PipelineModel pipelineModel = pipelinesDataFlowModel.getPipelines().get(pipelineName);
        final PluginModel pluginModel = loadPluginModel(pipelineModel, configurationFile);

        final Map<String, Object> settingsMap = Optional
                .ofNullable(pluginModel.getPluginSettings())
                .orElseGet(HashMap::new);
        final PluginSetting pluginSetting = new PluginSetting(pluginModel.getPluginName(), settingsMap);
        pluginSetting.setPipelineName(pipelineName);

        return pluginSetting;
    }

    private static PluginModel loadPluginModel(final PipelineModel pipelineModel, final String configurationFile) {
        if(pipelineModel.getProcessors() == null || pipelineModel.getProcessors().size() != 1) {
            throw new ParameterResolutionException("Test configurations must define plugins in the processor section. " + configurationFile);
        }

        return pipelineModel.getProcessors()
                .stream()
                .findFirst()
                .orElseThrow(() -> new ParameterResolutionException("Test configurations must define plugins in the processor section. " + configurationFile));
    }

    private static String readSinglePipeline(final PipelinesDataFlowModel pipelinesDataFlowModel, final String configurationFile) {
        if(pipelinesDataFlowModel.getPipelines().size() != 1) {
            throw new ParameterResolutionException("Test configurations must have exactly one pipeline. " + configurationFile);
        }

        return pipelinesDataFlowModel.getPipelines()
                .keySet()
                .stream()
                .findFirst()
                .orElseThrow(() -> new ParameterResolutionException("Test configurations must have exactly one pipeline. " + configurationFile));
    }

    private static PipelinesDataFlowModel readPipelinesDataFlowModel(final Class<?> testClass, final String configurationFile) {
        final PipelinesDataFlowModel pipelinesDataFlowModel;
        try(final InputStream resourceStream = testClass.getResourceAsStream(configurationFile)) {
            if(resourceStream == null) {
                throw new ParameterResolutionException("Unable to find a configuration file " + configurationFile + " in the " + testClass.getPackageName() + " package.");
            }

            final PipelinesDataflowModelParser pipelinesDataflowModelParser = new PipelinesDataflowModelParser(() -> List.of(resourceStream));
            try {
                pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
            } catch (final ParseException ex) {
                throw new ParameterResolutionException("Failed to parse configuration file " + configurationFile + ".", ex);
            }
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
        return pipelinesDataFlowModel;
    }

    private PluginFactory getOrCreatePluginFactory(final ExtensionContext extensionContext) {
        return TestApplicationContextProvider.get(extensionContext).getBean(PluginFactory.class);
    }
}
