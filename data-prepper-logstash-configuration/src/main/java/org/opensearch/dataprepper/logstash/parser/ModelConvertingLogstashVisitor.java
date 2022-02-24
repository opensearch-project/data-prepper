/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.parser;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.logstash.LogstashBaseVisitor;
import org.opensearch.dataprepper.logstash.LogstashParser;
import org.opensearch.dataprepper.logstash.exception.LogstashParsingException;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Class to populate Logstash configuration model objects using ANTLR library classes and generated code.
 *
 * @since 1.2
 */
@SuppressWarnings("rawtypes")
public class ModelConvertingLogstashVisitor extends LogstashBaseVisitor {

    @Override
    public Object visitConfig(final LogstashParser.ConfigContext configContext) {
        final Map<LogstashPluginType, List<LogstashPlugin>> pluginSections = new LinkedHashMap<>();

        configContext.plugin_section().forEach(pluginSection -> {
            final String pluginType = pluginSection.plugin_type().getText();
            if (!Arrays.asList("input", "filter", "output").contains(pluginType))
                throw new LogstashParsingException("only input, filter and output plugin sections are supported.");
            final LogstashPluginType logstashPluginType = LogstashPluginType.getByValue(pluginType);
            final List<LogstashPlugin> logstashPluginList = (List<LogstashPlugin>) visitPlugin_section(pluginSection);
            pluginSections.put(logstashPluginType, logstashPluginList);
        });

        return LogstashConfiguration.builder()
                .pluginSections(pluginSections)
                .build();
    }

    @Override
    public Object visitPlugin_section(final LogstashParser.Plugin_sectionContext pluginSectionContext) {

        return pluginSectionContext.branch_or_plugin().stream()
                .map(this::visitBranch_or_plugin)
                .collect(Collectors.toList());
    }

    @Override
    public Object visitBranch_or_plugin(final LogstashParser.Branch_or_pluginContext branchOrPluginContext) {

        if (branchOrPluginContext.getChild(0) instanceof LogstashParser.PluginContext)
            return visitPlugin(branchOrPluginContext.plugin());
        else
            throw new LogstashParsingException("conditionals are not supported");
    }

    @Override
    public Object visitPlugin(final LogstashParser.PluginContext pluginContext) {
        final String pluginName = normalizeText(pluginContext.name().getText());
        final List<LogstashAttribute> logstashAttributeList = pluginContext.attributes().attribute().stream()
                .map(attribute -> (LogstashAttribute) visitAttribute(attribute))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return LogstashPlugin.builder()
                .pluginName(pluginName)
                .attributes(logstashAttributeList)
                .build();
    }

    private class AttributeInformation {
        LogstashValueType logstashValueType = null;
        Object value = null;
    }

    @Override
    public Object visitAttribute(final LogstashParser.AttributeContext attributeContext) {
        AttributeInformation info = getAttributeInformation(attributeContext.value());
        if (info == null) return null;

        final LogstashAttributeValue logstashAttributeValue =  LogstashAttributeValue.builder()
                .attributeValueType(info.logstashValueType)
                .value(info.value)
                .build();

        return LogstashAttribute.builder()
                .attributeName(normalizeText(attributeContext.name().getText()))
                .attributeValue(logstashAttributeValue)
                .build();
    }

    @Override
    public Object visitArray(final LogstashParser.ArrayContext arrayContext) {
        return arrayContext.value().stream()
                .map(RuleContext::getText)
                .map(this::normalizeText)
                .collect(Collectors.toList());
    }

    @Override
    public Object visitHash(final LogstashParser.HashContext hashContext) {
        return visitHashentries(hashContext.hashentries());
    }

    @Override
    public Object visitHashentries(final LogstashParser.HashentriesContext hashentriesContext) {
        final Map<String, Object> hashEntries = new LinkedHashMap<>();

        hashentriesContext.hashentry().forEach(hashentryContext -> {
            final String key = normalizeText(hashentryContext.hashname().getText());
            final Object value = visitHashentry(hashentryContext);
            hashEntries.put(key, value);
        });

        return hashEntries;
    }

    @Override
    public Object visitHashentry(final LogstashParser.HashentryContext hashentryContext) {
        if (hashentryContext.value().getChild(0) instanceof LogstashParser.ArrayContext)
            return visitArray(hashentryContext.value().array());

        AttributeInformation info = getAttributeInformation(hashentryContext.value());

        if(info.value == null) {
            info.value = normalizeText(hashentryContext.value().getText());
        }

        return info.value;
    }

    private String normalizeText(final String unNormalizedLogstashText) {
        return unNormalizedLogstashText.replaceAll("^\"|\"$|^'|'$", "");
    }

    private AttributeInformation getAttributeInformation(LogstashParser.ValueContext value) {
        AttributeInformation info = new AttributeInformation();

        if(value == null)
            return null;

        if (value.getChild(0) instanceof LogstashParser.ArrayContext) {
            info.logstashValueType = LogstashValueType.ARRAY;
            info.value = visitArray(value.array());
        }
        else if (value.getChild(0) instanceof LogstashParser.HashContext) {
            info.logstashValueType = LogstashValueType.HASH;
            info.value = visitHash(value.hash());
        }
        else if (value.getChild(0) instanceof LogstashParser.PluginContext) {
            throw new LogstashParsingException("plugins are not supported in an attribute");
        }
        else if (value.NUMBER() != null && value.getText().equals(value.NUMBER().toString())) {
            info.logstashValueType = LogstashValueType.NUMBER;
            try {
                info.value = Integer.parseInt(value.getText());
            } catch (NumberFormatException e) {
                try {
                    info.value = Double.parseDouble(value.getText());
                } catch (NumberFormatException exception) {
                    throw new LogstashParsingException("NUMBER types must be either Integer or Double");
                }
            }
        }
        else if (value.BAREWORD() != null && value.getText().equals(value.BAREWORD().toString())) {
            info.logstashValueType = LogstashValueType.BAREWORD;
            info.value = value.getText();
            if (info.value.toString().equals("true") || info.value.toString().equals("false")) {
                info.value = Boolean.parseBoolean(info.value.toString());
            }
        }
        else if (value.STRING() != null && value.getText().equals(value.STRING().toString())) {
            info.logstashValueType = LogstashValueType.STRING;
            info.value = normalizeText(value.getText());
        }

        return info;
    }
}