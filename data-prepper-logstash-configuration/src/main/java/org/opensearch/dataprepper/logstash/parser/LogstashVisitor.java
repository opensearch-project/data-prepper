package org.opensearch.dataprepper.logstash.parser;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.logstash.exception.LogstashParsingException;
import org.opensearch.dataprepper.logstash.LogstashBaseVisitor;
import org.opensearch.dataprepper.logstash.LogstashParser;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * Class to populate Logstash configuration model objects using ANTLR library classes and generated code.
 *
 * @since 1.2
 */
@SuppressWarnings("rawtypes")
class LogstashVisitor extends LogstashBaseVisitor {

    @Override
    public Object visitConfig(final LogstashParser.ConfigContext configContext) {
        final Map<LogstashPluginType, List<LogstashPlugin>> pluginSections = new LinkedHashMap<>();

        configContext.plugin_section().forEach(pluginSection -> {
            final String pluginType = pluginSection.plugin_type().getText();
            if (!Arrays.asList(new String[]{"input", "filter", "output"}).contains(pluginType))
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
        final String pluginName = pluginContext.name().getText();
        final List<LogstashAttribute> logstashAttributeList = pluginContext.attributes().attribute().stream()
                .map(attribute -> (LogstashAttribute) visitAttribute(attribute))
                .collect(Collectors.toList());

        return LogstashPlugin.builder()
                .pluginName(pluginName)
                .attributes(logstashAttributeList)
                .build();
    }

    @Override
    public Object visitAttribute(final LogstashParser.AttributeContext attributeContext) {
        LogstashValueType logstashValueType = null;
        Object value = null;

        if (attributeContext.value().getChild(0) instanceof LogstashParser.ArrayContext) {
            logstashValueType = LogstashValueType.ARRAY;
            value = visitArray(attributeContext.value().array());
        }
        else if (attributeContext.value().getChild(0) instanceof LogstashParser.HashContext) {
            logstashValueType = LogstashValueType.HASH;
            value = visitHash(attributeContext.value().hash());
        }
        else if (attributeContext.value().getChild(0) instanceof LogstashParser.PluginContext) {
            throw new LogstashParsingException("plugins are not supported in an attribute");
        }
        else if (attributeContext.value().NUMBER() != null && attributeContext.value().getText().equals(attributeContext.value().NUMBER().toString())) {
            logstashValueType = LogstashValueType.NUMBER;
            value = Double.valueOf(attributeContext.value().getText());
        }
        else if (attributeContext.value().BAREWORD() != null && attributeContext.value().getText().equals(attributeContext.value().BAREWORD().toString())) {
            logstashValueType = LogstashValueType.BAREWORD;
            value = attributeContext.value().getText();
        }
        else if (attributeContext.value().STRING() != null && attributeContext.value().getText().equals(attributeContext.value().STRING().toString())) {
            logstashValueType = LogstashValueType.STRING;
            value = attributeContext.value().getText().replaceAll("^\"|\"$|^'|'$", "");
        }

        final LogstashAttributeValue logstashAttributeValue =  LogstashAttributeValue.builder()
                .attributeValueType(logstashValueType)
                .value(value)
                .build();

        return LogstashAttribute.builder()
                .attributeName(attributeContext.name().getText())
                .attributeValue(logstashAttributeValue)
                .build();
    }

    @Override
    public Object visitArray(final LogstashParser.ArrayContext arrayContext) {
        return arrayContext.value().stream()
                .map(RuleContext::getText)
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
            final String key = hashentryContext.hashname().getText();
            final Object value = visitHashentry(hashentryContext);
            hashEntries.put(key, value);
        });

        return hashEntries;
    }

    @Override
    public Object visitHashentry(final LogstashParser.HashentryContext hashentryContext) {
        if (hashentryContext.value().getChild(0) instanceof LogstashParser.ArrayContext)
            return visitArray(hashentryContext.value().array());

        return hashentryContext.value().getText();
    }
}