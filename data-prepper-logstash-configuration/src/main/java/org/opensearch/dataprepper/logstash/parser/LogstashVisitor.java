package org.opensearch.dataprepper.logstash.parser;

import org.opensearch.dataprepper.logstash.exception.LogstashParsingException;
import org.opensearch.dataprepper.logstash.LogstashBaseVisitor;
import org.opensearch.dataprepper.logstash.LogstashParser;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.ArrayList;

/**
 * Class to populate Logstash configuration model objects using ANTLR
 *
 * @since 1.2
 */

@SuppressWarnings("rawtypes")
public class LogstashVisitor extends LogstashBaseVisitor {

    private List<LogstashPlugin> logstashPluginList;
    private final Map<LogstashPluginType, List<LogstashPlugin>> pluginSections = new LinkedHashMap<>();
    private final Map<String, Object> hashEntries = new LinkedHashMap<>();

    @Override
    public Object visitConfig(LogstashParser.ConfigContext configContext) {
        for(int i = 0; i < configContext.plugin_section().size(); i++) {
            visitPlugin_section(configContext.plugin_section().get(i));
        }
        return LogstashConfiguration.builder()
                .pluginSections(pluginSections)
                .build();
    }

    @Override
    public Object visitPlugin_section(LogstashParser.Plugin_sectionContext pluginSectionContext) {

        LogstashPluginType logstashPluginType;
        logstashPluginList = new LinkedList<>();

        switch (pluginSectionContext.plugin_type().getText()) {
            case "input":
                logstashPluginType = LogstashPluginType.INPUT;
                break;
            case "filter":
                logstashPluginType = LogstashPluginType.FILTER;
                break;
            case "output":
                logstashPluginType = LogstashPluginType.OUTPUT;
                break;
            default:
                throw new LogstashParsingException("only input, filter and output plugin sections are supported.");
        }

        for (int i = 0; i < pluginSectionContext.branch_or_plugin().size(); i++) {
            logstashPluginList.add((LogstashPlugin) visitBranch_or_plugin(pluginSectionContext.branch_or_plugin().get(i)));
        }

        pluginSections.put(logstashPluginType, logstashPluginList);

        return pluginSections;
    }

    @Override
    public Object visitBranch_or_plugin(LogstashParser.Branch_or_pluginContext branchOrPluginContext) {

        if (branchOrPluginContext.getChild(0) instanceof LogstashParser.PluginContext) {
            return visitPlugin(branchOrPluginContext.plugin());
        }
        else {
            throw new LogstashParsingException("conditionals are not supported");
        }
    }

    @Override
    public Object visitPlugin(LogstashParser.PluginContext pluginContext) {
        String pluginName = pluginContext.name().getText();
        List<LogstashAttribute> logstashAttributeList = new ArrayList<>();

        for (int i = 0; i < pluginContext.attributes().attribute().size(); i++) {
            logstashAttributeList.add((LogstashAttribute) visitAttribute(pluginContext.attributes().attribute().get(i)));
        }

        return LogstashPlugin.builder()
                .pluginName(pluginName)
                .attributes(logstashAttributeList)
                .build();
    }

    @Override
    public Object visitAttribute(LogstashParser.AttributeContext attributeContext) {
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
            throw new LogstashParsingException("plugins are not supported in attribute");
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

        LogstashAttributeValue logstashAttributeValue =  LogstashAttributeValue.builder()
                .attributeValueType(logstashValueType)
                .value(value)
                .build();

        return LogstashAttribute.builder()
                .attributeName(attributeContext.name().getText())
                .attributeValue(logstashAttributeValue)
                .build();
    }

    @Override
    public Object visitArray(LogstashParser.ArrayContext arrayContext) {
        List<String> values = new LinkedList<>();

        for(int i = 0; i < arrayContext.value().size(); i++)
            values.add(arrayContext.value().get(i).getText() );
        return values;
    }

    @Override
    public Object visitHash(LogstashParser.HashContext hashContext) {
        return visitHashentries(hashContext.hashentries());
    }

    @Override
    public Object visitHashentries(LogstashParser.HashentriesContext hashentriesContext) {
        for (int i = 0; i < hashentriesContext.hashentry().size(); i++) {
            visitHashentry((hashentriesContext.hashentry().get(i)));
        }
        return hashEntries;
    }

    @Override
    public Object visitHashentry(LogstashParser.HashentryContext hashentryContext) {

        if (hashentryContext.value().getChild(0) instanceof LogstashParser.ArrayContext)
            hashEntries.put(hashentryContext.hashname().getText(), visitArray(hashentryContext.value().array()));

        else
            hashEntries.put(hashentryContext.hashname().getText(), hashentryContext.value().getText());

        return hashEntries;
    }
}