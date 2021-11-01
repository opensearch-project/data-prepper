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
 * Class to populate Logstash configuration model POJO's using ANTLR
 *
 * @since 1.2
 */

@SuppressWarnings("rawtypes")
public class LogstashVisitor extends LogstashBaseVisitor {

    private List<LogstashPlugin> logstashPluginList;
    private final Map<LogstashPluginType, List<LogstashPlugin>> pluginSections = new LinkedHashMap<>();
    private final Map<String, Object> hashEntries = new LinkedHashMap<>();

    @Override
    public Object visitConfig(LogstashParser.ConfigContext ctx) {
        for(int i = 0; i < ctx.plugin_section().size(); i++) {
            visitPlugin_section(ctx.plugin_section().get(i));
        }
        return LogstashConfiguration.builder()
                .pluginSections(pluginSections)
                .build();
    }

    @Override
    public Object visitPlugin_section(LogstashParser.Plugin_sectionContext ctx) {

        LogstashPluginType logstashPluginType;
        logstashPluginList = new LinkedList<>();

        switch (ctx.plugin_type().getText()) {
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

        for (int i = 0; i < ctx.branch_or_plugin().size(); i++) {
            logstashPluginList.add((LogstashPlugin) visitBranch_or_plugin(ctx.branch_or_plugin().get(i)));
        }

        pluginSections.put(logstashPluginType, logstashPluginList);

        return pluginSections;
    }

    @Override
    public Object visitBranch_or_plugin(LogstashParser.Branch_or_pluginContext ctx) {

        if (ctx.getChild(0) instanceof LogstashParser.PluginContext) {
            return visitPlugin(ctx.plugin());
        }
        else {
            throw new LogstashParsingException("conditionals are not supported");
        }
    }

    @Override
    public Object visitPlugin(LogstashParser.PluginContext ctx) {
        String pluginName = ctx.name().getText();
        List<LogstashAttribute> logstashAttributeList = new ArrayList<>();

        for (int i = 0; i < ctx.attributes().attribute().size(); i++) {
            logstashAttributeList.add((LogstashAttribute) visitAttribute(ctx.attributes().attribute().get(i)));
        }

        return LogstashPlugin.builder()
                .pluginName(pluginName)
                .attributes(logstashAttributeList)
                .build();
    }

    @Override
    public Object visitAttribute(LogstashParser.AttributeContext ctx) {
        LogstashValueType logstashValueType = null;
        Object value = null;

        if (ctx.value().getChild(0) instanceof LogstashParser.ArrayContext) {
            logstashValueType = LogstashValueType.ARRAY;
            value = visitArray(ctx.value().array());
        }
        else if (ctx.value().getChild(0) instanceof LogstashParser.HashContext) {
            logstashValueType = LogstashValueType.HASH;
            value = visitHash(ctx.value().hash());
        }
        else if (ctx.value().getChild(0) instanceof LogstashParser.PluginContext) {
            throw new LogstashParsingException("plugins are not supported in attribute");
        }

        else if (ctx.value().NUMBER() != null && ctx.value().getText().equals(ctx.value().NUMBER().toString())) {
            logstashValueType = LogstashValueType.NUMBER;
            value = Double.valueOf(ctx.value().getText());
        }
        else if (ctx.value().BAREWORD() != null && ctx.value().getText().equals(ctx.value().BAREWORD().toString())) {
            logstashValueType = LogstashValueType.BAREWORD;
            value = ctx.value().getText();
        }
        else if (ctx.value().STRING() != null && ctx.value().getText().equals(ctx.value().STRING().toString())) {
            logstashValueType = LogstashValueType.STRING;
            value = ctx.value().getText().replaceAll("^\"|\"$|^'|'$", "");
        }

        LogstashAttributeValue logstashAttributeValue =  LogstashAttributeValue.builder()
                .attributeValueType(logstashValueType)
                .value(value)
                .build();

        return LogstashAttribute.builder()
                .attributeName(ctx.name().getText())
                .attributeValue(logstashAttributeValue)
                .build();
    }

    @Override
    public Object visitArray(LogstashParser.ArrayContext ctx) {
        List<String> values = new LinkedList<>();

        for(int i = 0; i < ctx.value().size(); i++)
            values.add(ctx.value().get(i).getText() );
        return values;
    }

    @Override
    public Object visitHash(LogstashParser.HashContext ctx) {
        return visitHashentries(ctx.hashentries());
    }

    @Override
    public Object visitHashentries(LogstashParser.HashentriesContext ctx) {
        for (int i = 0; i < ctx.hashentry().size(); i++) {
            visitHashentry((ctx.hashentry().get(i)));
        }
        return hashEntries;
    }

    @Override
    public Object visitHashentry(LogstashParser.HashentryContext ctx) {

        if (ctx.value().getChild(0) instanceof LogstashParser.ArrayContext)
            hashEntries.put(ctx.hashname().getText(), visitArray(ctx.value().array()));

        else
            hashEntries.put(ctx.hashname().getText(), ctx.value().getText());

        return hashEntries;
    }
}