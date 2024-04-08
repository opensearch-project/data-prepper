package org.opensearch.dataprepper.pipeline.parser.rule;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.MapContext;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RuleEvaluator {
    private final JexlEngine jexlEngine;
    private final String templateFileNamePattern = "-template.yaml";

    public RuleEvaluator() {
        this.jexlEngine = new JexlBuilder().create();
    }

    public boolean isRuleValid(RuleConfig rule, PipelinesDataFlowModel model) {
        String expressionStr = rule.getApplyWhen();
        JexlExpression expression = jexlEngine.createExpression(expressionStr);
        JexlContext context = new MapContext();

        // Assuming `pipeline` is a simple Map that JEXL can navigate
        context.set("pipeline", model);

        // Evaluate the expression
        Object result = expression.evaluate(context);

        if (result instanceof Boolean) {
            return (Boolean) result;
        }

        throw new IllegalArgumentException("Rule expression did not result in a boolean value.");
    }

    public String getTemplateFileLocationForTransformation(RuleConfig rule){

        String expressionStr = rule.getApplyWhen();
        final String pluginName = getPluginNameThatNeedsTransformation(expressionStr);
        final String templateFileName = pluginName+templateFileNamePattern;

        //TODO - scan for templateFileName in class path
        String templateFilePath = "src/resources/" + pluginName + "/templates/" + templateFileName;
        return templateFilePath;
    }

    /**
     * Assumption: The rule is always of this format: "(pipeline.<source/processor>.PluginName)"
     * 
     */
    private String getPluginNameThatNeedsTransformation(String expressionStr) {

        //checking for rule pattern
        Pattern pattern = Pattern.compile("pipeline\\.(.*?)\\.(.*)\\)");
        Matcher matcher = pattern.matcher(expressionStr);

        if (matcher.find()) {
            // Extract the 'PluginName' part
            String pluginName = matcher.group(2);

            return pluginName;
        } else {
            throw new RuntimeException("Invalid rule expression format: " + expressionStr);
        }

    }   

}

