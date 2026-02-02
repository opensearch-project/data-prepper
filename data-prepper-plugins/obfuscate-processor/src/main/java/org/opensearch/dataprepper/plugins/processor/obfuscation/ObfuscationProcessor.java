/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.obfuscation.action.MaskAction;
import org.opensearch.dataprepper.plugins.processor.obfuscation.action.MaskActionConfig;
import org.opensearch.dataprepper.plugins.processor.obfuscation.action.ObfuscationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.opensearch.dataprepper.model.pattern.Matcher;
import org.opensearch.dataprepper.model.pattern.Pattern;

@DataPrepperPlugin(name = "obfuscate", pluginType = Processor.class, pluginConfigurationType = ObfuscationProcessorConfig.class)
public class ObfuscationProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final String COMMON_PATTERN_REGEX = "^%\\{([A-Z_0-9]+)}$";
    private static final Logger LOG = LoggerFactory.getLogger(ObfuscationProcessor.class);

    private final ExpressionEvaluator expressionEvaluator;
    private final ObfuscationProcessorConfig obfuscationProcessorConfig;

    private final String source;
    private final String target;
    private final boolean singleWordOnly;

    private final List<Pattern> patterns;
    private final ObfuscationAction action;


    @DataPrepperPluginConstructor
    public ObfuscationProcessor(final PluginMetrics pluginMetrics,
                                final ObfuscationProcessorConfig config,
                                final PluginFactory pluginFactory,
                                final ExpressionEvaluator expressionEvaluator) {
        // No special metrics generate by this processor.
        super(pluginMetrics);

        this.source = config.getSource();
        this.target = config.getTarget();
        this.patterns = new ArrayList<>();
        this.expressionEvaluator = expressionEvaluator;
        this.obfuscationProcessorConfig = config;
        this.singleWordOnly = config.getSingleWordOnly();

        config.validateObfuscateWhen(expressionEvaluator);

        final PluginModel actionPlugin = config.getAction();
        if (actionPlugin == null) {
            // Use default action
            this.action = createDefaultAction();
        } else {
            final PluginSetting actionPluginSettings = new PluginSetting(actionPlugin.getPluginName(),
                    actionPlugin.getPluginSettings());
            this.action = pluginFactory.loadPlugin(ObfuscationAction.class, actionPluginSettings);
        }

        if (config.getPatterns() != null) {
            for (String rawPattern : config.getPatterns()) {
                // Get the regex pattern
                // If it's in a format of %{xxx}, look for expr from predefined patterns.
                Pattern pattern = Pattern.compile(COMMON_PATTERN_REGEX);
                Matcher matcher = pattern.matcher(rawPattern);
                if (matcher.matches()) {
                    try {
                        CommonPattern cp = CommonPattern.valueOf(matcher.group(1));
                        // If found, used the common pattern regex
                        rawPattern = cp.getExpr();
                    } catch (IllegalArgumentException e) {
                        LOG.error("Unable to find a predefined pattern for '{}'", matcher.group(1));
                        // Throw exception as the pattern is for sure invalid
                        throw new InvalidPluginConfigurationException("Unable to find a predefined pattern for \"" + rawPattern + "\".");
                    }
                }
                if (singleWordOnly) {
                    rawPattern = "\\b" + rawPattern + "\\b";
                }
                try {
                    Pattern p = Pattern.compile(rawPattern);
                    patterns.add(p);
                } catch (Exception e) {
                    LOG.error(NOISY,e.getMessage());
                    LOG.atError()
                            .addMarker(EVENT)
                            .addMarker(NOISY)
                            .setMessage(e.getMessage())
                            .log();
                    throw new InvalidPluginConfigurationException("Invalid Pattern: \"" + rawPattern + "\" for source field " + this.source);
                }
            }
        }

    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {

        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {

                if (obfuscationProcessorConfig.getObfuscateWhen() != null && !expressionEvaluator.evaluateConditional(obfuscationProcessorConfig.getObfuscateWhen(), recordEvent)) {
                    continue;
                }

                if (!recordEvent.containsKey(source)) {
                    continue;
                }

                String rawValue = recordEvent.get(source, String.class);

                // Call obfuscation action
                String newValue = this.action.obfuscate(rawValue, patterns, record);

                // No changes means it does not match any patterns
                if (rawValue.equals(newValue)) {
                    recordEvent.getMetadata().addTags(obfuscationProcessorConfig.getTagsOnMatchFailure());
                }

                // Update the event record.
                if (target == null || target.isEmpty()) {
                    recordEvent.put(source, newValue);
                } else {
                    recordEvent.put(target, newValue);
                }
            } catch (final Exception e) {
                LOG.error(EVENT, "There was an exception while processing Event [{}]", recordEvent, e);
            }
        }
        return records;
    }

    private ObfuscationAction createDefaultAction() {
        LOG.debug("Create a default mask action");
        final MaskActionConfig config = new MaskActionConfig();
        return new MaskAction(config);
    }


    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {

    }

}
