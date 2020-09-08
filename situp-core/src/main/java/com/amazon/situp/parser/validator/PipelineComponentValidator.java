package com.amazon.situp.parser.validator;

import com.amazon.situp.model.configuration.Configuration;
import com.amazon.situp.model.configuration.PluginSetting;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.List;


public class PipelineComponentValidator implements ConstraintValidator<PipelineComponent, Configuration> {
    private PipelineComponent.Type type;

    @Override
    public void initialize(PipelineComponent pipelineComponent) {
        this.type = pipelineComponent.type();
    }

    @Override
    public boolean isValid(Configuration value, ConstraintValidatorContext context) {
        switch (type) {
            case Source:
                return isValidSource(value);
            case Buffer:
                return isValidBuffer(value);
            case Processor:
                return isValidProcessor(value);
            case Sink:
                return isValidSink(value);
            default:
                throw new IllegalArgumentException(String.format("Found invalid configuration type [%s]", type));
        }
    }

    /**
     * Currently we allow default plugins without any settings required - only checking for name.
     */
    private boolean isValidPluginSetting(final PluginSetting pluginSetting) {
        String pluginName = pluginSetting.getName();
        return pluginName != null && !pluginName.isEmpty();
    }

    /**
     * Valid Source has exactly one plugin with valid name and optional attribute map.
     */
    private boolean isValidSource(final Configuration configuration) {
        return isValidConfiguration(configuration, 1, 1);
    }

    /**
     * Valid Buffer setting is either no buffer or only one buffer.
     */
    private boolean isValidBuffer(final Configuration configuration) {
        return isValidConfiguration(configuration, 0, 1);
    }

    /**
     * Valid Processor configuration is either no processor or any number of processors
     * TODO: Should we limit maximum number of processors that can be configured
     */
    private boolean isValidProcessor(final Configuration configuration) {
        return isValidConfiguration(configuration, 0, Integer.MAX_VALUE);
    }

    /**
     * Valid Sink configuration is at least 1
     */
    private boolean isValidSink(final Configuration configuration) {
        return isValidConfiguration(configuration, 1, Integer.MAX_VALUE);
    }

    private boolean isValidConfiguration(final Configuration configuration, int minPlugins, int maxPlugins) {
        final List<PluginSetting> pluginSettings = configuration.getPluginSettings();
        if (pluginSettings == null || pluginSettings.size() < minPlugins || pluginSettings.size() > maxPlugins) {
            return false;
        }
        for (final PluginSetting pluginSetting : pluginSettings) {
            if (!isValidPluginSetting(pluginSetting)) {
                return false;
            }
        }
        return true;
    }
}
