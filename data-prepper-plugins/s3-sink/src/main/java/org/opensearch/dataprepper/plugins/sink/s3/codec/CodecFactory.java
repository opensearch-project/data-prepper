package org.opensearch.dataprepper.plugins.sink.s3.codec;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

public class CodecFactory {

    private final PluginFactory pluginFactory;

    private final PluginSetting codecPluginSetting;

    public CodecFactory(final PluginFactory pluginFactory,
                        final PluginModel codecConfiguration) {
        this.pluginFactory = pluginFactory;
        this.codecPluginSetting = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
    }

    public OutputCodec provideCodec() {
        return pluginFactory.loadPlugin(OutputCodec.class, codecPluginSetting);
    }
}
