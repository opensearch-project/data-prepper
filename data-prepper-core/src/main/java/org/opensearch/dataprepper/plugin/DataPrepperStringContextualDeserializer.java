package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.opensearch.dataprepper.plugins.aws.PluginConfigValueTranslator;

import java.io.IOException;
import java.lang.annotation.Annotation;

public class DataPrepperStringContextualDeserializer extends JsonDeserializer<String> implements ContextualDeserializer {

    private final PluginConfigValueTranslator pluginConfigValueTranslator;

    public DataPrepperStringContextualDeserializer(final PluginConfigValueTranslator pluginConfigValueTranslator) {
        this.pluginConfigValueTranslator = pluginConfigValueTranslator;
    }

    @Override
    public JsonDeserializer<String> createContextual(final DeserializationContext ctxt, final BeanProperty property) {
        final Annotation annotation = property.getAnnotation(SupportSecretString.class);
        if (annotation != null) {
            return this;
        } else {
            return new StringDeserializer();
        }
    }

    @Override
    public String deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
        return pluginConfigValueTranslator.translate(p.getText());
    }
}
