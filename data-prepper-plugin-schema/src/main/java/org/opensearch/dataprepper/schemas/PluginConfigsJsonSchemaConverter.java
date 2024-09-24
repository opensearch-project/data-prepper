package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.Source;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.dataprepper.model.configuration.PipelineModel.BUFFER_PLUGIN_TYPE;
import static org.opensearch.dataprepper.model.configuration.PipelineModel.PROCESSOR_PLUGIN_TYPE;
import static org.opensearch.dataprepper.model.configuration.PipelineModel.ROUTE_PLUGIN_TYPE;
import static org.opensearch.dataprepper.model.configuration.PipelineModel.SINK_PLUGIN_TYPE;
import static org.opensearch.dataprepper.model.configuration.PipelineModel.SOURCE_PLUGIN_TYPE;

public class PluginConfigsJsonSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(PluginConfigsJsonSchemaConverter.class);
    static final String SITE_URL_PLACEHOLDER = "{{site.url}}";
    static final String SITE_BASE_URL_PLACEHOLDER = "{{site.baseurl}}";
    static final String DOCUMENTATION_LINK_KEY = "documentation";
    static final String PLUGIN_NAME_KEY = "name";
    static final String PLUGIN_DOCUMENTATION_URL_FORMAT =
            "%s%s/data-prepper/pipelines/configuration/%s/%s/";
    static final Map<Class<?>, String> PLUGIN_TYPE_TO_URI_PARAMETER_MAP = Map.of(
            Source.class, "sources",
            Processor.class, "processors",
            ConditionalRoute.class, "processors",
            Buffer.class, "buffers",
            Sink.class, "sinks"
    );
    static final String CONDITIONAL_ROUTE_PROCESSOR_NAME = "routes";
    static final Map<String, Class<?>> PLUGIN_TYPE_NAME_TO_CLASS_MAP = Map.of(
            SOURCE_PLUGIN_TYPE, Source.class,
            PROCESSOR_PLUGIN_TYPE, Processor.class,
            ROUTE_PLUGIN_TYPE, ConditionalRoute.class,
            BUFFER_PLUGIN_TYPE, Buffer.class,
            SINK_PLUGIN_TYPE, Sink.class);

    private final String siteUrl;
    private final String siteBaseUrl;
    private final Reflections reflections;
    private final JsonSchemaConverter jsonSchemaConverter;

    public PluginConfigsJsonSchemaConverter(
            final Reflections reflections,
            final JsonSchemaConverter jsonSchemaConverter,
            final String siteUrl,
            final String siteBaseUrl) {
        this.reflections = reflections;
        this.jsonSchemaConverter = jsonSchemaConverter;
        this.siteUrl = siteUrl == null ? SITE_URL_PLACEHOLDER : siteUrl;
        this.siteBaseUrl = siteBaseUrl == null ? SITE_BASE_URL_PLACEHOLDER : siteBaseUrl;
    }

    public Set<String> validPluginTypeNames() {
        return PLUGIN_TYPE_NAME_TO_CLASS_MAP.keySet();
    }

    public Class<?> pluginTypeNameToPluginType(final String pluginTypeName) {
        final Class<?> pluginType = PLUGIN_TYPE_NAME_TO_CLASS_MAP.get(pluginTypeName);
        if (pluginType == null) {
            throw new IllegalArgumentException(String.format("Invalid plugin type name: %s.", pluginTypeName));
        }
        return pluginType;
    }

    public Map<String, String> convertPluginConfigsIntoJsonSchemas(
            final SchemaVersion schemaVersion, final OptionPreset optionPreset, final Class<?> pluginType) {
        final Map<String, Class<?>> nameToConfigClass = scanForPluginConfigs(pluginType);
        return nameToConfigClass.entrySet().stream()
                .flatMap(entry -> {
                    final String pluginName = entry.getKey();
                    String value;
                    try {
                        final ObjectNode jsonSchemaNode = jsonSchemaConverter.convertIntoJsonSchema(
                                schemaVersion, optionPreset, entry.getValue());
                        addPluginName(jsonSchemaNode, pluginName);
                        addDocumentationLink(jsonSchemaNode, pluginName, pluginType);
                        value = jsonSchemaNode.toPrettyString();
                    } catch (final Exception e) {
                        LOG.error("Encountered error retrieving JSON schema for {}", pluginName, e);
                        return Stream.empty();
                    }
                    return Stream.of(Map.entry(entry.getKey(), value));
                })
                .filter(entry -> Objects.nonNull(entry.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    private Map<String, Class<?>> scanForPluginConfigs(final Class<?> pluginType) {
        if (ConditionalRoute.class.equals(pluginType)) {
            return Map.of(CONDITIONAL_ROUTE_PROCESSOR_NAME, ConditionalRoute.class);
        }
        return reflections.getTypesAnnotatedWith(DataPrepperPlugin.class).stream()
                        .map(clazz -> clazz.getAnnotation(DataPrepperPlugin.class))
                        .filter(dataPrepperPlugin -> pluginType.equals(dataPrepperPlugin.pluginType()))
                        .collect(Collectors.toMap(
                                DataPrepperPlugin::name,
                                DataPrepperPlugin::pluginConfigurationType
                        ));
    }

    private void addDocumentationLink(final ObjectNode jsonSchemaNode,
                                      final String pluginName,
                                      final Class<?> pluginType) {
        jsonSchemaNode.put(DOCUMENTATION_LINK_KEY,
                String.format(
                        PLUGIN_DOCUMENTATION_URL_FORMAT,
                        siteUrl,
                        siteBaseUrl,
                        PLUGIN_TYPE_TO_URI_PARAMETER_MAP.get(pluginType),
                        pluginName));
    }

    private void addPluginName(final ObjectNode jsonSchemaNode,
                               final String pluginName) {
        jsonSchemaNode.put(PLUGIN_NAME_KEY, pluginName);
    }
}
