package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.ObjectBuilderDeserializer;
import org.opensearch.client.json.ObjectDeserializer;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.Alias;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;

/**
 * Utility class for deserializers of {@link PutTemplateRequest}. The built-in deserializer is buggy:
 * https://github.com/opensearch-project/opensearch-java/issues/417
 */
public class PutTemplateRequestDeserializer {
    private static final JsonpDeserializer<PutTemplateRequest> JSONP_DESERIALIZER = ObjectBuilderDeserializer
            .lazy(
                    PutTemplateRequest.Builder::new,
                    PutTemplateRequestDeserializer::setupPutTemplateRequestDeserializer);

    private static void setupPutTemplateRequestDeserializer(ObjectDeserializer<PutTemplateRequest.Builder> op) {

        op.add(PutTemplateRequest.Builder::name, JsonpDeserializer.stringDeserializer(), "name");
        op.add(PutTemplateRequest.Builder::aliases, JsonpDeserializer.stringMapDeserializer(Alias._DESERIALIZER), "aliases");
        op.add(PutTemplateRequest.Builder::indexPatterns, JsonpDeserializer.arrayDeserializer(JsonpDeserializer.stringDeserializer()),
                "index_patterns");
        op.add(PutTemplateRequest.Builder::mappings, TypeMapping._DESERIALIZER, "mappings");
        op.add(PutTemplateRequest.Builder::order, JsonpDeserializer.integerDeserializer(), "order");
        op.add(PutTemplateRequest.Builder::settings, JsonpDeserializer.stringMapDeserializer(JsonData._DESERIALIZER), "settings");
        op.add(PutTemplateRequest.Builder::version, JsonpDeserializer.longDeserializer(), "version");
    }

    public static JsonpDeserializer<PutTemplateRequest> getJsonpDeserializer() {
        return JSONP_DESERIALIZER;
    }
}
