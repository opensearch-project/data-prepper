package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

public final class DocumentBuilder {

    public static String build(final Event event, final String documentRootKey, final String tagsTargetKey, final List<String> includeKeys, final List<String> excludeKeys) {
        final String document = event.jsonBuilder()
                .rootKey(documentRootKey)
                .includeKeys(includeKeys)
                .excludeKeys(excludeKeys)
                .includeTags(tagsTargetKey)
                .toJsonString();

        if (document == null || !document.startsWith("{")) {
            return String.format("{\"data\": %s}", document);
        }
        return document;
    }

    public static String build(final Event event, final String documentRootKey, final String tagsTargetKey) {
        return build(event, documentRootKey, tagsTargetKey, null, null);
    }
}
