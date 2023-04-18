package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.model.event.Event;

public final class DocumentBuilder {

    public static String build(final Event event, final String documentRootKey) {
        if (documentRootKey != null && event.containsKey(documentRootKey)) {
            final String document = event.getAsJsonString(documentRootKey);
            if (document == null || !document.startsWith("{")) {
                return String.format("{\"data\": %s}", document);
            }
            return document;
        }
        return event.toJsonString();
    }
}
