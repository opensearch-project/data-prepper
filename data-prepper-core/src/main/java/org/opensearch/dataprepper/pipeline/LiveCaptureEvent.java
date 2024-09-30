/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import org.opensearch.dataprepper.model.event.Event;
import org.apache.commons.text.StringEscapeUtils;

import java.time.Instant;
import java.util.Map;

public class LiveCaptureEvent {
    public static final int CURRENT_VERSION = 1;
    private final int version;
    private final String description;
    private final Instant time;
    private final Event event;

    public LiveCaptureEvent(final String description, final Instant time, final Event event) {
        this.version = CURRENT_VERSION;
        this.description = description;
        this.time = time;
        this.event = event;
    }

    public Map<String, Object> toMap() {
        return Map.of("version", version, "description", description, "time", time, "event", event.toMap());
    }

    public String toJsonString() {
        return "{\"version\": " + version + ", \"description\": \"" + description +"\", \"time\": \""+time+"\", \"event\":\""+StringEscapeUtils.escapeJava(event.toJsonString())+"\"}"; 
    }

}
