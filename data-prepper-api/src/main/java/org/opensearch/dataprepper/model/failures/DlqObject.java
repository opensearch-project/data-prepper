/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.failures;


import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A model representing DLQ objects in Data Prepper
 *
 * @since 2.2
 */
public class DlqObject {
    private static final Logger LOG = LoggerFactory.getLogger(DlqObject.class);
    private static final String ISO8601_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(ISO8601_FORMAT_STRING)
            .withZone(ZoneId.systemDefault());;

    private final String pluginId;

    private final String pluginName;

    private final String pipelineName;

    private final Object failedData;

    private final String timestamp;

    // DLQ object is created either with EventHandles or event.
    @JsonIgnore
    private final Event event;
    @JsonIgnore
    private final List<EventHandle> eventHandles;

    private DlqObject(final String pluginId, final String pluginName, final String pipelineName,
                      final String timestamp, final Object failedData, final List<EventHandle> eventHandles, final Event event) {

        checkNotNull(pluginId, "pluginId cannot be null");
        checkArgument(!pluginId.isEmpty(), "pluginId cannot be an empty string");
        checkNotNull(pluginName, "pluginName cannot be null");
        checkArgument(!pluginName.isEmpty(), "pluginName cannot be an empty string");
        checkNotNull(pipelineName, "pipelineName cannot be null");
        checkArgument(!pipelineName.isEmpty(), "pipelineName cannot be an empty string");
        checkNotNull(failedData, "failedData cannot be null");
        checkArgument((eventHandles == null || event == null), "Only one of eventhandles and event should be non-null");

        this.pluginId = pluginId;
        this.pluginName = pluginName;
        this.pipelineName = pipelineName;
        this.failedData = failedData;
        this.eventHandles = eventHandles;
        this.event = event;

        this.timestamp = StringUtils.isEmpty(timestamp) ? FORMATTER.format(Instant.now()) : timestamp;
    }
    
    public String getPluginId() {
        return pluginId;
    }
    
    public String getPluginName() {
        return pluginName;
    }
    
    public String getPipelineName() {
        return pipelineName;
    }
    
    public Object getFailedData() {
        return failedData;
    }
    
    public String getTimestamp() {
        return timestamp;
    }

    public Event getEvent() {
        return event;
    }

    public List<EventHandle> getEventHandles() {
        return eventHandles;
    }

    public void releaseEventHandle(boolean result) {
        if (event != null) {
            // This should not happen. DLQ objects with event should be sent to DLQ pipeline and should not be released.
            LOG.warn(NOISY, "Attempted to release DLQObject with event");
        } else if (eventHandles != null && eventHandles.size() == 1) {
            eventHandles.get(0).release(result);
        }
    }

    public void releaseEventHandles(boolean result) {
        if (eventHandles == null) {
            if (event != null) {
                // This should not happen. DLQ objects with event should be sent to DLQ pipeline and should not be released.
                LOG.warn(NOISY, "Attempted to release DLQObject with event");
            }
            return;
        }
        for (final EventHandle eventHandle: eventHandles) {
            eventHandle.release(result);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DlqObject that = (DlqObject) o;
        return Objects.equals(failedData, that.getFailedData())
                && Objects.equals(pluginId, that.pluginId)
                && Objects.equals(pluginName, that.pluginName)
                && Objects.equals(pipelineName, that.pipelineName)
                && Objects.equals(eventHandles, that.eventHandles)
                && Objects.equals(event, that.event)
                && Objects.equals(timestamp, that.getTimestamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(pluginId, pluginName, pipelineName, timestamp, failedData);
    }

    @Override
    public String toString() {
        return "DlqObject{" +
            "pluginId='" + pluginId + '\'' +
            ", pluginName='" + pluginName + '\'' +
            ", pipelineName='" + pipelineName + '\'' +
            ", timestamp='" + timestamp + '\'' +
            ", failedData=" + failedData +
            '}';
    }

    public static DlqObject createDlqObject(PluginSetting pluginSetting, List<EventHandle> eventHandles, Object failedData) {
        return DlqObject.builder()
                .withEventHandles(eventHandles)
                .withEvent(null)
                .withFailedData(failedData)
                .withPluginName(pluginSetting.getName())
                .withPipelineName(pluginSetting.getPipelineName())
                .withPluginId(pluginSetting.getName())
                .build();
    }

    public static DlqObject createDlqObject(PluginSetting pluginSetting, Event event, Object failedData) {
        return DlqObject.builder()
                .withEventHandles(null)
                .withEvent(event)
                .withFailedData(failedData)
                .withPluginName(pluginSetting.getName())
                .withPipelineName(pluginSetting.getPipelineName())
                .withPluginId(pluginSetting.getName())
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {

        private String pluginId;
        private String pluginName;
        private String pipelineName;
        private Object failedData;
        private List<EventHandle> eventHandles;
        private Event event;

        private String timestamp;

        public Builder withPluginId(final String pluginId) {
            this.pluginId = pluginId;
            return this;
        }

        public Builder withPluginName(final String pluginName) {
            this.pluginName = pluginName;
            return this;
        }

        public Builder withPipelineName(final String pipelineName) {
            this.pipelineName = pipelineName;
            return this;
        }

        public Builder withFailedData(final Object failedData) {
            this.failedData = failedData;
            return this;
        }

        public Builder withTimestamp(final String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withEventHandles(final List<EventHandle> eventHandles) {
            this.eventHandles = eventHandles;
            return this;
        }

        public Builder withEvent(final Event event) {
            this.event = event;
            return this;
        }
        public Builder withEventHandle(final EventHandle eventHandle) {
            this.eventHandles = new ArrayList<>();
            this.eventHandles.add(eventHandle);
            return this;
        }

        public Builder withTimestamp(final Instant instant) {
            this.timestamp = FORMATTER.format(instant);
            return this;
        }

        public DlqObject build() {
            return new DlqObject(this.pluginId, this.pluginName, this.pipelineName, this.timestamp, this.failedData, this.eventHandles, this.event);
        }

    }
}
