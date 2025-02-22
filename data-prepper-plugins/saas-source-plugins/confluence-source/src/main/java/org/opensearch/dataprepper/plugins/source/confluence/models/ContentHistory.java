package org.opensearch.dataprepper.plugins.source.confluence.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Setter
@Getter
public class ContentHistory {

    // Example format "createdDate": "2025-02-17T23:34:44.633Z"
    @JsonProperty("createdDate")
    String createdDate;

    @JsonProperty("lastUpdated")
    LastUpdated lastUpdated;

    /**
     * Converts the createdDate ISO 8601 timestamp to milliseconds since epoch
     *
     * @return milliseconds since epoch, or 0 if createdDate is null or invalid
     */
    public long getCreatedDateInMillis() {
        try {
            if (createdDate != null) {
                return Instant.parse(createdDate).toEpochMilli();
            }
        } catch (Exception e) {
            return 0L;
        }
        return 0L;
    }

    public long getLastUpdatedInMillis() {
        try {
            if (lastUpdated != null && lastUpdated.when != null) {
                return Instant.parse(lastUpdated.when).toEpochMilli();
            }
        } catch (Exception e) {
            return 0L;
        }
        return 0L;
    }

    @Setter
    @Getter
    public static class LastUpdated {
        // Example format  "when": "2025-02-17T23:34:44.633Z"
        @JsonProperty("when")
        String when;
    }


}
