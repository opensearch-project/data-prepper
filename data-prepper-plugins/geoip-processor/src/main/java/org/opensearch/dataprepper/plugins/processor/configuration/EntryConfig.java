/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import org.opensearch.dataprepper.plugins.processor.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.processor.GeoIPField;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EntryConfig {
    static final String DEFAULT_TARGET = "geo";
    @JsonProperty("source")
    @NotEmpty
    private String source;

    @JsonProperty("target")
    private String target = DEFAULT_TARGET;

    @JsonProperty("include_fields")
    private List<String> includeFields;

    @JsonProperty("exclude_fields")
    private List<String> excludeFields;

    @JsonIgnore
    private List<GeoIPField> geoIPFields;

    @JsonIgnore
    private Set<GeoIPDatabase> geoIPDatabasesToUse;


    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }


    public List<GeoIPField> getFields() {
        if (geoIPFields != null) {
            return geoIPFields;
        }
        geoIPFields = new ArrayList<>();
        if (includeFields != null) {
            for(final String field: includeFields) {
                final GeoIPField geoIPField = GeoIPField.findByName(field);
                if (geoIPField != null) {
                    geoIPFields.add(geoIPField);
                }
            }
            return geoIPFields;
        } else if (excludeFields != null) {
            final List<GeoIPField> excludeGeoIPFields = new ArrayList<>();
            for(final String field: excludeFields) {
                final GeoIPField geoIPField = GeoIPField.findByName(field);
                if (geoIPField != null) {
                    excludeGeoIPFields.add(geoIPField);
                }
            }
            geoIPFields = new ArrayList<>(List.of(GeoIPField.values()));
            geoIPFields.removeAll(excludeGeoIPFields);
            return geoIPFields;
        }
        return geoIPFields;
    }

    public Set<GeoIPDatabase> getGeoIPDatabases() {
        if (geoIPDatabasesToUse != null) {
            return geoIPDatabasesToUse;
        }
        geoIPDatabasesToUse = new HashSet<>();
        for (final GeoIPField geoIPField: getFields()) {
            final Set<GeoIPDatabase> geoIPDatabases = geoIPField.getGeoIPDatabases();
            geoIPDatabasesToUse.addAll(geoIPDatabases);
        }
        return geoIPDatabasesToUse;
    }

    @AssertTrue(message = "include_fields and exclude_fields are mutually exclusive. include_fields or exclude_fields is required.")
    boolean areFieldsValid() {
        if (includeFields == null && excludeFields == null) {
            return false;
        }
        return includeFields == null || excludeFields == null;
    }
}
