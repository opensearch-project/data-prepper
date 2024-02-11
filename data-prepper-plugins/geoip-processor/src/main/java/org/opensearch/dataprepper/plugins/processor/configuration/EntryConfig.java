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
    private List<String> includeFields = new ArrayList<>();

    @JsonProperty("exclude_fields")
    private List<String> excludeFields = new ArrayList<>();

    @JsonIgnore
    private final List<GeoIPField> geoIPFields = new ArrayList<>();


    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }


    public List<GeoIPField> getFields() {
        if (!geoIPFields.isEmpty()) {
            return geoIPFields;
        }
        if (!includeFields.isEmpty()) {
            for(final String field: includeFields) {
                final GeoIPField geoIPField = GeoIPField.findByName(field);
                if (geoIPField != null) {
                    geoIPFields.add(geoIPField);
                }
            }
            return geoIPFields;
        } else if (!excludeFields.isEmpty()) {
            final List<GeoIPField> excludeGeoIPFields = new ArrayList<>();
            for(final String field: excludeFields) {
                final GeoIPField geoIPField = GeoIPField.findByName(field);
                if (geoIPField != null) {
                    excludeGeoIPFields.add(geoIPField);
                }
            }
            final List<GeoIPField> values = new ArrayList<>(List.of(GeoIPField.values()));
            values.removeAll(excludeGeoIPFields);
            return values;
        }
        return geoIPFields;
    }

    public Set<GeoIPDatabase> getGeoIPDatabases() {
        final Set<GeoIPDatabase> databaseTypes = new HashSet<>();
        for (final GeoIPField geoIPField: getFields()) {
            final Set<GeoIPDatabase> geoIPDatabases = geoIPField.getGeoIPDatabases();
            databaseTypes.addAll(geoIPDatabases);
        }
        return databaseTypes;
    }

    @AssertTrue(message = "include_fields and exclude_fields are mutually exclusive. include_fields or exclude_fields is required with at least one field.")
    boolean areFieldsValid() {
        if (includeFields.isEmpty() && excludeFields.isEmpty()) {
            return false;
        }
        return includeFields.isEmpty() || excludeFields.isEmpty();
    }
}
