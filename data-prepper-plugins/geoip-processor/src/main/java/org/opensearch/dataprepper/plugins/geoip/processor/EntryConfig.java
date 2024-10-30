/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("Defines a single entry for geolocation.")
public class EntryConfig {
    static final String DEFAULT_TARGET = "geo";

    @JsonPropertyDescription("The key of the source field containing the IP address to geolocate.")
    @JsonProperty("source")
    @NotEmpty
    @ExampleValues({
        @Example(value = "clientip", description = "The processor will extract available geolocation data from the IP address provided in the field named 'clientip'")
    })
    private String source;

    @JsonPropertyDescription("The key of the target field in which to set the geolocation data.")
    @JsonProperty(value = "target", defaultValue = DEFAULT_TARGET)
    @ExampleValues({
        @Example(value = "clientlocation", description = "The processor will put the geolocation data into a field named 'clientlocation'")
    })
    private String target = DEFAULT_TARGET;

    @JsonPropertyDescription("The list of geolocation fields to include in the target object. By default, this is all the fields provided by the configured databases. " +
            "For example, if you wish to only obtain the actual location, you can specify <code>location</code>.")
    @JsonProperty("include_fields")
    @ExampleValues({
        @Example(value = "[asn, asn_organization, network]", description = "The processor will include these fields while extracting geolocation data.")
    })
    private List<String> includeFields;

    @JsonPropertyDescription("The list of geolocation fields to exclude from the target object. " +
            "For example, you can exclude ASN fields by including <code>asn</code>, <code>asn_organization</code>, <code>network</code>, <code>ip</code>.")
    @JsonProperty("exclude_fields")
    @ExampleValues({
        @Example(value = "[asn, asn_organization, network]", description = "The processor will exclude these fields while extracting geolocation data.")
    })
    private List<String> excludeFields;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    List<String> getIncludeFields() {
        return includeFields;
    }

    List<String> getExcludeFields() {
        return excludeFields;
    }

    @AssertTrue(message = "include_fields and exclude_fields are mutually exclusive. include_fields or exclude_fields is required.")
    boolean areFieldsValid() {
        if (includeFields == null && excludeFields == null) {
            return false;
        }
        return includeFields == null || excludeFields == null;
    }

    /**
     * Gets the desired {@link GeoIPField} based on the configuration.
     * @return A collection of {@link GeoIPField}
     */
    @JsonIgnore
    public Collection<GeoIPField> getGeoIPFields() {

        if (includeFields != null && !includeFields.isEmpty()) {
            final EnumSet<GeoIPField> geoIPFields = EnumSet.noneOf(GeoIPField.class);
            for (final String field : includeFields) {
                final GeoIPField geoIPField = GeoIPField.findByName(field);
                if (geoIPField != null) {
                    geoIPFields.add(geoIPField);
                }
            }
            return geoIPFields;
        } else if (excludeFields != null) {
            final EnumSet<GeoIPField> geoIPFields = EnumSet.allOf(GeoIPField.class);
            for (final String field : excludeFields) {
                final GeoIPField geoIPField = GeoIPField.findByName(field);
                if (geoIPField != null) {
                    geoIPFields.remove(geoIPField);
                }
            }
            return geoIPFields;
        }

        return GeoIPField.allFields();

    }
}
