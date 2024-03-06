/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class GeoIPFieldTest {

    @Test
    void test_findByName_should_return_geoip_field_if_valid() {
        final GeoIPField geoIPField = GeoIPField.findByName("city_name");
        assertThat(geoIPField, equalTo(GeoIPField.CITY_NAME));
    }

    @Test
    void test_findByName_should_return_null_if_invalid() {
        final GeoIPField geoIPField = GeoIPField.findByName("coordinates");
        assertThat(geoIPField, equalTo(null));
    }
}