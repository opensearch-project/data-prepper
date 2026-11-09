/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.mongo.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class MongoDBSourceConfigTest {

    private MongoDBSourceConfig objectUnderTest;

    @BeforeEach
    void setup() {
        objectUnderTest = new MongoDBSourceConfig();
    }

    @Test
    void test_default_empty_scan_delay_returns_zero() {
        assertThat(objectUnderTest.getEmptyScanDelay(), equalTo(Duration.ZERO));
        assertThat(objectUnderTest.isEmptyScanDelayValid(), equalTo(true));
    }

    @Test
    void test_empty_scan_delay_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        setField(MongoDBSourceConfig.class, objectUnderTest, "emptyScanDelay", Duration.ofSeconds(60));
        assertThat(objectUnderTest.getEmptyScanDelay(), equalTo(Duration.ofSeconds(60)));
        assertThat(objectUnderTest.isEmptyScanDelayValid(), equalTo(true));
    }

    @Test
    void test_empty_scan_delay_returns_zero_when_null() throws NoSuchFieldException, IllegalAccessException {
        setField(MongoDBSourceConfig.class, objectUnderTest, "emptyScanDelay", null);
        assertThat(objectUnderTest.getEmptyScanDelay(), equalTo(Duration.ZERO));
    }

    @Test
    void test_empty_scan_delay_at_max_boundary_is_valid() throws NoSuchFieldException, IllegalAccessException {
        setField(MongoDBSourceConfig.class, objectUnderTest, "emptyScanDelay", Duration.ofMinutes(30));
        assertThat(objectUnderTest.isEmptyScanDelayValid(), equalTo(true));
    }

    @Test
    void test_empty_scan_delay_over_max_is_invalid() throws NoSuchFieldException, IllegalAccessException {
        setField(MongoDBSourceConfig.class, objectUnderTest, "emptyScanDelay", Duration.ofMinutes(30).plusSeconds(1));
        assertThat(objectUnderTest.isEmptyScanDelayValid(), equalTo(false));
    }

    @Test
    void test_empty_scan_delay_negative_is_invalid() throws NoSuchFieldException, IllegalAccessException {
        setField(MongoDBSourceConfig.class, objectUnderTest, "emptyScanDelay", Duration.ofSeconds(-1));
        assertThat(objectUnderTest.isEmptyScanDelayValid(), equalTo(false));
    }
}
