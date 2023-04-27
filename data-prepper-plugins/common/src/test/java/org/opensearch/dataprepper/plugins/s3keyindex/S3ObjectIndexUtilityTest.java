/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3keyindex;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class S3ObjectIndexUtilityTest {

    @Test
    void testObjectDateTimePatterns_not_equal() throws IllegalArgumentException {

        String expectedYearMonthDateFormatter = S3ObjectIndexUtility.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        String actualYearMonthDateFormatter = S3ObjectIndexUtility.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        assertFalse(actualYearMonthDateFormatter.contains(expectedYearMonthDateFormatter));
    }
    
    @Test
    void test_getObjectPathPrefix_equal() throws IllegalArgumentException {

        String expectedYearFormatter = S3ObjectIndexUtility.getObjectPathPrefix("events-%{yyyy}");
        String actualYearFormatter = S3ObjectIndexUtility.getObjectPathPrefix("events-%{yyyy}");
        assertTrue(actualYearFormatter.contains(expectedYearFormatter));
    }

    @Test
    void test_objectTimePattern_Exceptional_time_TooGranular() throws IllegalArgumentException {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndexUtility.validateAndGetDateTimeFormatter("events-%{yyyy-AA-dd}");
        });
    }

    @Test
    void test_objectTimePatterns_equal() throws IllegalArgumentException {

        DateTimeFormatter expectedTimeFormatter = S3ObjectIndexUtility.validateAndGetDateTimeFormatter("events-%{yyyy-MM-dd}");
        DateTimeFormatter actualTimeFormatter = S3ObjectIndexUtility.validateAndGetDateTimeFormatter("events-%{yyyy-MM-dd}");
        assertEquals(actualTimeFormatter.toString(), expectedTimeFormatter.toString());
    }

    @Test
    void test_utc_current_time() throws IllegalArgumentException {

        ZonedDateTime expectedUtcTime = S3ObjectIndexUtility.getCurrentUtcTime();
        ZonedDateTime actualUtcTime = S3ObjectIndexUtility.getCurrentUtcTime();

        assertEquals(expectedUtcTime.getDayOfYear(), actualUtcTime.getDayOfYear());
        assertEquals(expectedUtcTime.getDayOfMonth(), actualUtcTime.getDayOfMonth());
        assertEquals(expectedUtcTime.getDayOfWeek(), actualUtcTime.getDayOfWeek());
    }

    @Test
    void test_objectTimePattern_Exceptional_TooGranular() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndexUtility.validateAndGetDateTimeFormatter("events-%{yyyy-AA-ddThh:mm}");
        });
    }

    @Test
    void test_objectTimePattern_Exceptional_at_theEnd() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndexUtility.validateAndGetDateTimeFormatter("events-%{yyy{MM}dd}");
        });
    }

    @Test
    void test_object_allows_one_date_time_pattern_Exceptional() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndexUtility.validateAndGetDateTimeFormatter("events-%{yyyy-MM-dd}-%{yyyy-MM-dd}");
        });
    }

    @Test
    void test_object_nested_pattern_Exceptional() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndexUtility.validateAndGetDateTimeFormatter("bucket-name-\\%{\\%{yyyy.MM.dd}}");
        });
    }

    @Test
    void test_object_null_time_pattern() throws NullPointerException {
        assertNull(S3ObjectIndexUtility.validateAndGetDateTimeFormatter("bucket-name"));
    }

    @Test
    void test_objectAliasWithDatePrefix_Exceptional_time_TooGranular() throws IllegalArgumentException {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndexUtility.getObjectNameWithDateTimeId("events-%{yyyy-AA-dd}");
        });
    }

    @Test
    void test_objectAliasWithDatePrefix_equal() throws IllegalArgumentException {

        String expectedTimeFormatter = S3ObjectIndexUtility.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        String actualTimeFormatter = S3ObjectIndexUtility.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        assertNotEquals(actualTimeFormatter.toString(), expectedTimeFormatter.toString());
    }

    @Test
    void test_default_constructor() {
        S3ObjectIndexUtility object = new S3ObjectIndexUtility();
        assertNotNull(object);
    }
}