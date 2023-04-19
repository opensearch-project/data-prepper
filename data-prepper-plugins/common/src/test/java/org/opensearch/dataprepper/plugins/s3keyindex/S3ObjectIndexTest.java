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

class S3ObjectIndexTest {

    @Test
    void testObjectDateTimePatterns_not_equal() throws IllegalArgumentException {

        String expectedIndex = S3ObjectIndex.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        String actualIndex = S3ObjectIndex.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        assertFalse(actualIndex.contains(expectedIndex));
    }
    
    @Test
    void testgetObjectPathPrefix_not_equal() throws IllegalArgumentException {

        String expectedIndex = S3ObjectIndex.getObjectPathPrefix("events-%{yyyy}");
        String actualIndex = S3ObjectIndex.getObjectPathPrefix("events-%{yyyy}");
        assertTrue(actualIndex.contains(expectedIndex));
    }

    @Test
    void testObjectTimePattern_Exceptional_time_TooGranular() throws IllegalArgumentException {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndex.getDatePatternFormatter("events-%{yyyy-AA-dd}");
        });
    }

    @Test
    void testObjectTimePatterns_equal() throws IllegalArgumentException {

        DateTimeFormatter expectedIndex = S3ObjectIndex.getDatePatternFormatter("events-%{yyyy-MM-dd}");
        DateTimeFormatter actualIndex = S3ObjectIndex.getDatePatternFormatter("events-%{yyyy-MM-dd}");
        assertEquals(actualIndex.toString(), expectedIndex.toString());
    }

    @Test
    void test_utc_current_time() throws IllegalArgumentException {

        ZonedDateTime expectedIndex = S3ObjectIndex.getCurrentUtcTime();
        ZonedDateTime actualIndex = S3ObjectIndex.getCurrentUtcTime();

        assertEquals(expectedIndex.getDayOfYear(), actualIndex.getDayOfYear());
        assertEquals(expectedIndex.getDayOfMonth(), actualIndex.getDayOfMonth());
        assertEquals(expectedIndex.getDayOfWeek(), actualIndex.getDayOfWeek());
    }

    @Test
    void testObjectTimePattern_Exceptional_TooGranular() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndex.getDatePatternFormatter("events-%{yyyy-AA-ddThh:mm}");
        });
    }

    @Test
    void testObjectTimePattern_Exceptional_at_theEnd() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndex.getDatePatternFormatter("events-%{yyy{MM}dd}");
        });
    }

    @Test
    void testObject_allows_one_date_time_pattern_Exceptional() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndex.getDatePatternFormatter("events-%{yyyy-MM-dd}-%{yyyy-MM-dd}");
        });
    }

    @Test
    void testObject_nested_pattern_Exceptional() {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndex.getDatePatternFormatter("bucket-name-\\%{\\%{yyyy.MM.dd}}");
        });
    }

    @Test
    void testObject_null_time_pattern() throws NullPointerException {
        assertNull(S3ObjectIndex.getDatePatternFormatter("bucket-name"));
    }

    @Test
    void testObjectAliasWithDatePrefix_Exceptional_time_TooGranular() throws IllegalArgumentException {
        assertThrows(IllegalArgumentException.class, () -> {
            S3ObjectIndex.getObjectNameWithDateTimeId("events-%{yyyy-AA-dd}");
        });
    }

    @Test
    void testObjectAliasWithDatePrefix_equal() throws IllegalArgumentException {

        String expectedIndex = S3ObjectIndex.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        String actualIndex = S3ObjectIndex.getObjectNameWithDateTimeId("events-%{yyyy-MM-dd}");
        assertNotEquals(actualIndex.toString(), expectedIndex.toString());
    }

    @Test
    void test_default_constructor() {
        S3ObjectIndex object = new S3ObjectIndex();
        assertNotNull(object);
    }
}