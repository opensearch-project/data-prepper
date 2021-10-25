/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

public class StringPrepperTests {

    private final String UPPERCASE_TEST_STRING = "data_prepper";
    private final String LOWERCASE_TEST_STRING = "STRING_CONVERTER";
    private final Record<String> TEST_RECORD_1 = new Record<>(UPPERCASE_TEST_STRING);
    private final Record<String> TEST_RECORD_2 = new Record<>(LOWERCASE_TEST_STRING);
    private final List<Record<String>> TEST_RECORDS = Arrays.asList(TEST_RECORD_1, TEST_RECORD_2);
    private StringPrepper.Configuration configuration;

    @BeforeEach
    void setUp() {
        configuration = new StringPrepper.Configuration();
    }

    private StringPrepper createObjectUnderTest() {
        return new StringPrepper(configuration);
    }

    @Test
    public void testStringPrepperDefault() {

        final StringPrepper stringPrepper = createObjectUnderTest();
        final List<Record<String>> modifiedRecords = (List<Record<String>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<String> modifiedRecordData = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());
        final List<String> expectedRecordData = Arrays.asList(UPPERCASE_TEST_STRING.toUpperCase(), LOWERCASE_TEST_STRING);

        assertThat(modifiedRecordData, equalTo(expectedRecordData));
    }

    @Test
    public void testStringPrepperLowerCase() {
        configuration.setUpperCase(false);
        final StringPrepper stringPrepper = createObjectUnderTest();
        final List<Record<String>> modifiedRecords = (List<Record<String>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<String> modifiedRecordData = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());
        final List<String> expectedRecordData = Arrays.asList(UPPERCASE_TEST_STRING, LOWERCASE_TEST_STRING.toLowerCase());

        assertThat(modifiedRecordData.size(), equalTo(2));
        assertThat(modifiedRecordData, hasItems(UPPERCASE_TEST_STRING, LOWERCASE_TEST_STRING.toLowerCase()));
    }

    @Test
    public void testStringPrepperUpperCase() {
        configuration.setUpperCase(true);
        final StringPrepper stringPrepper = createObjectUnderTest();
        final List<Record<String>> modifiedRecords = (List<Record<String>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<String> modifiedRecordData = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());
        final List<String> expectedRecordData = Arrays.asList(UPPERCASE_TEST_STRING.toUpperCase(), LOWERCASE_TEST_STRING);

        assertThat(modifiedRecordData, equalTo(expectedRecordData));
    }

    @Test
    public void testPrepareForShutdown() {
        final StringPrepper stringPrepper = createObjectUnderTest();

        stringPrepper.prepareForShutdown();

        assertThat(stringPrepper.isReadyForShutdown(), equalTo(true));
    }

}
