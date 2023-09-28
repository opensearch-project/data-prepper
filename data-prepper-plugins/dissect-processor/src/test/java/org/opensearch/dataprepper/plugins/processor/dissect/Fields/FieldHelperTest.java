/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect.Fields;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class FieldHelperTest {
    private FieldHelper fieldHelper;

    @BeforeEach
    void setUp() {
        fieldHelper = new FieldHelper();
    }

    @Test
    void testNullFieldString() {
        assertThat(fieldHelper.getField(null), is(nullValue()));
    }

    @Test
    void testWhitespaces() {
        Field field = fieldHelper.getField("  ");
        assertThat(field, is(instanceOf(AppendField.class)));
        assertThat(field.getKey(), is(""));
    }

    @Test
    void testNormalFieldString() {
        Field field = fieldHelper.getField("field1");
        assertThat(field, is(instanceOf(NormalField.class)));
        assertThat(field.getKey(), is("field1"));
        assertThat(field.stripTrailing, is(false));
    }

    @Test
    void testNormalFieldWithSuffixString() {
        Field field = fieldHelper.getField("field1->");
        assertThat(field, is(instanceOf(NormalField.class)));
        assertThat(field.getKey(), is("field1"));
        assertThat(field.stripTrailing, is(true));
    }

    @Test
    void testNamedSkipFieldString() {
        Field field = fieldHelper.getField("?field1");
        assertThat(field, is(instanceOf(SkipField.class)));
        assertThat(field.getKey(), is("field1"));
    }

    @Test
    void testAppendFieldString() {
        Field field = fieldHelper.getField("+field1");
        assertThat(field, is(instanceOf(AppendField.class)));
        assertThat(field.getKey(), is("field1"));
    }

    @Test
    void testAppendFieldWithIndexString() {
        Field field = fieldHelper.getField("+field1/1");
        assertThat(field, is(instanceOf(AppendField.class)));
        assertThat(field.getKey(), is("field1"));
        assertThat(((AppendField)field).getIndex(), is(1));
    }

    @Test
    void testIndirectFieldString() {
        Field field = fieldHelper.getField("&field1");
        assertThat(field, is(instanceOf(IndirectField.class)));
        assertThat(field.getKey(), is("field1"));
    }
}