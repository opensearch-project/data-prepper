/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect.Fields;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FieldHelperTest {
    private FieldHelper fieldHelper;

    @BeforeEach
    void setUp() {
        fieldHelper = new FieldHelper();
    }

    @Test
    void testNullFieldString() {
        assertThat(fieldHelper.getField(null, null), is(nullValue()));
    }

    @Test
    void testWhitespaces() {
        Field field = fieldHelper.getField("  ", null);
        assertThat(field, is(instanceOf(AppendField.class)));
        assertThat(field.getKey(), is(""));
    }

    @Test
    void testNormalFieldString() {
        Field field = fieldHelper.getField("field1", null);
        assertThat(field, is(instanceOf(NormalField.class)));
        assertThat(field.getKey(), is("field1"));
        assertThat(field.stripTrailing, is(false));
    }

    @Test
    void testNormalFieldWithSuffixString() {
        Field field = fieldHelper.getField("field1->", null);
        assertThat(field, is(instanceOf(NormalField.class)));
        assertThat(field.getKey(), is("field1"));
        assertThat(field.stripTrailing, is(true));
    }

    @Test
    void testNamedSkipFieldString() {
        Field field = fieldHelper.getField("?field1", null);
        assertThat(field, is(instanceOf(SkipField.class)));
        assertThat(field.getKey(), is("field1"));
    }

    @Test
    void testAppendFieldString() {
        Field field = fieldHelper.getField("+field1", null);
        assertThat(field, is(instanceOf(AppendField.class)));
        assertThat(field.getKey(), is("field1"));
    }

    @Test
    void testAppendFieldWithIndexString() {
        Field field = fieldHelper.getField("+field1/1", null);
        assertThat(field, is(instanceOf(AppendField.class)));
        assertThat(field.getKey(), is("field1"));
        assertThat(((AppendField)field).getIndex(), is(1));
    }

    @Test
    void testIndirectFieldString() {
        Field field = fieldHelper.getField("&field1", null);
        assertThat(field, is(instanceOf(IndirectField.class)));
        assertThat(field.getKey(), is("field1"));
    }

    @Test
    void testGetField_wiresLinkedList_whenLastFieldProvided() {
        Field first = fieldHelper.getField("field1", null);
        Field second = fieldHelper.getField("field2", first);
        assertThat(first.getNext(), is(second));
    }

    @Test
    void testSetNext_throwsWhenCalledTwice() {
        NormalField field = new NormalField("a");
        NormalField next1 = new NormalField("b");
        NormalField next2 = new NormalField("c");
        field.setNext(next1);
        assertThrows(IllegalStateException.class, () -> field.setNext(next2));
    }
}
