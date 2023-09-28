/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.Field;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DissectorTest {

    @Test
    void test_normal_field_trailing_spaces(){
        Dissector dissector = createObjectUnderTest(" %{field1} %{field2} ");
        boolean result = dissector.dissectText(" foo bar ");

        assertTrue(result);

        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("foo"));
        assertThat(fields.get(1).getKey(), is("field2"));
        assertThat(fields.get(1).getValue(), is("bar"));
    }

    @Test
    void test_normal_field_without_trailing_spaces(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");
        boolean result = dissector.dissectText("dm1 foo bar dm2");
        assertTrue(result);

        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("foo"));
        assertThat(fields.get(1).getKey(), is("field2"));
        assertThat(fields.get(1).getValue(), is("bar"));
    }

    @Test
    void test_normal_field_failure_without_delimiters(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");

        boolean result = dissector.dissectText("dm1 foo bar");
        assertFalse(result);
    }

    @Test
    void test_normal_field_failure_with_extra_whitespaces(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");

        boolean result = dissector.dissectText(" dm1 foo bar dm2");
        assertFalse(result);
    }

    @Test
    void test_named_skip_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1} %{field2} dm2");


        boolean result = dissector.dissectText("dm1 foo bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("field2"));
        assertThat(fields.get(0).getValue(), is("bar"));
    }

    @Test
    void test_unnamed_skip_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{} %{field2} dm2");


        boolean result = dissector.dissectText("dm1 foo bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("field2"));
        assertThat(fields.get(0).getValue(), is("bar"));
    }

    @Test
    void test_indirect_field_with_skip_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1} %{&field1} dm2");


        boolean result = dissector.dissectText("dm1 foo bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("foo"));
        assertThat(fields.get(0).getValue(), is("bar"));
    }

    @Test
    void test_indirect_field_with_normal_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{&field1} dm2");


        boolean result = dissector.dissectText("dm1 foo bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("foo"));
        assertThat(fields.get(1).getKey(), is("foo"));
        assertThat(fields.get(1).getValue(), is("bar"));
    }

    @Test
    void test_append_field_without_index(){
        Dissector dissector = createObjectUnderTest("dm1 %{+field1} %{+field1} dm2");


        boolean result = dissector.dissectText("dm1 foo bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("foobar"));
    }

    @Test
    void test_append_field_with_index(){
        Dissector dissector = createObjectUnderTest("dm1 %{+field1/2} %{+field1/1} dm2");


        boolean result = dissector.dissectText("dm1 foo bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("barfoo"));
    }

    @Test
    void test_append_whitespace_normal_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1->} %{field2} dm2");


        boolean result = dissector.dissectText("dm1 foo      bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("foo"));
        assertThat(fields.get(1).getKey(), is("field2"));
        assertThat(fields.get(1).getValue(), is("bar"));
    }

    @Test
    void test_append_whitespace_append_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{+field1->} %{+field1} dm2");


        boolean result = dissector.dissectText("dm1 foo      bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("foobar"));
    }

    @Test
    void test_append_whitespace_indirect_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1->} %{&field1} dm2");


        boolean result = dissector.dissectText("dm1 foo      bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("foo"));
        assertThat(fields.get(0).getValue(), is("bar"));
    }

    @Test
    void test_skip_fields_with_padding(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1->} %{?field3} %{field2} dm2");


        boolean result = dissector.dissectText("dm1 foo     skip   bar dm2");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(1));
        assertThat(fields.get(0).getKey(), is("field2"));
        assertThat(fields.get(0).getValue(), is("bar"));
    }

    @Test
    void test_indirect_field_with_append(){
        Dissector dissector = createObjectUnderTest("%{+field1->} %{+field1} %{&field1->}");


        boolean result = dissector.dissectText("foo     bar result     ");
        assertTrue(result);
        List<Field> fields = dissector.getDissectedFields();

        assertThat(fields.size(), is(2));
        assertThat(fields.get(0).getKey(), is("field1"));
        assertThat(fields.get(0).getValue(), is("foobar"));
        assertThat(fields.get(1).getKey(), is("foobar"));
        assertThat(fields.get(1).getValue(), is("result"));
    }

    private Dissector createObjectUnderTest(String dissectPatternString) {
        return new Dissector(dissectPatternString);
    }
}