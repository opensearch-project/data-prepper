/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DissectorTest {

    @Test
    void test_normal_field_trailing_spaces(){
        Dissector dissector = createObjectUnderTest(" %{field1} %{field2} ");
        Map<String, String> result = dissector.dissectText(" foo bar ");

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertThat(result.get("field1"), is("foo"));
        assertThat(result.get("field2"), is("bar"));
    }

    @Test
    void test_normal_field_without_trailing_spaces(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");
        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertThat(result.get("field1"), is("foo"));
        assertThat(result.get("field2"), is("bar"));
    }

    @Test
    void test_normal_field_failure_without_delimiters(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");

        assertNull(dissector.dissectText("dm1 foo bar"));
    }

    @Test
    void test_normal_field_failure_with_extra_whitespaces(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");

        assertNull(dissector.dissectText(" dm1 foo bar dm2"));
    }

    @Test
    void test_named_skip_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1} %{field2} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("field2"), is("bar"));
    }

    @Test
    void test_unnamed_skip_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{} %{field2} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("field2"), is("bar"));
    }

    @Test
    void test_indirect_field_with_skip_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1} %{&field1} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("foo"), is("bar"));
    }

    @Test
    void test_indirect_field_with_normal_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{&field1} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.get("field1"), is("foo"));
        assertThat(result.get("foo"), is("bar"));
    }

    @Test
    void test_append_field_without_index(){
        Dissector dissector = createObjectUnderTest("dm1 %{+field1} %{+field1} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("field1"), is("foobar"));
    }

    @Test
    void test_append_field_with_index(){
        Dissector dissector = createObjectUnderTest("dm1 %{+field1/2} %{+field1/1} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("field1"), is("barfoo"));
    }

    @Test
    void test_append_whitespace_normal_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1->} %{field2} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo      bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.get("field1"), is("foo"));
        assertThat(result.get("field2"), is("bar"));
    }

    @Test
    void test_append_whitespace_append_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{+field1->} %{+field1} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo      bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("field1"), is("foobar"));
    }

    @Test
    void test_append_whitespace_indirect_field(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1->} %{&field1} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo      bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("foo"), is("bar"));
    }

    @Test
    void test_skip_fields_with_padding(){
        Dissector dissector = createObjectUnderTest("dm1 %{?field1->} %{?field3} %{field2} dm2");

        Map<String, String> result = dissector.dissectText("dm1 foo     skip   bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("field2"), is("bar"));
    }

    @Test
    void test_indirect_field_with_append(){
        Dissector dissector = createObjectUnderTest("%{+field1->} %{+field1} %{&field1->}");

        Map<String, String> result = dissector.dissectText("foo     bar result     ");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(2));
        assertThat(result.get("field1"), is("foobar"));
        assertThat(result.get("foobar"), is("result"));
    }

    @Test
    void test_dissect_text_returns_null_on_failure(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");
        assertNull(dissector.dissectText(null));
    }

    @Test
    void test_indirect_field_unresolved(){
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{&field2} dm2");
        Map<String, String> result = dissector.dissectText("dm1 foo bar dm2");

        assertFalse(result.isEmpty());
        assertThat(result.size(), is(1));
        assertThat(result.get("field1"), is("foo"));
        assertFalse(result.containsKey("bar"));
    }

    @Test
    void test_concurrent_dissect_no_cross_contamination() throws InterruptedException {
        Dissector dissector = createObjectUnderTest("dm1 %{field1} %{field2} dm2");

        int threadCount = 10;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<String> errors = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < threadCount; t++) {
            final String field1Value = "value" + t;
            final String field2Value = "data" + t;
            final String input = "dm1 " + field1Value + " " + field2Value + " dm2";

            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        Map<String, String> result = dissector.dissectText(input);
                        if (!field1Value.equals(result.get("field1"))) {
                            errors.add("field1 mismatch: expected " + field1Value + " got " + result.get("field1"));
                        }
                        if (!field2Value.equals(result.get("field2"))) {
                            errors.add("field2 mismatch: expected " + field2Value + " got " + result.get("field2"));
                        }
                    }
                } catch (Exception e) {
                    errors.add(e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "Test timed out");
        executor.shutdown();

        assertTrue(errors.isEmpty(), "Concurrency errors: " + errors);
    }

    private Dissector createObjectUnderTest(String dissectPatternString) {
        return new Dissector(dissectPatternString);
    }
}
