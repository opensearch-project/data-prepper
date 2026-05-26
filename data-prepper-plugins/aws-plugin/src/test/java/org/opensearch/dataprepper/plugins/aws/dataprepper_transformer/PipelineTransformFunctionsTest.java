/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws.dataprepper_transformer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.annotations.TransformationFunction;
import org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider;

import java.lang.reflect.Method;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineTransformFunctionsTest {

    private Supplier<String> originalSupplier;

    @BeforeEach
    void setUp() {
        originalSupplier = PipelineTransformFunctions.sourceCoordinationIdentifierSupplier;
    }

    @AfterEach
    void tearDown() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = originalSupplier;
    }

    @Test
    void class_implements_PipelineTransformFunctionProvider() {
        assertTrue(PipelineTransformFunctionProvider.class.isAssignableFrom(PipelineTransformFunctions.class));
    }

    @Test
    void all_public_transformation_methods_are_annotated() throws Exception {
        String[] methodNames = {
                "calculateDepth",
                "calculateDepthForRdsSource",
                "getSourceCoordinationIdentifierEnvVariable",
                "getIncludePrefixForRdsSource",
                "getAccountIdFromRole"
        };

        for (String methodName : methodNames) {
            Method method = PipelineTransformFunctions.class.getMethod(methodName, String.class);
            assertTrue(method.isAnnotationPresent(TransformationFunction.class),
                    "Method " + methodName + " should be annotated with @TransformationFunction");
        }
    }

    @Test
    void default_sourceCoordinationIdentifierSupplier_reads_env_variable() {
        // Exercises the default lambda on line 39: () -> System.getenv(...)
        // env var is not set in test, so returns null — but the lambda bytecode is covered
        String result = originalSupplier.get();
        assertNull(result);
    }


    // --- calculateDepth ---

    @Test
    void calculateDepth_returns_4_when_prefix_is_null() {
        assertEquals("4", PipelineTransformFunctions.calculateDepth(null));
    }

    @Test
    void calculateDepth_returns_correct_depth_with_single_segment_prefix() {
        assertEquals("5", PipelineTransformFunctions.calculateDepth("myprefix"));
    }

    @Test
    void calculateDepth_returns_correct_depth_with_multi_segment_prefix() {
        assertEquals("6", PipelineTransformFunctions.calculateDepth("prefix/subfolder"));
    }

    // --- calculateDepthForRdsSource ---

    @Test
    void calculateDepthForRdsSource_returns_2_when_prefix_is_null_and_no_env_var() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> null;
        assertEquals("2", PipelineTransformFunctions.calculateDepthForRdsSource(null));
    }

    @Test
    void calculateDepthForRdsSource_returns_3_when_prefix_is_null_and_env_var_set() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> "my-identifier";
        assertEquals("3", PipelineTransformFunctions.calculateDepthForRdsSource(null));
    }

    @Test
    void calculateDepthForRdsSource_adds_prefix_segments_with_env_var() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> "my-identifier";
        assertEquals("5", PipelineTransformFunctions.calculateDepthForRdsSource("a/b"));
    }

    @Test
    void calculateDepthForRdsSource_adds_prefix_segments_without_env_var() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> null;
        assertEquals("4", PipelineTransformFunctions.calculateDepthForRdsSource("a/b"));
    }

    // --- getSourceCoordinationIdentifierEnvVariable ---

    @Test
    void getSourceCoordinationIdentifierEnvVariable_returns_env_var_when_prefix_null() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> "test-id";
        assertEquals("test-id", PipelineTransformFunctions.getSourceCoordinationIdentifierEnvVariable(null));
    }

    @Test
    void getSourceCoordinationIdentifierEnvVariable_returns_null_when_env_not_set_and_prefix_null() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> null;
        assertNull(PipelineTransformFunctions.getSourceCoordinationIdentifierEnvVariable(null));
    }

    @Test
    void getSourceCoordinationIdentifierEnvVariable_prepends_prefix_to_env_var() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> "test-id";
        assertEquals("myprefix/test-id",
                PipelineTransformFunctions.getSourceCoordinationIdentifierEnvVariable("myprefix"));
    }

    // --- getIncludePrefixForRdsSource ---

    @Test
    void getIncludePrefixForRdsSource_returns_buffer_prefix_when_all_null() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> null;
        assertEquals("/buffer", PipelineTransformFunctions.getIncludePrefixForRdsSource(null));
    }

    @Test
    void getIncludePrefixForRdsSource_returns_shortened_id_plus_buffer_when_prefix_null_and_env_set() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> "short";
        assertEquals("short/buffer", PipelineTransformFunctions.getIncludePrefixForRdsSource(null));
    }

    @Test
    void getIncludePrefixForRdsSource_returns_prefix_plus_buffer_when_env_null() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> null;
        assertEquals("myprefix/buffer", PipelineTransformFunctions.getIncludePrefixForRdsSource("myprefix"));
    }

    @Test
    void getIncludePrefixForRdsSource_returns_full_path_when_both_set() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> "short";
        assertEquals("myprefix/short/buffer",
                PipelineTransformFunctions.getIncludePrefixForRdsSource("myprefix"));
    }

    @Test
    void getIncludePrefixForRdsSource_shortens_long_identifier() {
        PipelineTransformFunctions.sourceCoordinationIdentifierSupplier = () -> "this-is-a-very-long-identifier-exceeding-max";
        String result = PipelineTransformFunctions.getIncludePrefixForRdsSource(null);
        assertNotNull(result);
        assertTrue(result.endsWith("/buffer"));
        // shortened id is 15 chars + "/buffer" = 22 chars total
        assertEquals(22, result.length());
    }

    // --- getAccountIdFromRole ---

    @Test
    void getAccountIdFromRole_returns_account_id_from_valid_arn() {
        assertEquals("123456789012",
                PipelineTransformFunctions.getAccountIdFromRole("arn:aws:iam::123456789012:role/MyRole"));
    }

    @Test
    void getAccountIdFromRole_returns_null_when_arn_is_null() {
        assertNull(PipelineTransformFunctions.getAccountIdFromRole(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "not-an-arn", "arn:aws:iam:::role/test-role"})
    void getAccountIdFromRole_returns_null_for_invalid_arns(String invalidArn) {
        assertNull(PipelineTransformFunctions.getAccountIdFromRole(invalidArn));
    }

    // --- shortenIdentifier ---

    @Test
    void shortenIdentifier_returns_original_when_within_limit() {
        assertEquals("short", PipelineTransformFunctions.shortenIdentifier("short", 15));
    }

    @Test
    void shortenIdentifier_returns_shortened_hash_when_exceeds_limit() {
        String longId = "this-is-a-very-long-identifier-that-exceeds-limit";
        String result = PipelineTransformFunctions.shortenIdentifier(longId, 15);
        assertNotNull(result);
        assertEquals(15, result.length());
    }

    @Test
    void shortenIdentifier_returns_consistent_results() {
        String id = "consistent-test-identifier";
        String result1 = PipelineTransformFunctions.shortenIdentifier(id, 10);
        String result2 = PipelineTransformFunctions.shortenIdentifier(id, 10);
        assertEquals(result1, result2);
    }
}
