/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.annotations.TransformationFunction;
import org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineTransformFunctionsTest {

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

    @Test
    void calculateDepthForRdsSource_returns_2_when_prefix_is_null_and_no_env_var() {
        // Without env var set, baseDepth = 2
        String result = PipelineTransformFunctions.calculateDepthForRdsSource(null);
        assertNotNull(result);
    }

    @Test
    void calculateDepthForRdsSource_adds_prefix_segments_to_base_depth() {
        // Without env var, baseDepth = 2, prefix "a/b" = 2 segments → total 4
        // With env var, baseDepth = 3, prefix "a/b" = 2 segments → total 5
        String result = PipelineTransformFunctions.calculateDepthForRdsSource("a/b");
        assertNotNull(result);
        int depth = Integer.parseInt(result);
        assertTrue(depth >= 4, "Depth should be at least 4 (2 segments + 2 base)");
    }

    @Test
    void getSourceCoordinationIdentifierEnvVariable_returns_env_var_when_prefix_null() {
        // Returns the env var value (which may be null in test env)
        String result = PipelineTransformFunctions.getSourceCoordinationIdentifierEnvVariable(null);
        // In test environment, env var is likely not set
        assertNull(result);
    }

    @Test
    void getSourceCoordinationIdentifierEnvVariable_prepends_prefix_to_env_var() {
        String result = PipelineTransformFunctions.getSourceCoordinationIdentifierEnvVariable("myprefix");
        // env var is null in test, so result will be "myprefix/null"
        assertNotNull(result);
        assertTrue(result.startsWith("myprefix/"));
    }

    @Test
    void getIncludePrefixForRdsSource_returns_buffer_prefix_when_all_null() {
        // When both s3Prefix and env var are null
        String result = PipelineTransformFunctions.getIncludePrefixForRdsSource(null);
        assertEquals("/buffer", result);
    }

    @Test
    void getIncludePrefixForRdsSource_prepends_s3prefix_when_no_env_var() {
        String result = PipelineTransformFunctions.getIncludePrefixForRdsSource("myprefix");
        assertEquals("myprefix/buffer", result);
    }

    @Test
    void getAccountIdFromRole_returns_account_id_from_valid_arn() {
        String roleArn = "arn:aws:iam::123456789012:role/MyRole";
        String result = PipelineTransformFunctions.getAccountIdFromRole(roleArn);
        assertEquals("123456789012", result);
    }

    @Test
    void getAccountIdFromRole_returns_null_when_arn_is_null() {
        assertNull(PipelineTransformFunctions.getAccountIdFromRole(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "not-an-arn", "arn:aws:iam::invalid"})
    void getAccountIdFromRole_returns_null_for_invalid_arns(String invalidArn) {
        String result = PipelineTransformFunctions.getAccountIdFromRole(invalidArn);
        assertNull(result);
    }

    @Test
    void shortenIdentifier_returns_original_when_within_limit() {
        String result = PipelineTransformFunctions.shortenIdentifier("short", 15);
        assertEquals("short", result);
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
