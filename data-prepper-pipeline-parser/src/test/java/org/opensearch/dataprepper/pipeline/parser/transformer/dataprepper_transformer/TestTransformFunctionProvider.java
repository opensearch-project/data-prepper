/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.pipeline.parser.transformer.dataprepper_transformer;

import org.opensearch.dataprepper.model.annotations.TransformationFunction;
import org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider;

/**
 * Test-only function provider for pipeline transformation tests.
 * Replicates the logic from PipelineTransformFunctions in aws-plugin.
 */
public class TestTransformFunctionProvider implements PipelineTransformFunctionProvider {

    private static final String SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE = "SOURCE_COORDINATION_PIPELINE_IDENTIFIER";

    @TransformationFunction
    public static String calculateDepth(String s3Prefix) {
        return Integer.toString(getDepth(s3Prefix, 4));
    }

    @TransformationFunction
    public static String calculateDepthForRdsSource(String s3Prefix) {
        String envSourceCoordinationIdentifier = System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
        int baseDepth = envSourceCoordinationIdentifier != null ? 3 : 2;
        return Integer.toString(getDepth(s3Prefix, baseDepth));
    }

    @TransformationFunction
    public static String getSourceCoordinationIdentifierEnvVariable(String s3Prefix) {
        String envSourceCoordinationIdentifier = System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
        if (s3Prefix == null) {
            return envSourceCoordinationIdentifier;
        }
        return s3Prefix + "/" + envSourceCoordinationIdentifier;
    }

    @TransformationFunction
    public static String getAccountIdFromRole(final String roleArn) {
        if (roleArn == null) {
            return null;
        }
        try {
            return software.amazon.awssdk.arns.Arn.fromString(roleArn).accountId().orElse(null);
        } catch (Exception e) {
            return null;
        }
    }

    @TransformationFunction
    public static String getIncludePrefixForRdsSource(String s3Prefix) {
        final String envSourceCoordinationIdentifier = System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
        if (s3Prefix == null && envSourceCoordinationIdentifier == null) {
            return "/buffer";
        } else if (s3Prefix == null) {
            return envSourceCoordinationIdentifier + "/buffer";
        } else if (envSourceCoordinationIdentifier == null) {
            return s3Prefix + "/buffer";
        }
        return s3Prefix + "/" + envSourceCoordinationIdentifier + "/buffer";
    }

    private static int getDepth(String s3Prefix, int baseDepth) {
        if (s3Prefix == null) {
            return baseDepth;
        }
        return s3Prefix.split("/").length + baseDepth;
    }
}
