/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.annotations.TransformationFunction;
import org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Provides static utility methods for pipeline template transformations.
 * These methods are invoked dynamically via the FUNCTION_NAME placeholder mechanism
 * in template YAML files.
 */
public class PipelineTransformFunctions implements PipelineTransformFunctionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineTransformFunctions.class);

    private static final String SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE = "SOURCE_COORDINATION_PIPELINE_IDENTIFIER";
    private static final String S3_BUFFER_PREFIX = "/buffer";
    private static final int MAX_SOURCE_IDENTIFIER_LENGTH = 15;

    private PipelineTransformFunctions() {
    }

    /**
     * Calculate s3 folder scan depth for DocDB source pipeline.
     *
     * @param s3Prefix s3 prefix defined in the source configuration
     * @return s3 folder scan depth as a string
     */
    @TransformationFunction
    public static String calculateDepth(String s3Prefix) {
        return Integer.toString(getDepth(s3Prefix, 4));
    }

    /**
     * Calculate s3 folder scan depth for RDS source pipeline.
     *
     * @param s3Prefix s3 prefix defined in the source configuration
     * @return s3 folder scan depth as a string
     */
    @TransformationFunction
    public static String calculateDepthForRdsSource(String s3Prefix) {
        String envSourceCoordinationIdentifier = getSourceCoordinationIdentifier();
        int baseDepth = envSourceCoordinationIdentifier != null ? 3 : 2;
        return Integer.toString(getDepth(s3Prefix, baseDepth));
    }

    /**
     * Get the source coordination identifier environment variable, optionally prefixed with s3Prefix.
     *
     * @param s3Prefix s3 prefix defined in the source configuration
     * @return source coordination identifier value
     */
    @TransformationFunction
    public static String getSourceCoordinationIdentifierEnvVariable(String s3Prefix) {
        String envSourceCoordinationIdentifier = System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
        if (s3Prefix == null) {
            return envSourceCoordinationIdentifier;
        }
        return s3Prefix + "/" + envSourceCoordinationIdentifier;
    }

    /**
     * Get the include_prefix for the s3 scan source in RDS pipelines.
     *
     * @param s3Prefix s3 prefix defined in the source configuration
     * @return the actual include_prefix value
     */
    @TransformationFunction
    public static String getIncludePrefixForRdsSource(String s3Prefix) {
        final String envSourceCoordinationIdentifier = System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
        final String shortenedSourceIdentifier = envSourceCoordinationIdentifier != null ?
                shortenIdentifier(envSourceCoordinationIdentifier, MAX_SOURCE_IDENTIFIER_LENGTH) : null;
        if (s3Prefix == null && envSourceCoordinationIdentifier == null) {
            return S3_BUFFER_PREFIX;
        } else if (s3Prefix == null) {
            return shortenedSourceIdentifier + S3_BUFFER_PREFIX;
        } else if (envSourceCoordinationIdentifier == null) {
            return s3Prefix + S3_BUFFER_PREFIX;
        }
        return s3Prefix + "/" + shortenedSourceIdentifier + S3_BUFFER_PREFIX;
    }

    /**
     * Extract the AWS account ID from a role ARN.
     *
     * @param roleArn the IAM role ARN
     * @return the account ID, or null if the ARN is invalid
     */
    @TransformationFunction
    public static String getAccountIdFromRole(final String roleArn) {
        if (roleArn == null) {
            return null;
        }
        try {
            return Arn.fromString(roleArn).accountId().orElse(null);
        } catch (Exception e) {
            LOG.warn("Malformatted role ARN for dynamic transformation: {}", roleArn);
            return null;
        }
    }

    private static String getSourceCoordinationIdentifier() {
        return System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
    }

    private static int getDepth(String s3Prefix, int baseDepth) {
        if (s3Prefix == null) {
            return baseDepth;
        }
        return s3Prefix.split("/").length + baseDepth;
    }

    static String shortenIdentifier(final String identifier, final int maxLength) {
        if (identifier.length() <= maxLength) {
            return identifier;
        }

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedHash = digest.digest(identifier.getBytes(StandardCharsets.UTF_8));
            String base64Hash = Base64.getUrlEncoder().withoutPadding().encodeToString(encodedHash);
            return base64Hash.substring(0, Math.min(base64Hash.length(), maxLength));
        } catch (final NoSuchAlgorithmException e) {
            return identifier.substring(0, maxLength);
        }
    }
}
