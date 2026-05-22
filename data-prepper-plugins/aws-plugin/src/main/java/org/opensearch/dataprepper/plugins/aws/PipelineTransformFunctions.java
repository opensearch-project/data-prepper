/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.annotations.SkipTestCoverageGenerated;
import org.opensearch.dataprepper.model.annotations.TransformationFunction;
import org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.function.Supplier;

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

    static Supplier<String> sourceCoordinationIdentifierSupplier =
            () -> System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);

    @SkipTestCoverageGenerated
    private PipelineTransformFunctions() {
    }

    @TransformationFunction
    public static String calculateDepth(String s3Prefix) {
        return Integer.toString(getDepth(s3Prefix, 4));
    }

    @TransformationFunction
    public static String calculateDepthForRdsSource(String s3Prefix) {
        String envSourceCoordinationIdentifier = sourceCoordinationIdentifierSupplier.get();
        int baseDepth = envSourceCoordinationIdentifier != null ? 3 : 2;
        return Integer.toString(getDepth(s3Prefix, baseDepth));
    }

    @TransformationFunction
    public static String getSourceCoordinationIdentifierEnvVariable(String s3Prefix) {
        String envSourceCoordinationIdentifier = sourceCoordinationIdentifierSupplier.get();
        if (s3Prefix == null) {
            return envSourceCoordinationIdentifier;
        }
        return s3Prefix + "/" + envSourceCoordinationIdentifier;
    }

    @TransformationFunction
    public static String getIncludePrefixForRdsSource(String s3Prefix) {
        final String envSourceCoordinationIdentifier = sourceCoordinationIdentifierSupplier.get();
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

    private static int getDepth(String s3Prefix, int baseDepth) {
        if (s3Prefix == null) {
            return baseDepth;
        }
        return s3Prefix.split("/").length + baseDepth;
    }

    @SkipTestCoverageGenerated
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
