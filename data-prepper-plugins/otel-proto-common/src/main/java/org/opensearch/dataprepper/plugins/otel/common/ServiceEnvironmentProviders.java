/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

import static org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil.getStringAttribute;
import java.util.Map;

class ServiceEnvironmentProviders {
    public static String getDeploymentEnvironment(final Map<String, Object> spanAttributes) {
        try {
            // Navigate: spanAttributes -> "resource" -> "attributes" -> deployment keys
            @SuppressWarnings("unchecked")
            Map<String, Object> resourceAttrs = (Map<String, Object>)
                ((Map<String, Object>) spanAttributes.get("resource")).get("attributes");

            // Extract from resource.attributes.deployment.environment.name
            String env = getStringAttribute(resourceAttrs, "deployment.environment.name");
            if (env != null && !env.trim().isEmpty()) {
                return env;
            }

            // Fall back to resource.attributes.deployment.environment
            env = getStringAttribute(resourceAttrs, "deployment.environment");
            if (env != null && !env.trim().isEmpty()) {
                return env;
            }
        } catch (Exception ignored) {
            // Any navigation failure falls through to default
        }
        // Default: 'generic:default'
        return "generic:default";
    }

    /**
     * Get an attribute by checking span-level attributes first, then resource attributes.
     */
    private static String getAttributeFromSpanOrResource(final Map<String, Object> spanAttributes,
                                                          final Map<String, Object> resourceAttributes,
                                                          final String key) {
        String value = getStringAttribute(spanAttributes, key);
        if (value == null && resourceAttributes != null) {
            value = getStringAttribute(resourceAttributes, key);
        }
        return value;
    }

    /**
     * Try to extract resource attributes from the nested structure: spanAttributes["resource"]["attributes"]
     */
    private static Map<String, Object> extractResourceAttributes(final Map<String, Object> spanAttributes) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> resourceAttrs = (Map<String, Object>)
                ((Map<String, Object>) spanAttributes.get("resource")).get("attributes");
            return resourceAttrs;
        } catch (Exception ignored) {
            return null;
        }
    }

    public static String getAwsServiceEnvironment(final Map<String, Object> spanAttributes) {
        try {
            final Map<String, Object> resourceAttributes = extractResourceAttributes(spanAttributes);

            String cloudPlatform = getAttributeFromSpanOrResource(spanAttributes, resourceAttributes, "cloud.platform");
            if ("aws_api_gateway".equals(cloudPlatform)) {
                String stage = getAttributeFromSpanOrResource(spanAttributes, resourceAttributes, "aws.api_gateway.stage");
                return "api-gateway:" + stage;
            }
            if ("aws_ec2".equals(cloudPlatform)) {
                return "ec2:default";
            }
            if ("aws_ecs".equals(cloudPlatform)) {
                String launchType = getAttributeFromSpanOrResource(spanAttributes, resourceAttributes, "aws.ecs.launchtype");
                if ("fargate".equals(launchType)) {
                    return "ecs-fargate:default";
                }
                if ("ec2".equals(launchType)) {
                    return "ecs-ec2:default";
                }
                return "ecs:default";
            }
            if ("aws_eks".equals(cloudPlatform)) {
                return "eks:default";
            }
            if ("aws_elastic_beanstalk".equals(cloudPlatform)) {
                return "elastic-beanstalk:default";
            }
            if ("aws_lambda".equals(cloudPlatform)) {
                return "lambda:default";
            }

            String cloudResourceId = getAttributeFromSpanOrResource(spanAttributes, resourceAttributes, "cloud.resource_id");
            String invokedArn = getAttributeFromSpanOrResource(spanAttributes, resourceAttributes, "aws.lambda.invoked_arn");
            if ((cloudResourceId != null && cloudResourceId.startsWith("arn:aws:lambda:")) || invokedArn != null) {
                return "lambda:default";
            }

            String cloudProvider = resourceAttributes != null ? getStringAttribute(resourceAttributes, "cloud.provider") : null;
            String faasName = resourceAttributes != null ? getStringAttribute(resourceAttributes, "faas.name") : null;
            if (cloudProvider != null && cloudProvider.equals("aws") && faasName != null) {
                return "lambda:default";
            }
        } catch (Exception ignored) {
        }
        return null;
    }
}
