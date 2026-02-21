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

public class ServiceEnvironmentProviders {
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

    public static String getAwsServiceEnvironment(final Map<String, Object> spanAttributes) {
        try {
            String cloudPlatform = getStringAttribute(spanAttributes, "cloud.platform");
            if (cloudPlatform != null && cloudPlatform.equals("aws_api_gateway")) {
                return "api-gateway:"+getStringAttribute(spanAttributes, "aws.api_gateway.stage");
            }
            if (cloudPlatform != null && cloudPlatform.equals("aws_ec2")) {
                return "ec2:default";
            }
            String cloudResourceId = getStringAttribute(spanAttributes, "cloud.resource_id");
            String invokedArn = getStringAttribute(spanAttributes, "aws.lambda.invoked_arn");
            if (cloudResourceId != null && cloudResourceId.startsWith("arn:aws:lambda:") || invokedArn != null) {
                return "lambda:default";
            }
        
            @SuppressWarnings("unchecked")
            Map<String, Object> resourceAttributes = (Map<String, Object>)
                ((Map<String, Object>) spanAttributes.get("resource")).get("attributes");
            String cloudProvider = getStringAttribute(resourceAttributes, "cloud.provider");
            String faasName = getStringAttribute(resourceAttributes, "faas.name");
            if (cloudProvider != null && cloudProvider.equals("aws") && faasName != null) {
                return "lambda:default";
            }
        } catch (Exception ignored) {
        }
        return null;
    }
}
