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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ServiceEnvironmentProvidersTest {

    private static Map<String, Object> spanAttributesWithResourceAttributes(final Map<String, Object> resourceAttrs) {
        final Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttrs);
        final Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);
        return spanAttributes;
    }

    /**
     * Verifies that the AWS environment lookup falls back to {@code resource.attributes} when the
     * relevant keys are not present at the span-attribute top level. This is the path that was
     * broken before issue #6786.
     */
    @Nested
    class ResourceAttributeFallback {

    @Test
    void getAwsServiceEnvironment_returnsEc2_whenCloudPlatformInResourceAttributes() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_ec2");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("ec2:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsApiGatewayWithStage_whenBothInResourceAttributes() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_api_gateway");
        resourceAttrs.put("aws.api_gateway.stage", "prod");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("api-gateway:prod", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsLambda_whenInvokedArnInResourceAttributes() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("aws.lambda.invoked_arn", "arn:aws:lambda:us-east-1:123456789012:function:my-fn");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("lambda:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsLambda_whenCloudResourceIdLambdaArnInResourceAttributes() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.resource_id", "arn:aws:lambda:us-east-1:123456789012:function:my-fn");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("lambda:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsEc2_whenCloudPlatformAtTopLevel() {
        // Regression: legacy callers that flatten resource attributes to the top level.
        final Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("cloud.platform", "aws_ec2");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(spanAttributes);

        assertEquals("ec2:default", env);
    }

    @Test
    void getAwsServiceEnvironment_prefersTopLevelOverResource_whenBothPresent() {
        // Span-level attributes take precedence over resource attributes for the same key.
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_api_gateway");

        final Map<String, Object> spanAttributes = spanAttributesWithResourceAttributes(resourceAttrs);
        spanAttributes.put("cloud.platform", "aws_ec2");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(spanAttributes);

        assertEquals("ec2:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsLambda_whenCloudProviderAndFaasNameInResourceAttributes() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.provider", "aws");
        resourceAttrs.put("faas.name", "my-fn");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("lambda:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsNull_whenResourceMapHasNullAttributes() {
        final Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", null);
        final Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(spanAttributes);

        assertNull(env);
    }

    @Test
    void getAwsServiceEnvironment_returnsNull_whenSpanAttributesIsNull() {
        assertNull(ServiceEnvironmentProviders.getAwsServiceEnvironment(null));
    }

    @Test
    void getDeploymentEnvironment_unchangedAfterRefactor() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("deployment.environment.name", "staging");

        final String env = ServiceEnvironmentProviders.getDeploymentEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("staging", env);
    }
    }

    /**
     * Verifies environment derivation for AWS platforms added by issue #6787 (ECS with launch
     * type, EKS, Elastic Beanstalk, and Lambda detected via {@code cloud.platform}).
     */
    @Nested
    class AdditionalAwsPlatforms {

    @Test
    void getAwsServiceEnvironment_returnsEcsFargate_whenCloudPlatformAwsEcsAndLaunchTypeFargate() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_ecs");
        resourceAttrs.put("aws.ecs.launchtype", "fargate");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("ecs-fargate:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsEcsEc2_whenCloudPlatformAwsEcsAndLaunchTypeEc2() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_ecs");
        resourceAttrs.put("aws.ecs.launchtype", "ec2");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("ecs-ec2:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsEcsDefault_whenCloudPlatformAwsEcsAndLaunchTypeMissing() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_ecs");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("ecs:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsEcsDefault_whenCloudPlatformAwsEcsAndLaunchTypeUnknown() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_ecs");
        resourceAttrs.put("aws.ecs.launchtype", "external");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("ecs:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsEks_whenCloudPlatformAwsEks() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_eks");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("eks:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsElasticBeanstalk_whenCloudPlatformAwsElasticBeanstalk() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_elastic_beanstalk");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("elastic-beanstalk:default", env);
    }

    @Test
    void getAwsServiceEnvironment_returnsLambda_whenCloudPlatformAwsLambda() {
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("cloud.platform", "aws_lambda");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(
                spanAttributesWithResourceAttributes(resourceAttrs));

        assertEquals("lambda:default", env);
    }

    @Test
    void getAwsServiceEnvironment_resourceFallbackForLaunchType_whenAwsEcsLaunchtypeInResource() {
        // Platform at top level, launch type only in resource attributes — both must be consulted.
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("aws.ecs.launchtype", "fargate");

        final Map<String, Object> spanAttributes = spanAttributesWithResourceAttributes(resourceAttrs);
        spanAttributes.put("cloud.platform", "aws_ecs");

        final String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(spanAttributes);

        assertEquals("ecs-fargate:default", env);
    }
    }
}
