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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServicePlatformDetectorTest {

    private static Map<String, Object> withResource(final Map<String, Object> resourceAttributes) {
        final Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);
        final Map<String, Object> span = new HashMap<>();
        span.put("resource", resource);
        return span;
    }

    @Test
    void eks_keeps_cloud_and_kubernetes_keys_but_no_pod() {
        final Map<String, String> keys = ServicePlatformDetector.detect(withResource(Map.of(
                "cloud.platform", "aws_eks", "cloud.region", "us-east-1",
                "k8s.cluster.name", "prod-1", "k8s.namespace.name", "retail",
                "k8s.deployment.name", "pricing", "k8s.pod.name", "pricing-abc", "k8s.pod.uid", "u1")));
        assertEquals("eks", keys.get(ServicePlatformDetector.PLATFORM_KEY));
        assertEquals("prod-1", keys.get("k8s.cluster.name"));
        assertEquals("pricing", keys.get("k8s.deployment.name"));
        assertEquals("us-east-1", keys.get("cloud.region"));
        assertFalse(keys.containsKey("k8s.pod.name"));
        assertFalse(keys.containsKey("k8s.pod.uid"));
    }

    @Test
    void kubernetes_scope_keeps_the_cluster_namespace_and_workload_together() {
        final Map<String, String> keys = ServicePlatformDetector.detect(withResource(Map.of(
                "cloud.platform", "aws_eks", "k8s.cluster.name", "prod-1", "k8s.namespace.name", "retail",
                "k8s.deployment.name", "pricing")));
        assertEquals("prod-1|retail|deployment|pricing", keys.get(ServicePlatformDetector.K8S_SCOPE_KEY));

        final Map<String, String> noWorkload = ServicePlatformDetector.detect(withResource(Map.of(
                "k8s.namespace.name", "shop")));
        assertEquals("|shop||", noWorkload.get(ServicePlatformDetector.K8S_SCOPE_KEY));
    }

    @Test
    void cronjob_pods_keep_the_cronjob_not_the_per_run_job() {
        final Map<String, String> keys = ServicePlatformDetector.detect(withResource(Map.of(
                "k8s.namespace.name", "batch", "k8s.cronjob.name", "nightly",
                "k8s.job.name", "nightly-29012345")));
        assertEquals("nightly", keys.get("k8s.cronjob.name"));
        assertFalse(keys.containsKey("k8s.job.name"));
        assertEquals("|batch|cronjob|nightly", keys.get(ServicePlatformDetector.K8S_SCOPE_KEY));

        final Map<String, String> job = ServicePlatformDetector.detect(withResource(Map.of(
                "k8s.namespace.name", "batch", "k8s.job.name", "migrate")));
        assertEquals("migrate", job.get("k8s.job.name"));
        assertEquals("|batch|job|migrate", job.get(ServicePlatformDetector.K8S_SCOPE_KEY));
    }

    @Test
    void kubernetes_scope_escapes_separators_inside_a_cluster_name() {
        final Map<String, String> keys = ServicePlatformDetector.detect(withResource(Map.of(
                "k8s.cluster.name", "east|1\\a", "k8s.namespace.name", "retail", "k8s.deployment.name", "pricing")));
        assertEquals("east\\|1\\\\a|retail|deployment|pricing", keys.get(ServicePlatformDetector.K8S_SCOPE_KEY));
        assertEquals("east|1\\a", keys.get("k8s.cluster.name"));
    }

    @Test
    void non_kubernetes_platforms_have_no_scope() {
        assertFalse(ServicePlatformDetector.detect(withResource(Map.of("host.id", "h1")))
                .containsKey(ServicePlatformDetector.K8S_SCOPE_KEY));
    }

    @Test
    void self_managed_kubernetes_without_cloud_platform() {
        final Map<String, String> keys = ServicePlatformDetector.detect(withResource(Map.of(
                "k8s.namespace.name", "shop", "k8s.statefulset.name", "inventory")));
        assertEquals("kubernetes", keys.get(ServicePlatformDetector.PLATFORM_KEY));
        assertEquals("inventory", keys.get("k8s.statefulset.name"));
    }

    @Test
    void ec2_keeps_host_id() {
        final Map<String, String> keys = ServicePlatformDetector.detect(withResource(Map.of(
                "cloud.platform", "aws_ec2", "host.id", "i-1", "host.name", "ip-1")));
        assertEquals("ec2", keys.get(ServicePlatformDetector.PLATFORM_KEY));
        assertEquals("i-1", keys.get("host.id"));
        assertFalse(keys.containsKey("host.name"));
    }

    @Test
    void bare_host_and_generic() {
        assertEquals("host", ServicePlatformDetector.detect(withResource(Map.of("host.id", "m-1")))
                .get(ServicePlatformDetector.PLATFORM_KEY));
        assertEquals("generic", ServicePlatformDetector.detect(withResource(Map.of("service.name", "a")))
                .get(ServicePlatformDetector.PLATFORM_KEY));
        assertEquals("generic", ServicePlatformDetector.detect(null).get(ServicePlatformDetector.PLATFORM_KEY));
    }

    @Test
    void ecs_and_lambda_keys() {
        final Map<String, String> ecs = ServicePlatformDetector.detect(withResource(Map.of(
                "cloud.platform", "aws_ecs", "aws.ecs.task.family", "orders", "aws.ecs.task.arn", "arn:task")));
        assertEquals("ecs", ecs.get(ServicePlatformDetector.PLATFORM_KEY));
        assertEquals("orders", ecs.get("aws.ecs.task.family"));
        assertFalse(ecs.containsKey("aws.ecs.task.arn"));
        final Map<String, String> lambda = ServicePlatformDetector.detect(withResource(Map.of(
                "cloud.platform", "aws_lambda", "faas.name", "fn")));
        assertEquals("lambda", lambda.get(ServicePlatformDetector.PLATFORM_KEY));
        assertEquals("fn", lambda.get("faas.name"));
    }

    @Test
    void deployment_first_prefers_the_deployment_environment() {
        final Map<String, Object> span = withResource(Map.of(
                "cloud.platform", "aws_eks", "deployment.environment.name", "production"));
        assertEquals("eks:default", OTelSpanDerivationUtil.computeEnvironment(span));
        assertEquals("eks:default", OTelSpanDerivationUtil.computeEnvironment(span, EnvironmentSource.PLATFORM_FIRST));
        assertEquals("production", OTelSpanDerivationUtil.computeEnvironment(span, EnvironmentSource.DEPLOYMENT_FIRST));
    }

    @Test
    void deployment_first_falls_back_to_the_platform_then_generic() {
        assertEquals("ec2:default", OTelSpanDerivationUtil.computeEnvironment(
                withResource(Map.of("cloud.platform", "aws_ec2")), EnvironmentSource.DEPLOYMENT_FIRST));
        assertEquals("generic:default", OTelSpanDerivationUtil.computeEnvironment(
                withResource(Map.of()), EnvironmentSource.DEPLOYMENT_FIRST));
        assertEquals("staging", OTelSpanDerivationUtil.computeEnvironment(
                withResource(Map.of("deployment.environment", "staging")), EnvironmentSource.DEPLOYMENT_FIRST));
    }

    @Test
    void environment_source_parses_config_values() {
        assertEquals(EnvironmentSource.DEPLOYMENT_FIRST, EnvironmentSource.fromValue("deployment_first"));
        assertEquals(EnvironmentSource.PLATFORM_FIRST, EnvironmentSource.fromValue("PLATFORM_FIRST"));
        assertThrows(IllegalArgumentException.class, () -> EnvironmentSource.fromValue("other"));
    }
}
