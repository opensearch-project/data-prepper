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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Detects the platform a service runs on and the low-cardinality keys that scope its
 * infrastructure, from the resource (and span) attributes of one of its spans. The keys keep their
 * OpenTelemetry semantic-convention names; {@value #PLATFORM_KEY} holds a normalized platform for
 * the cases semconv has no {@code cloud.platform} value for (self-managed Kubernetes, a bare host).
 *
 * Pods, pod UIDs, task ARNs and container ids are never returned: they change on every restart.
 */
public final class ServicePlatformDetector {
    public static final String PLATFORM_KEY = "apm.platform";

    static final List<String> CLOUD_KEYS = List.of("cloud.platform", "cloud.region", "cloud.account.id");
    static final List<String> K8S_KEYS = List.of(
            "k8s.cluster.name", "k8s.namespace.name",
            "k8s.deployment.name", "k8s.statefulset.name", "k8s.daemonset.name",
            "k8s.job.name", "k8s.cronjob.name");
    static final List<String> ECS_KEYS = List.of("aws.ecs.cluster.arn", "aws.ecs.task.family", "aws.ecs.launchtype");
    static final List<String> LAMBDA_KEYS = List.of("faas.name");
    static final String HOST_ID = "host.id";

    private ServicePlatformDetector() {
    }

    /**
     * @param spanAttributes span attributes, with the resource under {@code "resource"} -> {@code "attributes"}
     * @return the platform keys (insertion-ordered), always including {@value #PLATFORM_KEY}
     */
    public static Map<String, String> detect(final Map<String, Object> spanAttributes) {
        final Map<String, Object> resource = resourceAttributes(spanAttributes);
        final Map<String, String> keys = new LinkedHashMap<>();
        final String cloudPlatform = attr(spanAttributes, resource, "cloud.platform");
        final boolean k8s = attr(spanAttributes, resource, "k8s.namespace.name") != null
                || attr(spanAttributes, resource, "k8s.pod.name") != null;

        final String platform;
        if ("aws_eks".equals(cloudPlatform)) {
            platform = "eks";
        } else if (k8s) {
            platform = "kubernetes";
        } else if ("aws_ecs".equals(cloudPlatform)) {
            platform = "ecs";
        } else if ("aws_lambda".equals(cloudPlatform) || attr(spanAttributes, resource, "faas.name") != null) {
            platform = "lambda";
        } else if ("aws_elastic_beanstalk".equals(cloudPlatform)) {
            platform = "elastic_beanstalk";
        } else if ("aws_ec2".equals(cloudPlatform)) {
            platform = "ec2";
        } else if (attr(spanAttributes, resource, HOST_ID) != null) {
            platform = "host";
        } else {
            platform = "generic";
        }
        keys.put(PLATFORM_KEY, platform);
        copy(spanAttributes, resource, CLOUD_KEYS, keys);
        switch (platform) {
            case "eks":
            case "kubernetes":
                copy(spanAttributes, resource, K8S_KEYS, keys);
                break;
            case "ecs":
                copy(spanAttributes, resource, ECS_KEYS, keys);
                break;
            case "lambda":
                copy(spanAttributes, resource, LAMBDA_KEYS, keys);
                break;
            case "ec2":
            case "elastic_beanstalk":
            case "host":
                copy(spanAttributes, resource, List.of(HOST_ID), keys);
                break;
            default:
                break;
        }
        return keys;
    }

    private static void copy(final Map<String, Object> span, final Map<String, Object> resource,
                             final List<String> names, final Map<String, String> into) {
        for (final String name : names) {
            final String value = attr(span, resource, name);
            if (value != null) {
                into.put(name, value);
            }
        }
    }

    private static String attr(final Map<String, Object> span, final Map<String, Object> resource, final String key) {
        Object value = resource != null ? resource.get(key) : null;
        if (value == null && span != null) {
            value = span.get(key);
        }
        if (value == null) {
            return null;
        }
        final String s = value.toString().trim();
        return s.isEmpty() ? null : s;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> resourceAttributes(final Map<String, Object> spanAttributes) {
        if (spanAttributes == null) {
            return Collections.emptyMap();
        }
        final Object resource = spanAttributes.get("resource");
        if (resource instanceof Map) {
            final Object attributes = ((Map<String, Object>) resource).get("attributes");
            if (attributes instanceof Map) {
                return (Map<String, Object>) attributes;
            }
        }
        return Collections.emptyMap();
    }
}
