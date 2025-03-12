/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;

import java.util.Optional;

// TODO: remove in 2.0
public class OTelHelper {

    static final String SERVICE_NAME_KEY = "service.name";

    /**
     * Helper method which accepts a OpenTelemetry trace resource object and return the service name object.
     *
     * @param resource resource
     * @return returns service name
     */
    public static Optional<String> getServiceName(final Resource resource) {
        return resource.getAttributesList().stream().filter(
                keyValue -> keyValue.getKey().equals(SERVICE_NAME_KEY)
                        && !keyValue.getValue().getStringValue().isEmpty()
        ).findFirst().map(i -> i.getValue().getStringValue());
    }

    /**
     * Helper method which checks if OpenTelemetry span is valid for processing.
     *
     * @param span span
     * @return returns if the span is valid or not
     */
    public static boolean checkValidSpan(final Span span) {
        return !span.getTraceId().isEmpty() && !span.getSpanId().isEmpty() && !span.getName().isEmpty();
    }
}
