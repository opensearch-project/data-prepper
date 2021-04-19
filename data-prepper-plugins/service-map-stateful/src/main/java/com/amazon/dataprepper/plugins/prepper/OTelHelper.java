/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper;

import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;

import java.util.Optional;

public class OTelHelper {

    static final String SERVICE_NAME_KEY = "service.name";

    /**
     * Helper method which accepts a OpenTelemetry trace resource object and return the service name object.
     *
     * @param resource
     * @return
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
     * @param span
     * @return
     */
    public static boolean checkValidSpan(final Span span) {
        return !span.getTraceId().isEmpty() && !span.getSpanId().isEmpty() && !span.getName().isEmpty();
    }
}
