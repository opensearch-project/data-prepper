/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;

import org.opensearch.dataprepper.plugins.otel.utils.OTelSpanDerivationUtil;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

// TODO : 1. Add new rules as per Producer/Consumers/LocalRoot
@Getter
public class SpanStateData implements Serializable {
    private String serviceName;
    private String spanId;
    private String parentSpanId;
    private String traceId;
    private String spanKind;
    private String spanName;
    private String operation;
    private Long durationInNanos;
    private String status;
    private String endTime;
    private int error;
    private int fault;
    private String operationName;
    private String environment;
    private Map<String, String> groupByAttributes;

    public SpanStateData(final String serviceName,
                         final String spanId,
                         final String parentSpanId,
                         final String traceId,
                         final String spanKind,
                         final String spanName,
                         final String operation,
                         final Long durationInNanos,
                         final String status,
                         final String endTime,
                         final Map<String, String> groupByAttributes,
                         final Map<String, Object> spanAttributes) {
        this.serviceName = serviceName;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.traceId = traceId;
        this.spanKind = spanKind;
        this.spanName = spanName;
        this.operation = operation;
        this.durationInNanos = durationInNanos;
        this.status = status;
        this.endTime = endTime;
        this.groupByAttributes = groupByAttributes != null ? groupByAttributes : Collections.emptyMap();

        OTelSpanDerivationUtil.ErrorFaultResult errorFault = OTelSpanDerivationUtil.computeErrorAndFault(status, spanAttributes);
        this.error = errorFault.getError();
        this.fault = errorFault.getFault();

        this.operationName = OTelSpanDerivationUtil.computeOperationName(spanName, spanAttributes);

        this.environment = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);
    }

    /**
     * Get error indicator
     *
     * @return 1 if span has error, 0 otherwise
     */
    public int getError() {
        return error;
    }

    /**
     * Get fault indicator
     *
     * @return 1 if span has fault, 0 otherwise
     */
    public int getFault() {
        return fault;
    }

    /**
     * Get computed operation name
     *
     * @return Operation name derived using HTTP-aware rules
     */
    public String getOperationName() {
        return operationName;
    }

    /**
     * Get computed environment
     *
     * @return Environment derived from resource attributes
     */
    public String getEnvironment() {
        return environment;
    }

    /**
     * Extract first section from URL path
     *
     * @param path The URL path
     * @return First section of the path (e.g., "/payment/1234" -> "/payment")
     */
    private String extractFirstPathSection(final String path) {
        if (path == null || path.isEmpty()) {
            return "/";
        }

        String normalizedPath = path.startsWith("/") ? path : "/" + path;

        final int secondSlashIndex = normalizedPath.indexOf('/', 1);
        if (secondSlashIndex == -1) {

            return normalizedPath;
        } else {

            return normalizedPath.substring(0, secondSlashIndex);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpanStateData that = (SpanStateData) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(spanId, that.spanId) &&
                Objects.equals(traceId, that.traceId) &&
                Objects.equals(parentSpanId, that.parentSpanId) &&
                Objects.equals(spanKind, that.spanKind) &&
                Objects.equals(spanName, that.spanName) &&
                Objects.equals(operation, that.operation);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, spanKind, spanName, operation);
        result = 31 * result + ((spanId == null) ? 0 : spanId.hashCode());
        result = 31 * result + ((parentSpanId == null) ? 0 : parentSpanId.hashCode());
        result = 31 * result + ((traceId == null) ? 0 : traceId.hashCode());
        return result;
    }

    @VisibleForTesting
    public void setDurationInNanos(Long duration) {
        durationInNanos = duration;
    }

    @VisibleForTesting
    public void setStatus(String status) {
        this.status = status;
    }
}
