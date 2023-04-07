/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.client.opensearch._types.ErrorCause;

public class ErrorCauseStringCreator {
    public static String toSingleLineDisplayString(ErrorCause errorCause) {
        ErrorCause currentErrorCause = errorCause;

        StringBuilder errorString = new StringBuilder();
        while (currentErrorCause != null) {
            final String reasonLine = currentErrorCause.reason() != null ? currentErrorCause.reason() : "unknown";
            if (currentErrorCause != errorCause) {
                errorString.append(" caused by ");
            }
            errorString.append(reasonLine);
            currentErrorCause = currentErrorCause.causedBy();
        }

        return errorString.toString();
    }
}
