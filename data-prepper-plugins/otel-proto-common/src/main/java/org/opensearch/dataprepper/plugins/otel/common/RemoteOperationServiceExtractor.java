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

import java.util.Map;

public interface RemoteOperationServiceExtractor {
    boolean appliesToSpan(Map<String, Object> spanAttributes);
    RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object optionalArg);
}
