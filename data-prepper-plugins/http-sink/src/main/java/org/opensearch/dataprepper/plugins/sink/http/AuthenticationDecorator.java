/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http;

import com.linecorp.armeria.common.HttpRequest;

import java.util.List;
import java.util.Map;

public interface AuthenticationDecorator {
    HttpRequest buildRequest(String url, byte[] payload, Map<String, List<String>> customHeaders);
}
