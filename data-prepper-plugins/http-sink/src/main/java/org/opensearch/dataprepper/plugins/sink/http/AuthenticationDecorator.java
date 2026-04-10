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

import software.amazon.awssdk.http.SdkHttpFullRequest;

public interface AuthenticationDecorator {
    SdkHttpFullRequest authenticate(SdkHttpFullRequest request);
}
