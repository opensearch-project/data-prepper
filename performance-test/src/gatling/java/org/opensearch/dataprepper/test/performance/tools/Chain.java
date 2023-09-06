/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.http.HttpDsl;

public final class Chain {
    private Chain() {
    }

    public static ChainBuilder sendApacheCommonLogPostRequest(final int batchSize) {
        return sendApacheCommonLogPostRequest("Post log", batchSize);
    }

    public static ChainBuilder sendApacheCommonLogPostRequest(final String name, final int batchSize) {
        return CoreDsl.exec(
                HttpDsl.http(name)
                        .post(PathTarget.getPath())
                        .body(CoreDsl.StringBody(Templates.apacheCommonLogTemplate(batchSize))));
    }
}
