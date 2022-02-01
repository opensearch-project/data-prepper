/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.javaapi.core.Session;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public final class Templates {
    public static final String APACHE_COMMON_LOG_DATETIME_PATTERN = "d/LLL/uuuu:HH:mm:ss";
    public static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(APACHE_COMMON_LOG_DATETIME_PATTERN);

    public static String now() {
        return formatter.format(LocalDateTime.now()) + " -0700";
    }
    
    public static Function<Session, String> apacheCommonLogTemplate(final int batchSize) {
        return session -> {
            final String log = "{\"log\": \"127.0.0.1 - frank [" + now() + "] \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\"}";
            final List<String> logs = Collections.nCopies(batchSize, log);
            final String logArray = String.join(",", logs);
            return "[" + logArray + "]";
        };
    }

    private Templates() {
    }
}
