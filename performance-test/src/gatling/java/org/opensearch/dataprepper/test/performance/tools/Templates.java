/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.javaapi.core.Session;
import org.opensearch.dataprepper.test.data.generation.IpAddress;
import org.opensearch.dataprepper.test.data.generation.UserAgent;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class Templates {
    private static final String APACHE_COMMON_LOG_DATETIME_PATTERN = "dd/LLL/uuuu:HH:mm:ss";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(APACHE_COMMON_LOG_DATETIME_PATTERN);

    private static final Random RANDOM = new Random();
    private static final List<String> HTTP_METHODS = List.of("GET", "HEAD", "PUT", "POST");
    private static final List<String> HTTP_STATUS_CODES = List.of("200", "201", "400", "401", "403", "404", "500");


    public static String now() {
        return FORMATTER.format(LocalDateTime.now()) + " -0700";
    }
    
    public static Function<Session, String> apacheCommonLogTemplate(final int batchSize) {
        return session -> {
            final List<String> logs = IntStream.range(0, batchSize)
                    .mapToObj(i -> "{\"log\": \"" + ipAddress() + " - frank [" + now() + "] \\\"" + httpMethod() + " /apache_pb.gif HTTP/1.0\\\" "+ statusCode() + " " + responseSize() + "\"}")
                    .collect(Collectors.toList());
            final String logArray = String.join(",", logs);
            return "[" + logArray + "]";
        };
    }

    public static Function<Session, String> userAgent(final int batchSize) {
        return session -> {
            final List<String> logs = IntStream.range(0, batchSize)
                    .mapToObj(i -> "{\"log\": \"" + userAgent() + "\"}")
                    .collect(Collectors.toList());
            final String logArray = String.join(",", logs);
            return "[" + logArray + "]";
        };
    }

    private static String ipAddress() {
        return IpAddress.getInstance().ipAddress();
    }

    private static String userAgent() {
        return UserAgent.getInstance().userAgent();
    }

    private static String httpMethod() {
        return randomFromList(HTTP_METHODS);
    }

    private static String statusCode() {
        return randomFromList(HTTP_STATUS_CODES);
    }

    private static int responseSize() {
        return RANDOM.nextInt(2900) + 100;
    }

    private static <T> T randomFromList(final List<T> list) {
        return list.get(RANDOM.nextInt(list.size()));
    }

    private Templates() {
    }
}
