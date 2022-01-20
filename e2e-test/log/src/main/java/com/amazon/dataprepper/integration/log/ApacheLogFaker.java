/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.integration.log;

import com.github.javafaker.Faker;
import com.github.javafaker.Internet;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * This class provides the API to generate fake Apache log.
 */
public class ApacheLogFaker {
    private final String[] HTTP_METHODS = new String[] {"GET", "POST", "DELETE", "PUT"};
    private final String[] FAKE_URIS = new String[] {"/list", "/explore", "/search/tag/list", "/apps/cart.jsp?appID="};
    private final String[] FAKE_STATUS = new String[] {"200", "404", "500", "301"};
    private final String APACHE_LOG_FORMAT = "%s %s %s [%s] \"%s %s HTTP/1.0\" %s %s \"http://%s\" \"%s\"";
    private final Faker faker = new Faker();
    private final Random random = new Random();
    private final DateFormat dateFormat = new SimpleDateFormat("dd/MMM/y:HH:mm:ss Z");

    public String generateRandomApacheLog() {
        final Internet internet = faker.internet();
        final String username = faker.name().username();
        final String password = internet.password();
        final String httpMethod = HTTP_METHODS[random.nextInt(4)];
        final String uri = FAKE_URIS[random.nextInt(4)];
        final String status = FAKE_STATUS[random.nextInt(4)];
        final int bytes = random.nextInt(1001) + 4000;
        return String.format(
                APACHE_LOG_FORMAT,
                internet.ipV4Address(),
                username,
                password,
                dateFormat.format(faker.date().past(100, TimeUnit.DAYS)),
                httpMethod,
                uri,
                status,
                bytes,
                internet.url(),
                internet.userAgentAny()
        );
    }
}
