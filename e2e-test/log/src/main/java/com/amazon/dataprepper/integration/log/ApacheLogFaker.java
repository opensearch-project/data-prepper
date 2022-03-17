/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.integration.log;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * This class provides the API to generate fake Apache log.
 */
public class ApacheLogFaker {
    private static final String APACHE_LOG_FORMAT = "%s %s %s [%s] \"%s %s HTTP/1.0\" %s %s \"http://%s\" \"%s\"";
    private static final String VALID_PASSWORD_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private final String[] HTTP_METHODS = new String[] {"GET", "POST", "DELETE", "PUT"};
    private final String[] FAKE_URIS = new String[] {"/list", "/explore", "/search/tag/list", "/apps/cart.jsp?appID="};
    private final String[] FAKE_STATUS = new String[] {"200", "404", "500", "301"};
    private final Random random = new Random();
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/y:HH:mm:ss Z");
    private final String[] names;
    private final String[] passwords;
    private final String[] urls;
    private final String[] userAgents;

    public ApacheLogFaker() {
        names = staticNameArray();
        passwords = randomPasswprdArray(20, 8);
        urls = staticUrlArray();
        userAgents = staticUserAgentArray();
    }

    private String[] staticNameArray() {
        return new String[] {
                "rasul",
                "danny",
                "juste",
                "volodislavu",
                "reilly",
                "stas",
                "agapetus",
                "dev",
                "kornelie",
                "mats",
                "komang",
                "mandla",
                "samuil",
                "eastmund",
                "mathias",
                "sion",
                "margarita",
                "amata",
                "klavs",
                "jude",
        };
    }

    private String[] staticUrlArray() {
        return new String[]{
                "amazon.com",
                "opensearch.org",
                "github.com",
                "wikipedia.com",
        };
    }

    private String[] staticUserAgentArray() {
        return new String[] {
                "Mozilla/4.0 (compatible; MSIE 6.0; AOL 9.0; Windows NT 5.1)",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
        };
    }

    private String[] randomPasswprdArray(final int passwordCount, final int passwordSize) {
        String[] randomPasswords = new String[passwordCount];
        for (int i = 0; i < passwordCount; i++) {
            randomPasswords[i] = "";
            for (int n = 0; n < passwordSize; n++) {
                randomPasswords[i] += VALID_PASSWORD_CHARACTERS.charAt(random.nextInt(VALID_PASSWORD_CHARACTERS.length()));
            }
        }
        return randomPasswords;
    }

    private String randomIpV4Address() {
        return random.nextInt(255) + "." +
                random.nextInt(255) + "." +
                random.nextInt(255) + "." +
                random.nextInt(255);
    }

    private String getRandomIn(final String[] array) {
        return array[random.nextInt(array.length)];
    }

    private String randomDate() {
        final Duration offset = Duration.ZERO
                .plusSeconds(random.nextInt(60))
                .plusMinutes(random.nextInt(60))
                .plusHours(random.nextInt(24))
                .plusDays(random.nextInt(365));

        return ZonedDateTime.now().minus(offset).format(dateTimeFormatter);
    }

    public String generateRandomApacheLog() {
        final String ipV4Address = randomIpV4Address();
        final String username = getRandomIn(names);
        final String password = getRandomIn(passwords);
        final String httpMethod = HTTP_METHODS[random.nextInt(4)];
        final String uri = FAKE_URIS[random.nextInt(4)];
        final String status = FAKE_STATUS[random.nextInt(4)];
        final int bytes = random.nextInt(1001) + 4000;
        return String.format(
                APACHE_LOG_FORMAT,
                ipV4Address,
                username,
                password,
                randomDate(),
                httpMethod,
                uri,
                status,
                bytes,
                getRandomIn(urls),
                getRandomIn(userAgents)
        );
    }
}
