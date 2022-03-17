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
                "Rasul",
                "Danny",
                "Juste",
                "Volodislavu",
                "Reilly",
                "Stas",
                "Agapetus",
                "Dev",
                "Kornelie",
                "Mats",
                "Komang",
                "Mandla",
                "Samuil",
                "Eastmund",
                "Mathias",
                "Sion",
                "Margarita",
                "Amata",
                "Klavs",
                "Jude",
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
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)",
                "Mozilla/5.0 (compatible; Googlebot/2.1; +https://www.google.com/bot.html)",
                "curl/7.64.1",
                "PostmanRuntime/7.26.5",
        };
    }

    private String[] randomPasswprdArray(final int passwordCount, final int passwordSize) {
        String[] randomPasswords = new String[passwordCount];
        for (int i = 0; i < passwordCount; i++) {
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
