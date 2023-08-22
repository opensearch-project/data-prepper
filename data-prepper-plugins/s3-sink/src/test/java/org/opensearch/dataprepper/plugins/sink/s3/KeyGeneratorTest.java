/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyGeneratorTest {
    public static final String OBJECT_KEY_NAME_PATTERN_START = "events-";
    public static final String OBJECT_KEY_NAME_PATTERN = OBJECT_KEY_NAME_PATTERN_START + "%{yyyy-MM-dd'T'hh-mm-ss}";

    @Mock
    private S3SinkConfig s3SinkConfig;

    @Mock
    private ExtensionProvider extensionProvider;

    @Mock
    private ObjectKeyOptions objectKeyOptions;

    @BeforeEach
    void setUp() {
        when(s3SinkConfig.getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(objectKeyOptions.getNamePattern()).thenReturn(OBJECT_KEY_NAME_PATTERN);
    }

    private KeyGenerator createObjectUnderTest() {
        return new KeyGenerator(s3SinkConfig, extensionProvider);
    }

    @Test
    void test_generateKey_with_general_prefix() {
        String pathPrefix = "events/";
        when(s3SinkConfig.getObjectKeyOptions().getPathPrefix()).thenReturn(pathPrefix);
        String key = createObjectUnderTest().generateKey();
        assertNotNull(key);
        assertThat(key, true);
        assertThat(key, key.contains(pathPrefix));
    }

    @Test
    void test_generateKey_with_date_prefix() {
        String pathPrefix = "logdata/";
        String datePattern = "%{yyyy}/%{MM}/%{dd}/";

        DateTimeFormatter fomatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        ZonedDateTime zdt = LocalDateTime.now().atZone(ZoneId.systemDefault())
                .withZoneSameInstant(ZoneId.of(TimeZone.getTimeZone("UTC").getID()));
        String dateString = fomatter.format(zdt);

        when(s3SinkConfig.getObjectKeyOptions()
                .getPathPrefix()).thenReturn(pathPrefix + datePattern);
        String key = createObjectUnderTest().generateKey();
        assertNotNull(key);
        assertThat(key, true);
        assertThat(key, key.contains(pathPrefix + dateString));
    }

    @Test
    void generateKey_ends_with_extension() {
        String extension = UUID.randomUUID().toString();
        when(extensionProvider.getExtension()).thenReturn(extension);
        String pathPrefix = "events/";
        when(s3SinkConfig.getObjectKeyOptions().getPathPrefix()).thenReturn(pathPrefix);
        String key = createObjectUnderTest().generateKey();
        assertThat(key, notNullValue());
        assertThat(key, key.endsWith("." + extension));
    }
}