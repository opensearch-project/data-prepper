/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.Mock;
import static org.mockito.Mockito.never;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventMetadataModifierTest {
    @Mock
    private Event event;

    @Mock
    private S3ObjectReference s3ObjectReference;
    private String bucketName;
    private String key;

    @BeforeEach
    void setUp() {
        bucketName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
    }

    private EventMetadataModifier createObjectUnderTest(final String metadataRootKey, final Boolean isDeleteS3MetadataInEvent) {
        return new EventMetadataModifier(metadataRootKey, isDeleteS3MetadataInEvent);
    }

    @Test
    void constructor_throws_if_metadataRootKey_is_null() {
        assertThrows(NullPointerException.class, () -> createObjectUnderTest(null, null));
    }

    @ParameterizedTest
    @ArgumentsSource(KeysArgumentsProvider.class)
    void accept_sets_correct_S3_bucket_and_key(final String metadataKey, final String expectedRootKey) {
        when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
        when(s3ObjectReference.getKey()).thenReturn(key);

        createObjectUnderTest(metadataKey, false).accept(event, s3ObjectReference);

        verify(event).put(expectedRootKey + "bucket", bucketName);
        verify(event).put(expectedRootKey + "key", key);
    }

    @ParameterizedTest
    @ArgumentsSource(KeysArgumentsProvider.class)
    void accept_does_not_set_correct_S3_bucket_and_key(final String metadataKey, final String expectedRootKey) {
        createObjectUnderTest(metadataKey, true).accept(event, s3ObjectReference);

        verify(event, never()).put(anyString(), anyString());
        verify(event, never()).put(anyString(), anyString());
    }

    static class KeysArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments("", ""),
                    arguments("/", ""),
                    arguments("s3", "s3/"),
                    arguments("s3/", "s3/"),
                    arguments("/s3", "s3/"),
                    arguments("/s3/", "s3/"),
                    arguments("s3/inner", "s3/inner/"),
                    arguments("s3/inner/", "s3/inner/"),
                    arguments("/s3/inner", "s3/inner/"),
                    arguments("/s3/inner/", "s3/inner/")
            );
        }
    }
}