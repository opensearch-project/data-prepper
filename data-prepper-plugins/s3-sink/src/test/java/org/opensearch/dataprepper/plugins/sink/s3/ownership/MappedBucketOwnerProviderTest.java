package org.opensearch.dataprepper.plugins.sink.s3.ownership;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MappedBucketOwnerProviderTest {
    private Map<String, String> bucketOwnershipMap;
    @Mock
    private BucketOwnerProvider fallbackProvider;

    private MappedBucketOwnerProvider createObjectUnderTest() {
        return new MappedBucketOwnerProvider(bucketOwnershipMap, fallbackProvider);
    }

    @Test
    void constructor_throws_with_null_ownership_map() {
        bucketOwnershipMap = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_fallback() {
        bucketOwnershipMap = Collections.emptyMap();
        fallbackProvider = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Nested
    class WithEmptyOwnersMap {
        private String bucket;
        @BeforeEach
        void setUp() {
            bucketOwnershipMap = Collections.emptyMap();
            bucket = UUID.randomUUID().toString();
        }

        @Test
        void getBucketOwner_returns_owner_from_fallback_when_not_in_map() {
            String fallbackAccount = UUID.randomUUID().toString();
            when(fallbackProvider.getBucketOwner(bucket)).thenReturn(Optional.of(fallbackAccount));

            Optional<String> optionalOwner = createObjectUnderTest().getBucketOwner(bucket);
            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(true));
            assertThat(optionalOwner.get(), equalTo(fallbackAccount));
        }

        @Test
        void getBucketOwner_returns_empty_when_not_in_map_nor_in_fallback() {
            when(fallbackProvider.getBucketOwner(bucket)).thenReturn(Optional.empty());

            Optional<String> optionalOwner = createObjectUnderTest().getBucketOwner(bucket);
            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(false));
        }
    }

    @Nested
    class WithOwnersMap {
        private String knownBucket;
        private String knownBucketAccount;

        @BeforeEach
        void setUp() {
            knownBucket = UUID.randomUUID().toString();
            knownBucketAccount = UUID.randomUUID().toString();
            bucketOwnershipMap = Map.of(
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                    knownBucket, knownBucketAccount,
                    UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }

        @Test
        void getBucketOwner_returns_owner_from_map_when_found() {
            Optional<String> optionalOwner = createObjectUnderTest().getBucketOwner(knownBucket);
            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(true));
            assertThat(optionalOwner.get(), equalTo(knownBucketAccount));
        }

        @Test
        void getBucketOwner_returns_owner_from_fallback_when_not_in_map() {
            String otherBucket = UUID.randomUUID().toString();
            String fallbackAccount = UUID.randomUUID().toString();
            when(fallbackProvider.getBucketOwner(otherBucket)).thenReturn(Optional.of(fallbackAccount));

            Optional<String> optionalOwner = createObjectUnderTest().getBucketOwner(otherBucket);
            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(true));
            assertThat(optionalOwner.get(), equalTo(fallbackAccount));
        }

        @Test
        void getBucketOwner_returns_empty_when_not_in_map_nor_in_fallback() {
            String otherBucket = UUID.randomUUID().toString();
            when(fallbackProvider.getBucketOwner(otherBucket)).thenReturn(Optional.empty());

            Optional<String> optionalOwner = createObjectUnderTest().getBucketOwner(otherBucket);
            assertThat(optionalOwner, notNullValue());
            assertThat(optionalOwner.isPresent(), equalTo(false));
        }
    }
}