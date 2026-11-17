/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class CredentialsCacheTest {
    @Test
    void getOrCreate_creates_once_per_equal_options() {
        final CredentialsCache objectUnderTest = new CredentialsCache();
        final AtomicInteger creations = new AtomicInteger();
        final GoogleCredentials credentials = mock(GoogleCredentials.class);

        final GoogleCredentials first = objectUnderTest.getOrCreate(GcpCredentialsOptions.builder().withProjectId("p").build(), () -> {
            creations.incrementAndGet();
            return credentials;
        });
        final GoogleCredentials second = objectUnderTest.getOrCreate(GcpCredentialsOptions.builder().withProjectId("p").build(), () -> {
            creations.incrementAndGet();
            return mock(GoogleCredentials.class);
        });

        assertThat(first, sameInstance(credentials));
        assertThat(second, sameInstance(credentials));
        assertThat(creations.get(), equalTo(1));
    }
}
