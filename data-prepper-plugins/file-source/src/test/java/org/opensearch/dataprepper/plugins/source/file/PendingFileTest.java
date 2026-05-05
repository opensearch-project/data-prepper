/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class PendingFileTest {

    @Mock
    private FileIdentity fileIdentity;

    @Test
    void constructorSetsFileIdentity() {
        final Path path = Path.of("/var/log/test.log");
        final PendingFile pendingFile = new PendingFile(fileIdentity, path);

        assertThat(pendingFile.getFileIdentity(), equalTo(fileIdentity));
    }

    @Test
    void constructorSetsPath() {
        final Path path = Path.of("/var/log/test.log");
        final PendingFile pendingFile = new PendingFile(fileIdentity, path);

        assertThat(pendingFile.getPath(), equalTo(path));
    }

    @Test
    void constructorSetsEnqueuedTimeMillis() {
        final long before = System.currentTimeMillis();
        final PendingFile pendingFile = new PendingFile(fileIdentity, Path.of("/tmp/file.log"));
        final long after = System.currentTimeMillis();

        assertThat(pendingFile.getEnqueuedTimeMillis(), greaterThan(0L));
        assertThat(pendingFile.getEnqueuedTimeMillis() >= before, equalTo(true));
        assertThat(pendingFile.getEnqueuedTimeMillis() <= after, equalTo(true));
    }

    @Test
    void constructorThrowsWhenFileIdentityIsNull() {
        assertThrows(NullPointerException.class, () -> new PendingFile(null, Path.of("/tmp/test.log")));
    }

    @Test
    void constructorThrowsWhenPathIsNull() {
        assertThrows(NullPointerException.class, () -> new PendingFile(fileIdentity, null));
    }

    @Test
    void toStringContainsPathAndIdentity() {
        final Path path = Path.of("/var/log/app.log");
        final PendingFile pendingFile = new PendingFile(fileIdentity, path);

        final String result = pendingFile.toString();

        assertThat(result, notNullValue());
        assertThat(result, containsString("path="));
        assertThat(result, containsString("identity="));
    }
}
