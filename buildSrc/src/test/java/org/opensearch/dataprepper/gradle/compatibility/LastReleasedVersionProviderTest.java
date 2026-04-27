/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.compatibility;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class LastReleasedVersionProviderTest {
    
    @ParameterizedTest
    @CsvSource({
            "2.15.0, 2.14.0",
            "3.1.0, 3.0.0"
    })
    void testGetLastReleasedMajorVersion_findsLatestInMajor(final String currentVersion, final String expectedVersion) throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<metadata>" +
            "<versioning>" +
            "<versions>" +
            "<version>2.9.0</version>" +
            "<version>2.10.0</version>" +
            "<version>2.14.0</version>" +
            "<version>3.0.0</version>" +
            "</versions>" +
            "</versioning>" +
            "</metadata>";
        
        InputStream stream = new ByteArrayInputStream(xml.getBytes());
        String result = LastReleasedVersionProvider.getLastReleasedMajorVersion(stream, currentVersion);
        
        assertThat(result, equalTo(expectedVersion));
    }

    @Test
    void testGetLastReleasedMajorVersion_findsLatestInMajor_singleDigits() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<metadata>" +
            "<versioning>" +
            "<versions>" +
            "<version>2.7.0</version>" +
            "<version>2.8.0</version>" +
            "<version>3.0.0</version>" +
            "</versions>" +
            "</versioning>" +
            "</metadata>";

        InputStream stream = new ByteArrayInputStream(xml.getBytes());
        String result = LastReleasedVersionProvider.getLastReleasedMajorVersion(stream, "2.15.0");

        assertThat(result, equalTo("2.8.0"));
    }

    @Test
    void testGetLastReleasedMajorVersion_noMatchingVersions() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<metadata>" +
            "<versioning>" +
            "<versions>" +
            "<version>1.0.0</version>" +
            "<version>3.0.0</version>" +
            "</versions>" +
            "</versioning>" +
            "</metadata>";
        
        InputStream stream = new ByteArrayInputStream(xml.getBytes());
        String result = LastReleasedVersionProvider.getLastReleasedMajorVersion(stream, "2.0.0");
        
        assertThat(result, nullValue());
    }
    
    @Test
    void testGetLastReleasedMajorVersion_ignoresNonReleaseVersions() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<metadata>" +
            "<versioning>" +
            "<versions>" +
            "<version>2.9.0</version>" +
            "<version>2.15.0-SNAPSHOT</version>" +
            "<version>2.14.0</version>" +
            "</versions>" +
            "</versioning>" +
            "</metadata>";
        
        InputStream stream = new ByteArrayInputStream(xml.getBytes());
        String result = LastReleasedVersionProvider.getLastReleasedMajorVersion(stream, "2.16.0-SNAPSHOT");
        
        assertThat(result, equalTo("2.14.0"));
    }
}
