/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.compatibility;

import org.gradle.api.Project;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

public class LastReleasedVersionProvider {
    private static final Pattern VERSION_PATTERN = Pattern.compile("\\d+\\.\\d+\\.\\d+");

    public static String getLastReleasedMajorVersion(final Project project) {
        final String group = project.getGroup().toString();
        final String artifactId = project.getName();
        final String currentVersion = project.getVersion().toString();
        final String groupPath = group.replace('.', '/');
        final String metadataUrl = String.format("https://repo1.maven.org/maven2/%s/%s/maven-metadata.xml", groupPath, artifactId);
        
        try (InputStream stream = new URL(metadataUrl).openStream()) {
            return getLastReleasedMajorVersion(stream, currentVersion);
        } catch (final Exception e) {
            return null;
        }
    }

    static String getLastReleasedMajorVersion(final InputStream metadataStream, final String currentVersion) throws Exception {
        final String majorVersion = currentVersion.split("\\.")[0];
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        dbf.setXIncludeAware(false);
        dbf.setExpandEntityReferences(false);
        final Document doc = dbf
            .newDocumentBuilder()
            .parse(metadataStream);
        
        final NodeList versions = doc.getElementsByTagName("version");
        final List<int[]> matchingVersions = new ArrayList<>();
        
        for (int i = 0; i < versions.getLength(); i++) {
            final String version = versions.item(i).getTextContent();
            if (VERSION_PATTERN.matcher(version).matches() && version.startsWith(majorVersion + ".")) {
                final String[] parts = version.split("\\.");
                matchingVersions.add(new int[]{
                    Integer.parseInt(parts[0]),
                    Integer.parseInt(parts[1]),
                    Integer.parseInt(parts[2])
                });
            }
        }
        
        if (matchingVersions.isEmpty()) {
            return null;
        }
        
        final int[] max = Collections.max(matchingVersions,
                Comparator.comparingInt((int[] a) -> a[0])
                        .thenComparingInt(a -> a[1])
                        .thenComparingInt(a -> a[2]));
        
        return max[0] + "." + max[1] + "." + max[2];
    }
}
