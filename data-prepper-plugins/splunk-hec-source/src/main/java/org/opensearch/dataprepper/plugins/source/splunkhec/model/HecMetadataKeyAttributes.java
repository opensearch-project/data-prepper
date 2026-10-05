/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec.model;

public final class HecMetadataKeyAttributes {

    public static final String INDEX = "splunk_index";
    public static final String SOURCE = "splunk_source";
    public static final String SOURCETYPE = "splunk_sourcetype";
    public static final String HOST = "splunk_host";
    public static final String CHANNEL = "splunk_channel";

    private HecMetadataKeyAttributes() {
    }
}
