/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.office365.utils;

/**
 * The type Constants.
 */
public class Constants {
    public static final String PLUGIN_NAME = "office365";
    public static final String MANAGEMENT_API_BASE_URL = "https://manage.office.com/api/v1.0/";
    public static final String[] CONTENT_TYPES = {
            "Audit.AzureActiveDirectory",
            "Audit.Exchange",
            "Audit.SharePoint",
            "Audit.General",
            "DLP.All"
    };
}