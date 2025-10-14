/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

public interface DBSource {
    String MAXMIND_DATABASE_EXTENSION = ".mmdb";
    void initiateDownload() throws Exception;
}
