/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;
import org.opensearch.dataprepper.plugins.processor.databasedownload.DBSourceOptions;

import java.util.List;

public class DbSourceIdentification {

    private DbSourceIdentification() {
        //TODO
    }

    /**
     * Check for database path is valid S3 URI or not
     * @param uriString uriString
     * @return boolean
     */
    public static boolean isS3Uri(String uriString) {
       //TODO: Logic for URL is S3 URI or not
        //return true if it is valid S3 URI
        return false;
    }

    /**
     * Check for database path is valid S3 URL or not
     * @param urlString urlString
     * @return boolean
     */
    public static boolean isS3Url(String urlString) {
        //TODO: Logic for URL is S3 URL or not
        //return true if it is valid S3 URL
        return false;
    }

    /**
     * Check for database path is valid URL or not
     * @param input input
     * @return boolean
     */
    public static boolean isURL(String input) {
        //TODO: Logic for URL is valid or not
        //return true if it is valid URL
        return false;
    }

    /**
     * Check for database path is local file path or not
     * @param input input
     * @return boolean
     */
    public static boolean isFilePath(String input) {
        //TODO: return true/false if local file path is valid or not
        return false;
    }

    /**
     * Get the databse path options based on input URL
     * @param dbPath dbPath
     * @return DBSourceOptions
     */
    public static DBSourceOptions getDatabasePathType(List<DatabasePathURLConfig> dbPath) {
       //TODO : logic for checking databse path options based on input URL
        return null;
    }
}
