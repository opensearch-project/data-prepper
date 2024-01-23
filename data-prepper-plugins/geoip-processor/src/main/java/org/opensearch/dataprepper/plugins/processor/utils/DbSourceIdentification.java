/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSourceOptions;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Implementation of class for checking whether URL type is S3 or file path
 */
public class DbSourceIdentification {

    private DbSourceIdentification() {

    }

    private static String s3DomainPattern = "[a-zA-Z0-9-]+\\.s3\\.amazonaws\\.com";

    /**
     * Check for database path is valid S3 URI or not
     * @param uriString uriString
     * @return boolean
     */
    public static boolean isS3Uri(String uriString) {
        try {
            URI uri = new URI(uriString);
            if (uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("s3")) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * Check for database path is valid S3 URL or not
     * @param urlString urlString
     * @return boolean
     */
    public static boolean isS3Url(String urlString) {
        try {
            URL url = new URL(urlString);
            if (Pattern.matches(s3DomainPattern, url.getHost())) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * Check for database path is valid URL or not
     * @param input input
     * @return boolean
     */
    public static boolean isURL(String input) {
        try {
            URI uri = new URI(input);
            URL url = new URL(input);
            return uri.getScheme() != null && !Pattern.matches(s3DomainPattern, url.getHost()) &&(uri.getScheme().equals("http") || uri.getScheme().equals("https"));
        } catch (URISyntaxException | MalformedURLException e) {
            return false;
        }
    }

    /**
     * Check for database path is local file path or not
     * @param input input
     * @return boolean
     */
    public static boolean isFilePath(String input) {
        return input.startsWith("/") || input.startsWith("./") || input.startsWith("\\") || (input.length() > 1 && input.charAt(1) == ':');
    }

    /**
     * Get the database path options based on input URL
     * @param databasePaths - List of database paths to get databases data from
     * @return DBSourceOptions
     */
    public static DBSourceOptions getDatabasePathType(List<String> databasePaths) {
        DBSourceOptions downloadSourceOptions = null;
        for(final String databasePath : databasePaths) {

            if(DbSourceIdentification.isFilePath(databasePath)) {
                return DBSourceOptions.PATH;
            }
            else if(DbSourceIdentification.isURL(databasePath))
            {
                downloadSourceOptions = DBSourceOptions.URL;
            }
            else if(DbSourceIdentification.isS3Uri(databasePath) || (DbSourceIdentification.isS3Url(databasePath)))
            {
                downloadSourceOptions = DBSourceOptions.S3;
            }
        }
        return downloadSourceOptions;
    }
}
