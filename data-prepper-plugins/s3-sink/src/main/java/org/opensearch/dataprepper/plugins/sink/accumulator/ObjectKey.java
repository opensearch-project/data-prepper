/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import java.util.regex.Pattern;
import org.opensearch.dataprepper.plugins.s3keyindex.S3ObjectIndexUtility;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Building the path prefix and name pattern.
 *
 */
public class ObjectKey {

    public static final Logger LOG = LoggerFactory.getLogger(ObjectKey.class);
    private static final String DEFAULT_CODEC_FILE_EXTENSION = "json";
    private static final String TIME_PATTERN_REGULAR_EXPRESSION = "\\%\\{.*?\\}";
    private static final Pattern SIMPLE_DURATION_PATTERN = Pattern.compile(TIME_PATTERN_REGULAR_EXPRESSION);

    private ObjectKey(){}

    /**
     * Building path inside bucket based on path_prefix.
     *
     * @param s3SinkConfig s3 sink configuration
     * @return s3 object path
     */
    public static String buildingPathPrefix(final S3SinkConfig s3SinkConfig) {
        String pathPrefix = s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix();
        StringBuilder s3ObjectPath = new StringBuilder();
        if (pathPrefix != null && !pathPrefix.isEmpty()) {
            String[] pathPrefixList = pathPrefix.split("\\/");
            for (String prefixPath : pathPrefixList) {
                if (SIMPLE_DURATION_PATTERN.matcher(prefixPath).find()) {
                    s3ObjectPath.append(S3ObjectIndexUtility.getObjectPathPrefix(prefixPath)).append("/");
                } else {
                    s3ObjectPath.append(prefixPath).append("/");
                }
            }
        }
        return s3ObjectPath.toString();
    }

    /**
     *Get the object file name with the extension
     *
     * @param s3SinkConfig s3 sink configuration
     * @return s3 object name with prefix
     */
    public static String objectFileName(S3SinkConfig s3SinkConfig) {
        String configNamePattern = s3SinkConfig.getBucketOptions().getObjectKeyOptions().getNamePattern();
        int extensionIndex = configNamePattern.lastIndexOf('.');
        if (extensionIndex > 0) {
            return S3ObjectIndexUtility.getObjectNameWithDateTimeId(configNamePattern.substring(0, extensionIndex)) + "."
                    + configNamePattern.substring(extensionIndex + 1);
        } else {
            return S3ObjectIndexUtility.getObjectNameWithDateTimeId(configNamePattern) + "." + DEFAULT_CODEC_FILE_EXTENSION;
        }
    }
}