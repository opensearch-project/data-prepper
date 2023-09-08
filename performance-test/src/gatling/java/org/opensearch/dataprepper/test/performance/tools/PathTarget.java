package org.opensearch.dataprepper.test.performance.tools;

public class PathTarget {
    private static final String PATH_PROPERTY_NAME = "path";
    private static final String DEFAULT_PATH = "/log/ingest";

    public static String getPath() {
        return System.getProperty(PATH_PROPERTY_NAME, DEFAULT_PATH);
    }
}
