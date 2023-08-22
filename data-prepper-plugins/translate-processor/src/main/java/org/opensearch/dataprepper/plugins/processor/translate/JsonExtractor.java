/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JsonExtractor class is a utility for handling JSON paths and extracting specific fields or objects from
 * JSON structures represented as Java objects. It provides a way to work with nested JSON structures and
 * retrieve the relevant data based on the provided paths.
 */
public class JsonExtractor {

    /**
     * Default delimiter between the fields when providing the path
     */
    private static final String DELIMITER = "/";

    /**
     * @param fullPath full path to the leaf field
     * @return the first field from the full path, returns empty string "" if the path is empty
     */
    public String getRootField(String fullPath) {
        final List<String> fieldsInPath = getFieldsInPath(fullPath);
        return fieldsInPath.isEmpty() ? "" : fieldsInPath.get(0);
    }

    /**
     * @param fullPath full path to the leaf field
     * @return the last field from the full path, returns empty string "" if the path is empty
     */
    public String getLeafField(String fullPath) {
        String strippedPath = getStrippedPath(fullPath);
        final String[] fields = strippedPath.split(DELIMITER);
        return fields.length==0 ? "" : fields[fields.length - 1].strip();
    }

    /**
     * @param fullPath full path to the leaf field
     * @return the path leading up to the lead field, returns empty string "" if there is no parent path
     */
    public String getParentPath(String fullPath) {
        String strippedPath = getStrippedPath(fullPath);
        final String[] fields = strippedPath.split(DELIMITER);
        if (fields.length <= 1) {
            return "";
        }
        return Arrays.stream(fields, 0, fields.length - 1).collect(Collectors.joining(DELIMITER));
    }

    /**
     * @param path full path to the leaf field
     * @param rootObject Java Object in which root field is located. Can be either a List or Map
     * @return all the Java Objects that are associated with the provided path .
     * Path : field1/field2/field3 gives the value of field3.
     */
    public List<Object> getObjectFromPath(String path, Object rootObject) {
        final List<String> fieldsInPath = getFieldsInPath(path);
        if (fieldsInPath.isEmpty()) {
            return List.of(rootObject);
        }
        return getLeafObjects(fieldsInPath, 0, rootObject);
    }

    /**
     * @param path path from one field to another
     * @return the list of fields in the provided path
     */
    private List<String> getFieldsInPath(String path) {
        String strippedPath = getStrippedPath(path);
        if (strippedPath.isEmpty()) {
            return List.of();
        }
        return new ArrayList<>(Arrays.asList(strippedPath.split(DELIMITER)));
    }

    /**
     * @param fieldsInPath list of fields in a path
     * @param level current level inside the nested object with reference to the root level
     * @param rootObject Java Object in which root field is located. Can be either a List or Map
     * @return all the Java Objects that satisfy the fields hierarchy in fieldsInPath
     */
    private List<Object> getLeafObjects(List<String> fieldsInPath, int level, Object rootObject) {
        if (Objects.isNull(rootObject)) {
            return List.of();
        }

        if (rootObject instanceof List) {
            return ((List<?>) rootObject).stream()
                    .flatMap(arrayObject -> getLeafObjects(fieldsInPath, level, arrayObject).stream())
                    .collect(Collectors.toList());
        } else if (rootObject instanceof Map) {
            if (level >= fieldsInPath.size()) {
                return List.of(rootObject);
            } else {
                String field = fieldsInPath.get(level);
                Object outObj = ((Map<?, ?>) rootObject).get(field);
                return getLeafObjects(fieldsInPath, level + 1, outObj);
            }
        }
        return List.of();
    }

    /**
     * @param path path from one field to another
     * @return path stripped of whitespaces
     */
    private String getStrippedPath(String path){
        checkNotNull(path, "path cannot be null");
        return path.strip();
    }
}
