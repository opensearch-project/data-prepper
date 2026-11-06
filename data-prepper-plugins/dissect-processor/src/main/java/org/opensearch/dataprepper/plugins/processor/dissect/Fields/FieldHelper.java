/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect.Fields;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldHelper {
    private final Pattern appendPattern = Pattern.compile("^(.*?)->");
    private final Pattern prefixPattern = Pattern.compile("([+&?])(.+)");
    private final Pattern indexPattern = Pattern.compile("/(\\d+)$");
    private final Map<String, SkipField> skipFieldMap = new HashMap<>();
    private final Map<String, NormalField> normalFieldMap = new HashMap<>();
    private final Map<String, IndirectField> indirectFieldMap = new HashMap<>();
    private final Map<String, List<AppendField>> appendFieldMap = new HashMap<>();

    public Map<String, SkipField> getSkipFieldMap(){
        return skipFieldMap;
    }
    public Map<String, NormalField> getNormalFieldMap(){
        return normalFieldMap;
    }
    public Map<String, IndirectField> getIndirectFieldMap(){
        return indirectFieldMap;
    }
    public Map<String, List<AppendField>> getAppendFieldMap(){
        return appendFieldMap;
    }

    public Field getField(String fieldString, Field lastField) {
        if (fieldString == null) {
            return null;
        }
        if(fieldString.trim().isEmpty()){
            AppendField emptyField = new AppendField("");
            if (lastField != null) {
                lastField.setNext(emptyField);
            }
            return emptyField;
        }

        Field field = null;

        Matcher matcher = prefixPattern.matcher(fieldString);
        if (matcher.matches()) {
            final String notation = matcher.group(1);
            String key = matcher.group(2);
            if (Objects.equals(notation, "+")) {
                Matcher indexMatcher = indexPattern.matcher(key);
                int index = -1;
                if (indexMatcher.find()) {
                    index = Integer.parseInt(indexMatcher.group(1));
                    key = key.substring(0, indexMatcher.start());
                }
                Matcher trailingMatcher = appendPattern.matcher(key);
                boolean stripTrailing = trailingMatcher.find();
                if (stripTrailing) {
                    key = trailingMatcher.group(1);
                }
                field = new AppendField(key);
                if (index >= 0) ((AppendField) field).setIndex(index);
                field.stripTrailing = stripTrailing;
                putInAppendMap((AppendField) field);
            } else if (Objects.equals(notation, "?")) {
                Matcher trailingMatcher = appendPattern.matcher(key);
                boolean stripTrailing = trailingMatcher.find();
                if (stripTrailing) {
                    key = trailingMatcher.group(1);
                }
                field = new SkipField(key);
                field.stripTrailing = stripTrailing;
                skipFieldMap.put(field.getKey(), (SkipField) field);
            } else if (Objects.equals(notation, "&")) {
                Matcher trailingMatcher = appendPattern.matcher(key);
                boolean stripTrailing = trailingMatcher.find();
                if (stripTrailing) {
                    key = trailingMatcher.group(1);
                }
                field = new IndirectField(key);
                field.stripTrailing = stripTrailing;
                indirectFieldMap.put(field.getKey(), (IndirectField) field);
            }
        } else {
            Matcher trailingMatcher = appendPattern.matcher(fieldString);
            boolean stripTrailing = trailingMatcher.find();
            String key = stripTrailing ? trailingMatcher.group(1) : fieldString;
            field = new NormalField(key);
            field.stripTrailing = stripTrailing;
            normalFieldMap.put(field.getKey(), (NormalField) field);
        }
        if (lastField != null && field != null) {
            lastField.setNext(field);
        }
        return field;
    }

    private void putInAppendMap(AppendField field) {
        String key = field.getKey();
        if (appendFieldMap.containsKey(key)) {
            appendFieldMap.get(key).add(field);
        } else {
            List<AppendField> appendFields = new ArrayList<>();
            appendFields.add(field);
            appendFieldMap.put(key, appendFields);
        }
    }

}
