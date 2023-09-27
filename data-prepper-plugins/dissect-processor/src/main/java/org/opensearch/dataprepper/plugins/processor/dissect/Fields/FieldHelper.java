/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

    public Field getField(String fieldString) {
        if (fieldString == null) {
            return null;
        }
        if(fieldString.trim().isEmpty()){
            return new AppendField("");
        }

        Field field = null;

        Matcher matcher = prefixPattern.matcher(fieldString);
        if (matcher.matches()) {
            final String notation = matcher.group(1);
            final String key = matcher.group(2);
            if (Objects.equals(notation, "+")) {
                field = new AppendField(key);
                setAppendIndex((AppendField) field);
                setStripTrailing(field);
                putInAppendMap((AppendField) field);
            } else if (Objects.equals(notation, "?")) {
                field = new SkipField(key);
                setStripTrailing(field);
                skipFieldMap.put(field.getKey(), (SkipField) field);
            } else if (Objects.equals(notation, "&")) {
                field = new IndirectField(key);
                setStripTrailing(field);
                indirectFieldMap.put(field.getKey(), (IndirectField) field);
            }
        } else {
            field = new NormalField(fieldString);
            setStripTrailing(field);
            normalFieldMap.put(field.getKey(), (NormalField) field);
        }
        return field;
    }

    private void setAppendIndex(AppendField field) {
        String fieldString = field.getKey();
        Matcher matcher = indexPattern.matcher(fieldString);

        if (matcher.find()) {
            String key = fieldString.substring(0, matcher.start());
            int index = Integer.parseInt(matcher.group(1));
            field.setKey(key);
            field.setIndex(index);
        }
    }

    private void setStripTrailing(Field field) {
        if (field == null) {
            return;
        }

        String fieldString = field.getKey();

        Matcher matcher = appendPattern.matcher(fieldString);
        if (matcher.find()) {
            field.setKey(matcher.group(1));
            field.stripTrailing = true;
        }
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
