/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.opensearch.dataprepper.plugins.processor.dissect.Fields.AppendField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.Field;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.FieldHelper;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.IndirectField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.NormalField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.SkipField;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Dissector {
    private Map<String, SkipField> skipFieldMap;
    private Map<String, NormalField> normalFieldMap;
    private Map<String, IndirectField> indirectFieldMap;
    private Map<String, List<AppendField>> unAppendedFieldsMap;
    private final FieldHelper fieldHelper = new FieldHelper();
    private final LinkedList<Field> fieldsList = new LinkedList<>();
    private final LinkedList<Delimiter> delimiterList = new LinkedList<>();
    private static final Pattern DISSECT_PATTERN = Pattern.compile("%\\{([^}]*)}");

    public Dissector(String dissectPatternString){
        int maxLength = dissectPatternString.length()/3;
        String[] delimiterArray = new String[maxLength];
        String[] fieldsArray = new String[maxLength];

        int lastIndex = 0;
        int fieldIndex = 0;
        int delimIndex = 0;

        Matcher matcher = DISSECT_PATTERN.matcher(dissectPatternString);
        while (matcher.find()) {
            int matchStart = matcher.start();
            int matchEnd = matcher.end();

            delimiterArray[delimIndex] = dissectPatternString.substring(lastIndex, matchStart);
            fieldsArray[fieldIndex] = matcher.group(1);

            delimIndex++;
            fieldIndex++;
            lastIndex = matchEnd;
        }

        if (lastIndex < dissectPatternString.length()) {
            delimiterArray[delimIndex] = dissectPatternString.substring(lastIndex);
        }
        parseFields(fieldsArray);
        parseDelimiters(delimiterArray);
        setFieldsMaps();
    }

    public Map<String, String> dissectText(String text) {
        if (text == null) {
            return null;
        }
        int n = delimiterList.size();
        int[] delimStarts = new int[n];
        int[] delimEnds = new int[n];
        if (!computeDelimiterPositions(text, delimStarts, delimEnds, n)) {
            return null;
        }
        Map<Field, String> localValues = new HashMap<>(); // keyed by identity — same Field instances used for put and get
        Field head = fieldsList.getFirst();
        for (int i = 0; i < n; i++) {
            int fieldStart = 0;
            int fieldEnd = delimStarts[i];
            if (i == 0 && delimStarts[i] == 0) {
                continue;
            }
            if (i > 0) {
                fieldStart = delimEnds[i - 1] + 1;
            }
            if (head == null) {
                return null;
            }
            String val = text.substring(fieldStart, fieldEnd);
            localValues.put(head, head.isStripTrailing() ? val.stripTrailing() : val);
            head = head.getNext();
        }
        if (delimEnds[n - 1] != text.length() - 1) {
            if (head == null) {
                return null;
            }
            String val = text.substring(delimEnds[n - 1] + 1);
            localValues.put(head, head.isStripTrailing() ? val.stripTrailing() : val);
        }
        return getDissectedFields(localValues);
    }

    private Map<String, String> getDissectedFields(Map<Field, String> localValues) {
        final Map<String, String> results = new HashMap<>();
        Map<String, String> appendFieldMap = getAppendedFields(localValues);

        for (NormalField templateField : normalFieldMap.values()) {
            String val = localValues.get(templateField);
            if (val != null) {
                results.put(templateField.getKey(), val);
            }
        }

        for (Map.Entry<String, String> entry : appendFieldMap.entrySet()) {
            results.put(entry.getKey(), entry.getValue());
        }

        for (IndirectField templateField : indirectFieldMap.values()) {
            String templateKey = templateField.getKey();
            String resolvedKey = null;
            if (normalFieldMap.containsKey(templateKey)) {
                resolvedKey = localValues.get(normalFieldMap.get(templateKey));
            } else if (skipFieldMap.containsKey(templateKey)) {
                resolvedKey = localValues.get(skipFieldMap.get(templateKey));
            } else if (appendFieldMap.containsKey(templateKey)) {
                resolvedKey = appendFieldMap.get(templateKey);
            }
            String val = localValues.get(templateField);
            if (resolvedKey != null && val != null) {
                results.put(resolvedKey, val);
            }
        }
        return results;
    }

    private void setFieldsMaps(){
        this.normalFieldMap = Collections.unmodifiableMap(fieldHelper.getNormalFieldMap());
        this.skipFieldMap = Collections.unmodifiableMap(fieldHelper.getSkipFieldMap());
        this.indirectFieldMap = Collections.unmodifiableMap(fieldHelper.getIndirectFieldMap());
        this.unAppendedFieldsMap = Collections.unmodifiableMap(fieldHelper.getAppendFieldMap());
    }

    private void parseFields(String[] fieldsArray){
        for(final String fieldString : fieldsArray){
            if(fieldString==null) {
                return;
            }
            Field field = fieldHelper.getField(fieldString);
            if(fieldsList.size()==0) {
                fieldsList.addLast(field);
            }
            else{
                fieldsList.getLast().setNext(field);
                fieldsList.addLast(field);
            }
        }
    }

    private void parseDelimiters(String[] delimiterArray) {
        for (final String delimiterString : delimiterArray) {
            if (delimiterString == null) {
                return;
            }
            if (delimiterString.length() == 0) {
                continue;
            }
            delimiterList.addLast(new Delimiter(delimiterString));
        }
    }

    private boolean computeDelimiterPositions(String text, int[] starts, int[] ends, int n) {
        int i = 0;
        for (Delimiter delimiter : delimiterList) {
            int prevEnd = 0;
            if (i > 0) {
                prevEnd = ends[i - 1] + 1;
            }
            String delimiterString = delimiter.toString();
            int start = text.indexOf(delimiterString, prevEnd);
            if (start < 0) {
                return false;
            }
            if (delimiterString.trim().isEmpty()) {
                start = start + findLastWhitespaceIndex(text.substring(start), delimiterString.length());
            }
            int end = start + delimiterString.length() - 1;
            if (end > text.length()) {
                return false;
            }
            starts[i] = start;
            ends[i] = end;
            i++;
        }
        return true;
    }

    private Map<String, String> getAppendedFields(Map<Field, String> localValues) {
        final Map<String, String> appendFieldMap = new HashMap<>();
        for (Map.Entry<String, List<AppendField>> entry : unAppendedFieldsMap.entrySet()) {
            List<AppendField> copy = new ArrayList<>(entry.getValue());
            Collections.sort(copy);
            String value = copy.stream()
                    .map(f -> localValues.getOrDefault(f, ""))
                    .collect(Collectors.joining());
            appendFieldMap.put(entry.getKey(), value);
        }
        return appendFieldMap;
    }

    private int findLastWhitespaceIndex(String s, int w) {

        final String[] leadingSpaces = s.split("\\S", 2);

        if (leadingSpaces.length > 0 && leadingSpaces[0].length() >= w) {
            return leadingSpaces[0].length() - w;
        }

        return 0;
    }
}
