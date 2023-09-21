/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.opensearch.dataprepper.plugins.processor.dissect.Fields.AppendField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.Field;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.FieldHelper;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.IndirectField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.NormalField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.SkipField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    public boolean dissectText(String text){
        try {
            if (!setDelimiterIndexes(text)) {
                return false;
            }
            Field head = fieldsList.getFirst();
            for (final Delimiter delimiter : delimiterList) {
                int fieldStart = 0;
                int fieldEnd = delimiter.getStart();
                if (delimiter.getPrev() == null && delimiter.getStart() == 0) {
                    continue;
                }
                if (delimiter.getPrev() != null || delimiter.getStart() == 0) {
                    fieldStart = delimiter.getPrev().getEnd() + 1;
                }
                head.setValue(text.substring(fieldStart, fieldEnd));
                head = head.getNext();
            }
            if (delimiterList.getLast().getEnd() != text.length() - 1) {
                int fieldStart = delimiterList.getLast().getEnd() + 1;
                int fieldEnd = text.length();
                head.setValue(text.substring(fieldStart, fieldEnd));
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public List<Field> getDissectedFields(){
        final List<Field> dissectedFields = new ArrayList<>();
        Map<String, AppendField> appendFieldMap = getAppendedFields(unAppendedFieldsMap);

        dissectedFields.addAll(normalFieldMap.values());
        dissectedFields.addAll(appendFieldMap.values());

        for(final Field indirectField : indirectFieldMap.values()){
            if(normalFieldMap.containsKey(indirectField.getKey())){
                indirectField.setKey(normalFieldMap.get(indirectField.getKey()).getValue());
            }
            if(skipFieldMap.containsKey(indirectField.getKey())){
                indirectField.setKey(skipFieldMap.get(indirectField.getKey()).getValue());
            }
            if(appendFieldMap.containsKey(indirectField.getKey())){
                indirectField.setKey(appendFieldMap
                                             .get(indirectField.getKey()).getValue());
            }
            dissectedFields.add(indirectField);
        }

        return dissectedFields;
    }

    private void setFieldsMaps(){
        this.normalFieldMap = fieldHelper.getNormalFieldMap();
        this.skipFieldMap = fieldHelper.getSkipFieldMap();
        this.indirectFieldMap = fieldHelper.getIndirectFieldMap();
        this.unAppendedFieldsMap = fieldHelper.getAppendFieldMap();
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
            Delimiter delimiter = new Delimiter(delimiterString);
            if (delimiterList.size() == 0) {
                delimiterList.addLast(delimiter);
            } else {
                delimiterList.getLast().setNext(delimiter);
                delimiter.setPrev(delimiterList.getLast());
                delimiterList.addLast(delimiter);
            }
        }
    }

    private boolean setDelimiterIndexes(String text){
        for (Delimiter delimiter : delimiterList) {
            int prevEnd = 0;
            if (delimiter.getPrev() != null) {
                prevEnd = delimiter.getPrev().getEnd() + 1;
            }
            String delimiterString = delimiter.toString();
            int start = text.indexOf(delimiterString, prevEnd);
            if (delimiterString.trim().isEmpty()) {
                start = start + findLastWhitespaceIndex(text.substring(start), delimiterString.length());
            }
            int end = start + delimiterString.length() -1;
            if (start < 0 || end > text.length()) {
                return false;
            }
            delimiter.setStart(start);
            delimiter.setEnd(end);
        }
        return true;
    }

    private Map<String, AppendField> getAppendedFields(Map<String, List<AppendField>> unAppendedFieldsMap){
        final Map<String, AppendField> appendFieldMap = new HashMap<>();
        for(final String key : unAppendedFieldsMap.keySet()){
            List<AppendField> appendFields = unAppendedFieldsMap.get(key);
            Collections.sort(appendFields);
            String value = appendFields.stream().map(AppendField::getValue).collect(Collectors.joining());
            AppendField sortedField = new AppendField(key);
            sortedField.setValue(value);
            appendFieldMap.put(sortedField.getKey(), sortedField);
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
