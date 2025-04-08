package org.opensearch.dataprepper.plugins.source.rds.utils;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PgArrayParser {

    public static Object parseTypedArray(String arrayString, PostgresDataType elementType, ValueParser valueParser) {
        char delimiter = getDelimiterForType(elementType);
        Object parsed = PgArrayParser.parseRawArray(arrayString, delimiter);
        if (parsed instanceof List) {
            List<Object> parsedList = (List<Object>) parsed;
            List<Object> answer = parsedList.stream()
                    .map(element -> parseTypedArrayElement(element, elementType, valueParser))
                    .collect(Collectors.toList());
            return answer;
        } else {
            throw new IllegalArgumentException("Unexpected structure from PgArrayParser");
        }
    }

    private static char getDelimiterForType(PostgresDataType elementType) {
        return elementType == PostgresDataType.BOX ? ';' : ',';
    }

    private static Object parseTypedArrayElement(Object arrayElement, PostgresDataType elementType, ValueParser valueParser) {
        if (arrayElement == null) {
            return null;
        } else if (arrayElement instanceof List) {
            List<Object> nestedArray = (List<Object>) arrayElement;
            return nestedArray.stream()
                    .map(element -> parseTypedArrayElement(element, elementType, valueParser))
                    .collect(Collectors.toList());
        } else {
            return valueParser.parse(elementType, arrayElement.toString());
        }
    }

    @FunctionalInterface
    public interface ValueParser {
        Object parse(PostgresDataType elementType, String value);
    }

    public static List<Object> parseRawArray(String arrayString, char delimiter) {
        if (arrayString == null) {
            return null;
        }
        return parseArrayString(arrayString, delimiter);
    }

    private static List<Object> parseArrayString(String arrayString, char delimiter) {
        if (arrayString == null || arrayString.isEmpty()) {
            return new ArrayList<>();
        }
        if (arrayString.charAt(0) != '{' || arrayString.charAt(arrayString.length() - 1) != '}') {
            throw new IllegalArgumentException("Array string must start and end with curly braces.");
        }

        return parseArrayElements(arrayString, 1, arrayString.length() - 1, delimiter);
    }

    private static List<Object> parseArrayElements(String arrayString, int offset, int end, char delimiter) {
        List<Object> result = new ArrayList<>();
        StringBuilder currentElement = new StringBuilder();
        boolean inQuotes = false;
        int nestedLevel = 0;
        boolean escaped = false;

        for (int i = offset; i <= end; i++) {
            char c = (i < end) ? arrayString.charAt(i) : '\0'; // Use '\0' as a sentinel for the end

            if (escaped) {
                currentElement.append(c);
                escaped = false;
                continue;
            }

            if (c == '\\') {
                escaped = true;
                currentElement.append(c);
                continue;
            }

            if (c == '"') {
                if (!inQuotes) {
                    inQuotes = true;
                } else if (i + 1 < end && (arrayString.charAt(i + 1) == delimiter)) {
                    inQuotes = false;
                }
                currentElement.append(c);
                continue;
            }

            if (!inQuotes) {
                nestedLevel = updateNestedLevel(c, nestedLevel);
                if (isElementEnd(c, delimiter, nestedLevel) || c == '\0') {
                    addElement(result, currentElement.toString().trim(), delimiter);
                    currentElement.setLength(0);
                    continue;
                }
            }
            currentElement.append(c);
        }

        if (currentElement.length() > 0) {
            addElement(result, currentElement.toString().trim(), delimiter);
        }

        return result;
    }

    private static int updateNestedLevel(char c, int nestedLevel) {
        if (c == '{') return nestedLevel + 1;
        if (c == '}') return nestedLevel - 1;
        return nestedLevel;
    }

    private static boolean isElementEnd(char c, char delimiter, int nestedLevel) {
        return c == delimiter && nestedLevel == 0;
    }

    private static void addElement(List<Object> result, String element, char delimiter){
        if (element.isEmpty()) {
            return;
        }
        if (element.equalsIgnoreCase("NULL")) {
            result.add(null);
        } else if (element.startsWith("{") && element.endsWith("}")) {
            result.add(parseArrayElements(element, 1, element.length() - 1, delimiter));
        } else {
            result.add(unquoteString(element));
        }
    }

    private static String unquoteString(String s) {
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
            // Unescape any embedded escaped characters
            s = s.replace("\\\\", "\\")
                    .replace("\\\"", "\"")
                    .replace("\\'", "'")
                    .replace("\\,", ",");
        }
        return s;
    }

}
