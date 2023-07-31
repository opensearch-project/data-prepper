package org.opensearch.dataprepper.plugins.codec.parquet;


import org.apache.avro.Schema;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParquetSchemaParserFromTabularFormat {

    private static final Pattern pattern = Pattern.compile("\\(([^()]*|)*\\)");

    public static Schema generateSchemaFromTabularString(final String inputString) throws IOException {
        String recordSchemaOutputString = inputString.trim();

        final String tableName = extractTableName(recordSchemaOutputString.split("\\s+"));
        recordSchemaOutputString = getStringFromParanthesis(recordSchemaOutputString);
        recordSchemaOutputString = removeSpaceAndReplaceParanthesis(recordSchemaOutputString);

        final StringBuilder mainSchemaBuilder = new StringBuilder();
        final String baseSchemaStr = "{\"type\":\"record\",\"name\":\"" + tableName + "\",\"fields\":[";
        mainSchemaBuilder.append(baseSchemaStr);
        iterateOverString(mainSchemaBuilder, recordSchemaOutputString);
        mainSchemaBuilder.append("]}");

        return new Schema.Parser().parse(mainSchemaBuilder.toString());
    }

    private static String removeSpaceAndReplaceParanthesis(String recordSchemaOutputString) {
        recordSchemaOutputString = recordSchemaOutputString.replaceAll("\\(", "");
        recordSchemaOutputString = recordSchemaOutputString.replaceAll("\\)", "");

        recordSchemaOutputString = recordSchemaOutputString.trim();

        recordSchemaOutputString = recordSchemaOutputString.replaceAll("\\n", "");
        recordSchemaOutputString = recordSchemaOutputString.replaceAll(",\\s+", ",");
        recordSchemaOutputString = recordSchemaOutputString.replaceAll(">\\s+", ">");
        recordSchemaOutputString = recordSchemaOutputString.replaceAll("<\\s+", "<");
        return recordSchemaOutputString;
    }

    private static String getStringFromParanthesis(String recordSchemaOutputString) {
        final Matcher matcher = pattern.matcher(recordSchemaOutputString);
        if (matcher.find()) {
            recordSchemaOutputString = matcher.group(0);
        }
        return recordSchemaOutputString;
    }

    private static String extractTableName(final String[] words) throws IOException {
        String tableName = null;
        if (words.length >= 2) {
            tableName = words[1];
        } else {
            throw new IOException("Invalid schema string.");
        }
        return tableName;
    }

    private static String buildSchemaStringForArr() {
        return "{\"type\":\"array\", \"items\":\"string\"}";
    }

    private static void iterateOverString(final StringBuilder mainSchemaBuilder, final String recordSchema) {
        boolean isNameStringFormed = false;
        StringBuilder fieldNameBuilder = new StringBuilder();
        StringBuilder fieldTypeBuilder = new StringBuilder();
        boolean isFirstRecordForName = true;

        final char[] schemaStrCharArr = recordSchema.toCharArray();
        int curPosInSchemaStrCharArr = 0;

        while (curPosInSchemaStrCharArr < schemaStrCharArr.length) {
            final char currentCharFromArr = schemaStrCharArr[curPosInSchemaStrCharArr];
            curPosInSchemaStrCharArr++;

            if (!isNameStringFormed) {
                if (currentCharFromArr == ' ') {
                    if (isFirstRecordForName) {
                        mainSchemaBuilder.append("{\"name\":\"" + fieldNameBuilder.toString() + "\",\"type\":\"");
                    } else {
                        mainSchemaBuilder.append(",{\"name\":\"" + fieldNameBuilder.toString() + "\",\"type\":\"");
                    }
                    isNameStringFormed = true;
                    fieldNameBuilder = new StringBuilder();
                    isFirstRecordForName = false;
                    continue;
                }
                fieldNameBuilder.append(currentCharFromArr);
            }

            if (isNameStringFormed) {

                if (currentCharFromArr == ',' || curPosInSchemaStrCharArr == schemaStrCharArr.length) {
                    if (curPosInSchemaStrCharArr == schemaStrCharArr.length) {
                        fieldTypeBuilder.append(currentCharFromArr);
                    }
                    final String type = fieldTypeBuilder.toString().trim() + "\"}";

                    mainSchemaBuilder.append(type);
                    isNameStringFormed = false;
                    fieldTypeBuilder = new StringBuilder();
                    continue;
                }

                fieldTypeBuilder.append(currentCharFromArr);
                // Below block if for logical
                if ("array".equals(fieldTypeBuilder.toString())) {
                    mainSchemaBuilder.deleteCharAt(mainSchemaBuilder.length() - 1);
                    mainSchemaBuilder.append(buildSchemaStringForArr());
                    String structSchemaStr = recordSchema.substring(curPosInSchemaStrCharArr);
                    int openClosedCounter = 0;
                    int structSchemaStrEndBracketPos = 0;

                    for (final char innerChar : structSchemaStr.toCharArray()) {
                        structSchemaStrEndBracketPos++;
                        if (innerChar == '<') {
                            openClosedCounter++;
                        } else if (innerChar == '>') {
                            openClosedCounter--;
                        }
                        if (openClosedCounter == 0) {
                            break;
                        }
                    }

                    mainSchemaBuilder.append("}");
                    curPosInSchemaStrCharArr = curPosInSchemaStrCharArr + structSchemaStrEndBracketPos;
                    if (curPosInSchemaStrCharArr < schemaStrCharArr.length) {
                        // Skip one comma after this
                        curPosInSchemaStrCharArr++;
                    }

                    isNameStringFormed = false;
                    fieldTypeBuilder = new StringBuilder();
                } else if ("<".equals(fieldTypeBuilder.toString())) {
                    mainSchemaBuilder.deleteCharAt(mainSchemaBuilder.length() - 1);
                    curPosInSchemaStrCharArr--;
                    String structSchemaStr = recordSchema.substring(curPosInSchemaStrCharArr);
                    StringBuilder logicalTypeStringBuilder = new StringBuilder();
                    int openClosedCounter = 0;
                    int structSchemaStrEndBracketPos = 0;
                    for (final char innerChar : structSchemaStr.toCharArray()) {
                        structSchemaStrEndBracketPos++;
                        if (innerChar == '<') {
                            openClosedCounter++;
                        } else if (innerChar == '>') {
                            openClosedCounter--;
                        } else {
                            logicalTypeStringBuilder.append(innerChar);
                        }

                        if (openClosedCounter == 0) {
                            break;
                        }
                    }

                    final String[] logicalTypeStr = logicalTypeStringBuilder.toString().split(",");

                    mainSchemaBuilder.append("{");
                    boolean isFirstRec = true;
                    for (final String str : logicalTypeStr) {
                        final String[] innerStr = str.split(":");
                        if (isFirstRec) {
                            mainSchemaBuilder.append("\"" + innerStr[0] + "\": \"" + innerStr[1] + "\"");
                            isFirstRec = false;
                        } else {
                            mainSchemaBuilder.append(",\"" + innerStr[0] + "\": \"" + innerStr[1] + "\"");
                        }
                    }
                    mainSchemaBuilder.append("}}");

                    curPosInSchemaStrCharArr = curPosInSchemaStrCharArr + structSchemaStrEndBracketPos;
                    if (curPosInSchemaStrCharArr < schemaStrCharArr.length) {
                        // Skip one comma after this
                        curPosInSchemaStrCharArr++;
                    }
                    isNameStringFormed = false;
                    fieldTypeBuilder = new StringBuilder();
                }
            }
        }
    }

}
