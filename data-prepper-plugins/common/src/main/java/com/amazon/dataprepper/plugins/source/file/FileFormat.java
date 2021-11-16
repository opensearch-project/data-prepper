package com.amazon.dataprepper.plugins.source.file;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * An enumm to represent the file data types supported in Data Prepper.
 * @since 1.2
 */
@JsonFormat(shape = JsonFormat.Shape.STRING)
public enum FileFormat {

    PLAIN("plain"),
    JSON("json");

    private static final Map<String, FileFormat> NAMES_MAP = Arrays.stream(FileFormat.values())
            .collect(Collectors.toMap(FileFormat::toString, Function.identity()));

    private final String name;

    FileFormat(final String name) {
        this.name = name;
    }

    public String toString() {
        return this.name;
    }

    @JsonValue
    public static FileFormat getByName(final String name) {
        return NAMES_MAP.get(name.toLowerCase());
    }
}