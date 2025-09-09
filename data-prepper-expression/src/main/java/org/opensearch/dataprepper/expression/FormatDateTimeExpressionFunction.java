/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.expression;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Named;

import org.opensearch.dataprepper.model.event.Event;

@Named
public class FormatDateTimeExpressionFunction implements ExpressionFunction {

    @Override
    public String getFunctionName() {
        return "formatDateTime";
    }

    @Override
    public Object evaluate(List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() < 2) {
            throw new IllegalArgumentException(getFunctionName() + "() takes at least 2 arguments");
        }
        if (args.size() > 4) {
            throw new IllegalArgumentException(getFunctionName() + "() takes at most 4 arguments");
        }
        List<String> argStrings;
        try {
            argStrings = args.stream()
                    .map(String.class::cast)
                    .map(String::trim)
                    .collect(Collectors.toUnmodifiableList());
        } catch (Exception e) {
            throw new IllegalArgumentException("Arguments in " + getFunctionName() + "() function should be of Json Pointer type or String type");
        }

        String eventKey = argStrings.get(0);
        String pattern = argStrings.get(1);
        pattern = unquote(pattern);


        ZoneId destinationTimeZone = ZoneOffset.UTC;
        if (argStrings.size() > 2) {
            String destinationZoneIdArg = unquote(argStrings.get(2));
            try {
                destinationTimeZone = ZoneId.of(destinationZoneIdArg);
            } catch (Exception e) {
                throw new IllegalArgumentException("Destination time zone [" + destinationZoneIdArg + "] is invalid");
            }
        }

        ZoneId sourceTimeZone = ZoneOffset.UTC;

        if (argStrings.size() > 3) {
            String sourceTimeZoneArg = unquote(argStrings.get(3));
            try {
                sourceTimeZone = ZoneId.of(sourceTimeZoneArg);
            } catch (Exception e) {
                throw new IllegalArgumentException("Source time zone [" + sourceTimeZoneArg + "] is invalid");
            }
        }

        DateTimeFormatter formatter;
        try {
            formatter = DateTimeFormatter.ofPattern(pattern).withZone(destinationTimeZone);
        } catch (Exception e) {
            throw new IllegalArgumentException("Date pattern [" + pattern + "] is invalid");
        }

        Object target = event.get(eventKey, Object.class);
        if (target instanceof Number) {
            return formatter.format(Instant.ofEpochMilli(((Number)target).longValue()).atZone(sourceTimeZone));
        }
        if (target instanceof String) {
            return formatter.format(DateTimeFormatter.ISO_DATE_TIME.withZone(sourceTimeZone).parse((String) target));
        }
        throw new IllegalArgumentException("Unsupported type passed as function argument: " + target.getClass());
    }

    private static String unquote(String input) {
        if (input.startsWith("\"") && input.endsWith("\"")) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }
}
