/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;

import javax.inject.Named;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;

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

        final Object firstArg = args.get(0);
        final EventKey eventKey;
        if (firstArg instanceof EventKey) {
            eventKey = (EventKey) firstArg;
        } else {
            throw new RuntimeException("Unexpected argument type for first argument: " + firstArg.getClass() +
                    ". Expected EventKey.");
        }

        final String pattern = getStringArg(args.get(1), "pattern");

        ZoneId destinationTimeZone = ZoneOffset.UTC;
        if (args.size() > 2) {
            String destinationZoneIdArg = getStringArg(args.get(2), "destination timezone");
            try {
                destinationTimeZone = ZoneId.of(destinationZoneIdArg);
            } catch (Exception e) {
                throw new IllegalArgumentException("Destination time zone [" + destinationZoneIdArg + "] is invalid");
            }
        }

        ZoneId sourceTimeZone = ZoneOffset.UTC;
        if (args.size() > 3) {
            String sourceTimeZoneArg = getStringArg(args.get(3), "source timezone");
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

    private String getStringArg(Object arg, String argName) {
        if (arg instanceof String) {
            return ((String) arg).trim();
        }
        throw new RuntimeException("Unexpected argument type for " + argName + ": " + arg.getClass() +
                ". Expected String.");
    }
}
