package org.opensearch.dataprepper.expression;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Named;
import org.opensearch.dataprepper.model.event.Event;

@Named
public class DateTimeFormatExpressionFunction implements ExpressionFunction{
    @Override
    public String getFunctionName() {
        return "dateTimeFormat";
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
        String pattern = argStrings.get(1); //TODO: handle Json Pointer?
        pattern = unquote(pattern);


        ZoneId destinationTimeZone = ZoneId.systemDefault();
        if (argStrings.size() > 2) {
            String destinationZoneIdArg = unquote(argStrings.get(2));
            try {
                destinationTimeZone = ZoneId.of(destinationZoneIdArg);
            } catch (Exception e) {
                throw new IllegalArgumentException("Destination time zone [" + destinationZoneIdArg + "] is invalid");
            }
        }

        ZoneId sourceTimeZone = ZoneId.systemDefault();

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
            formatter =  DateTimeFormatter.ofPattern(pattern).withZone(destinationTimeZone); //TODO: Suport Zone as an argument
        } catch (Exception e) {
            throw new IllegalArgumentException("Date pattern [" + pattern + "] is invalid");
        }

        Object target = event.get(eventKey, Object.class);
        if (target instanceof Number) {
            return formatter.format(Instant.ofEpochMilli(((Number)target).longValue()).atZone(sourceTimeZone));
        }
        if (target instanceof String) {
            return formatter.format(Instant.ofEpochMilli((Long.parseLong((String) target))).atZone(sourceTimeZone));
        }
        if (target instanceof TemporalAccessor) {
            return formatter.format((TemporalAccessor) target);
        }
        throw new IllegalArgumentException("Unsupported type passed as function argument");
    }

    private static String unquote(String input) {
        return input.substring(1, input.length() - 1);
    }
}
