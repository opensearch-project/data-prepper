/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.types;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class for representing a count or size of bytes.
 *
 * @since 2.1
 */
public class ByteCount {
    private static final Pattern BYTE_PATTERN = Pattern.compile("^(?<value>\\d+\\.?\\d*)(?<unit>[a-z]+)?\\z");
    private static final ByteCount ZERO_BYTES = new ByteCount(0);
    private final long bytes;

    private ByteCount(final long bytes) {
        this.bytes = bytes;
    }

    /**
     * Gets the value as bytes.
     *
     * @return A long representation of the bytes.
     * @since 2.1
     */
    public long getBytes() {
        return bytes;
    }

    private enum Unit {
        BYTE("b", 1),
        KILOBYTE("kb", 1024),
        MEGABYTE("mb", KILOBYTE.multiplier * 1024),
        GIGABYTE("gb", MEGABYTE.multiplier * 1024);

        private final String unitString;
        private final long multiplier;

        private static final Map<String, Unit> UNIT_MAP = Arrays.stream(Unit.values())
                .collect(Collectors.toMap(unit -> unit.unitString, Function.identity()));

        Unit(final String unitString, final long multiplier) {
            this.unitString = unitString;
            this.multiplier = multiplier;
        }

        static Optional<Unit> fromString(final String unitString) {
            return Optional.ofNullable(UNIT_MAP.get(unitString));
        }
    }

    /**
     * Parses a byte string to get the byte count.
     *
     * @param string A valid string representation of the bytes
     * @return The parsed {@link ByteCount}
     * @throws ByteCountParseException thrown if unable to parse the input string for the expected format
     * @throws ByteCountInvalidInputException thrown if the input is parsable but the units or value is invalid
     */
    public static ByteCount parse(final String string) {
        final Matcher matcher = BYTE_PATTERN.matcher(string);
        if(!matcher.find()) {
            throw new ByteCountParseException("Unable to parse bytes provided by '" + string + "'");
        }

        final String valueString = matcher.group("value");
        final String unitString = matcher.group("unit");

        if(unitString == null) {
            throw new ByteCountInvalidInputException("Byte counts must have a unit.");
        }

        final Unit unit = Unit.fromString(unitString)
                .orElseThrow(() -> new ByteCountInvalidInputException("Invalid byte unit: '" + unitString + "'"));

        final BigDecimal valueBigDecimal = new BigDecimal(valueString);

        final BigDecimal byteCount = scaleToBytes(valueBigDecimal, unit);

        if(unit == Unit.BYTE && isFractional(byteCount)) {
            throw new ByteCountInvalidInputException("The byte value '" + string + "' is explicitly declared as a fractional byte which is not allowed.");
        }

        return new ByteCount(byteCount.longValue());
    }

    /**
     * Returns a {@link ByteCount} with the total number of bytes provided.
     *
     * @param bytes The number of bytes
     * @return A new {@link ByteCount}
     * @since 2.7
     */
    public static ByteCount ofBytes(final long bytes) {
        if(bytes < 0)
            throw new IllegalArgumentException("The argument provided for bytes is negative.");

        return new ByteCount(bytes);
    }

    public static ByteCount zeroBytes() {
        return ZERO_BYTES;
    }

    private static BigDecimal scaleToBytes(final BigDecimal value, final Unit unit) {
        return value.multiply(BigDecimal.valueOf(unit.multiplier));
    }

    private static boolean isFractional(final BigDecimal value) {
        return value.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0;
    }
}
