/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logging;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.message.Message;
import org.slf4j.helpers.MessageFormatter;

import java.util.Arrays;
import java.util.Set;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

@Plugin(name = "sensitiveArgumentMaskingConverter", category = PatternConverter.CATEGORY)
@ConverterKeys({"mask"})
public class SensitiveArgumentMaskingConverter extends LogEventPatternConverter {
    static final Set<String> SENSITIVE_MARKER_NAMES = Set.of(EVENT.getName(), SENSITIVE.getName());
    static final String MASK_PATTERN = "******";

    public static SensitiveArgumentMaskingConverter newInstance() {
        return new SensitiveArgumentMaskingConverter("sensitiveArgument", null);
    }

    protected SensitiveArgumentMaskingConverter(final String name, final String style) {
        super(name, style);
    }

    @Override
    public void format(final LogEvent event, final StringBuilder toAppendTo) {
        final Marker marker = event.getMarker();
        final Message message = event.getMessage();
        // TODO: apply masking on Event type parameter only for EVENT marker.
        if (marker != null && SENSITIVE_MARKER_NAMES.contains(marker.getName())) {
            final Object[] maskPatternArguments = new Object[message.getParameters().length];
            Arrays.fill(maskPatternArguments, MASK_PATTERN);
            toAppendTo.append(MessageFormatter.arrayFormat(message.getFormat(), maskPatternArguments).getMessage());
        } else {
            toAppendTo.append(message.getFormattedMessage());
        }
    }
}
