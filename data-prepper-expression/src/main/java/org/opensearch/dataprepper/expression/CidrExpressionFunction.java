/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.expression.ExpressionArgumentsException;
import javax.inject.Named;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named
public class CidrExpressionFunction implements ExpressionFunction {

    private static final String FUNCTION_NAME = "cidrContains";
    @Override
    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    @Override
    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() <= 1) {
            throw new ExpressionArgumentsException(FUNCTION_NAME + "() takes at least two arguments");
        }

        final List<String> argStrings;
        try {
            argStrings = args.stream()
                    .map(arg -> ((String)arg).trim())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new ExpressionArgumentsException(
                    "Arguments in " + FUNCTION_NAME + "() function should be of Json Pointer type or String type");
        }

        final String ipAddressInEvent = event.get(argStrings.get(0), String.class);
        final List<String> cidrBlockStrs = argStrings.subList(1, argStrings.size()).stream()
                .map(str -> str.substring(1, str.length() - 1))
                .collect(Collectors.toList());

        return isIpInCidr(ipAddressInEvent, cidrBlockStrs);
    }

    private boolean isIpInCidr(final String ipAddressStr, final List<String> cidrBlockStrs) {
        if (Objects.isNull(ipAddressStr)) {
            // The IP address field is null or cannot be found in the event
            return false;
        }

        IPAddress address = new IPAddressString(ipAddressStr).getAddress();
        List<IPAddress> cidrBlocks = cidrBlockStrs.stream()
                .map(blockStr -> new IPAddressString(blockStr).getAddress())
                .collect(Collectors.toList());

        for (IPAddress cidrBlock : cidrBlocks) {
            if (cidrBlock.contains(address)) {
                return true;
            }
        }
        return false;
    }
}
