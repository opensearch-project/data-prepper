/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;

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
            throw new IllegalArgumentException(FUNCTION_NAME + "() takes at least two arguments");
        }

        final String ipAddressInEvent;
        final Object firstArg = args.get(0);
        if (firstArg instanceof EventKey) {
            ipAddressInEvent = event.get((EventKey) firstArg, String.class);
        } else {
            throw new RuntimeException("Unexpected argument type for first argument: " + firstArg.getClass());
        }

        final List<String> cidrBlockStrs = args.subList(1, args.size()).stream()
                .map(arg -> {
                    if (arg instanceof String) {
                        return (String) arg;
                    } else {
                        throw new RuntimeException("Unexpected argument type: " + arg.getClass());
                    }
                })
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
