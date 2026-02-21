/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

import static org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil.getStringAttribute;

import java.util.Map;

public class RemoteOperationAndServiceProviders {
    public final PeerServiceRemoteOperationServiceExtractor PeerServiceRemoteOperationServiceExtractor = new PeerServiceRemoteOperationServiceExtractor();
    public final GraphQlRemoteOperationServiceExtractor GraphQlRemoteOperationServiceExtractor = new GraphQlRemoteOperationServiceExtractor();
    public final MessagingSystemRemoteOperationServiceExtractor MessagingSystemRemoteOperationServiceExtractor = new MessagingSystemRemoteOperationServiceExtractor();
    public final FaasRemoteOperationServiceExtractor FaasRemoteOperationServiceExtractor = new FaasRemoteOperationServiceExtractor();
    public final DbRemoteOperationServiceExtractor DbRemoteOperationServiceExtractor = new DbRemoteOperationServiceExtractor();
    public final DbQueryRemoteOperationServiceExtractor DbQueryRemoteOperationServiceExtractor = new DbQueryRemoteOperationServiceExtractor();
    public final AwsRpcRemoteOperationServiceExtractor AwsRpcRemoteOperationServiceExtractor = new AwsRpcRemoteOperationServiceExtractor();

    private static boolean appliesToSpan(Map<String, Object> spanAttributes, final String attribute1) {
        return getStringAttribute(spanAttributes, attribute1) != null;
    }

    private static boolean appliesToSpan(Map<String, Object> spanAttributes, final String attribute1, final String attribute2) {
        return getStringAttribute(spanAttributes, attribute1) != null || getStringAttribute(spanAttributes, attribute2) != null;
    }

    private static boolean appliesToSpan(Map<String, Object> spanAttributes, final String attribute1, final String attribute2, final String attribute3) {
        return getStringAttribute(spanAttributes, attribute1) != null || getStringAttribute(spanAttributes, attribute2) != null || getStringAttribute(spanAttributes, attribute3) != null;
    }

    public static class PeerServiceRemoteOperationServiceExtractor implements RemoteOperationServiceExtractor {
        public boolean appliesToSpan(Map<String, Object> spanAttributes) {
            return RemoteOperationAndServiceProviders.appliesToSpan(spanAttributes, "peer.service");
        }

        public RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object optionalArg) {
            return new RemoteOperationAndService(null, getStringAttribute(spanAttributes, "peer.service"));
        }
    }

    public static class GraphQlRemoteOperationServiceExtractor implements RemoteOperationServiceExtractor {
        public boolean appliesToSpan(Map<String, Object> spanAttributes) {
            return RemoteOperationAndServiceProviders.appliesToSpan(spanAttributes, "graphql.operation.type");
        }

        public RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object optionalArg) {
            final String graphQLOperation = getStringAttribute(spanAttributes, "graphql.operation.type");
            if (graphQLOperation != null) {
                return new RemoteOperationAndService(graphQLOperation, "graphql");
            }
            return new RemoteOperationAndService(null, null);
        }
    }

    public static class MessagingSystemRemoteOperationServiceExtractor implements RemoteOperationServiceExtractor {
        public boolean appliesToSpan(Map<String, Object> spanAttributes) {
            return RemoteOperationAndServiceProviders.appliesToSpan(spanAttributes, "messaging.system", "messaging.operation");
        }

        public RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object optionalArg) {
            return new RemoteOperationAndService(getStringAttribute(spanAttributes, "messaging.operation"), getStringAttribute(spanAttributes, "messaging.system"));
        }
    }

    public static class FaasRemoteOperationServiceExtractor implements RemoteOperationServiceExtractor {

        public boolean appliesToSpan(Map<String, Object> spanAttributes) {
            return RemoteOperationAndServiceProviders.appliesToSpan(spanAttributes, "faas.invoked_name", "faas.trigger");
        }

        public RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object optionalArg) {
            return new RemoteOperationAndService(getStringAttribute(spanAttributes, "faas.trigger"), getStringAttribute(spanAttributes, "faas.invoked_name"));
        }
    }

    public static class DbRemoteOperationServiceExtractor implements RemoteOperationServiceExtractor {
        public boolean appliesToSpan(Map<String, Object> spanAttributes) {
            return RemoteOperationAndServiceProviders.appliesToSpan(spanAttributes, "db.system", "db.operation", "db.statment");
        }

        public RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object optionalArg) {
            String remoteOperation = getStringAttribute(spanAttributes, "db.operation");
            if (remoteOperation == null) {
                remoteOperation = getStringAttribute(spanAttributes, "db.statement");
            }
            if (remoteOperation != null) {
                remoteOperation = remoteOperation.trim().split("\\s+")[0].toUpperCase();
            }
            return new RemoteOperationAndService(remoteOperation,
                    getStringAttribute(spanAttributes, "db.system"));
        }
    }

    public static class DbQueryRemoteOperationServiceExtractor implements RemoteOperationServiceExtractor {
        public boolean appliesToSpan(Map<String, Object> spanAttributes) {
            return RemoteOperationAndServiceProviders.appliesToSpan(spanAttributes, "db.system.name", "db.operation.name", "db.query.text");
        }

        public RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object optionalArg) {
            String remoteOperation = getStringAttribute(spanAttributes, "db.operation.name");
            if (remoteOperation == null) {
                remoteOperation = getStringAttribute(spanAttributes, "db.query.text");
            }
            if (remoteOperation != null) {
                remoteOperation = remoteOperation.trim().split("\\s+")[0].toUpperCase();
            }
            return new RemoteOperationAndService(remoteOperation, getStringAttribute(spanAttributes, "db.system.name"));
        }
    }

    public static class AwsRpcRemoteOperationServiceExtractor implements RemoteOperationServiceExtractor {
        public boolean appliesToSpan(Map<String, Object> spanAttributes) {
            return RemoteOperationAndServiceProviders.appliesToSpan(spanAttributes, "rpc.service", "rpc.method");
        }

        public RemoteOperationAndService getRemoteOperationAndService(Map<String, Object> spanAttributes, Object awsServiceMappings) {
            String remoteService = null;
            String rpcService = getStringAttribute(spanAttributes, "rpc.service");
            String rpcSystem = getStringAttribute(spanAttributes, "rpc.system");
            if (rpcSystem != null && rpcSystem.equals("aws-api")) {
                remoteService = (String)((Map<String, Object>)awsServiceMappings).getOrDefault(rpcService, "AWS::" + rpcService);
            }
            return new RemoteOperationAndService(getStringAttribute(spanAttributes, "rpc.method"), remoteService);
        }
    }
}

