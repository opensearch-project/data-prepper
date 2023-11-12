package org.opensearch.dataprepper.plugins.source.dynamodb.utils;

import software.amazon.awssdk.arns.Arn;

public class TableUtil {

    public static String getTableNameFromArn(String tableArn) {
        Arn arn = Arn.fromString(tableArn);
        // resourceAsString is table/xxx
        return arn.resourceAsString().substring("table/".length());
    }

    public static String getTableArnFromStreamArn(String streamArn) {
        // e.g. Given a stream arn: arn:aws:dynamodb:us-west-2:xxx:table/test-table/stream/2023-07-31T04:59:58.190
        // Returns arn:aws:dynamodb:us-west-2:xxx:table/test-table
        return streamArn.substring(0, streamArn.lastIndexOf('/') - "stream/".length());
    }

    public static String getTableArnFromExportArn(String exportArn) {
        // e.g. given export arn:arn:aws:dynamodb:us-west-2:123456789012:table/Thread/export/01693291918297-bfeccbea
        // returns: arn:aws:dynamodb:us-west-2:123456789012:table/Thread
        return exportArn.substring(0, exportArn.lastIndexOf("/export/"));
    }


}
