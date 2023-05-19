/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;


import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public class Utility {

    public static StringBuilder getIndexList(OpenSearchSourceConfiguration openSearchSourceConfiguration)
    {
        List<String> include = openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude();
        List<String> exclude = openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude();
        String includeIndexes = null;
        String excludeIndexes = null;
        StringBuilder indexList = new StringBuilder();
        if(!include.isEmpty())
            includeIndexes = include.stream().collect(Collectors.joining(","));
        if(!exclude.isEmpty())
            excludeIndexes = exclude.stream().collect(Collectors.joining(",-*"));
        indexList.append(includeIndexes);
        indexList.append(",-*"+excludeIndexes);
        return indexList;
    }

    @Deprecated
    public static Object createRequest(Class<?> request) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        return request.newInstance();
    }

}