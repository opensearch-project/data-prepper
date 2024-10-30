/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.opensearch.dataprepper.plugins.source.jira.exception;

/**
 * Exception to indicate unauthorized access.
 * It could either be caused by invalid credentials supplied by the user or failed renew the credentials.
 */
public final class UnAuthorizedException extends RuntimeException {
    public UnAuthorizedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public UnAuthorizedException(final String message) {
        super(message);
    }
}
