/*
 *
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.common.exceptions;

import java.net.HttpURLConnection;

/**
 * Extension of a GenieException for all precondition failures.
 *
 * @author tgianos
 */
public class GeniePreconditionException extends GenieException {

    /**
     * Constructor.
     *
     * @param msg human readable message
     * @param cause reason for this exception
     */
    public GeniePreconditionException(final String msg, final Throwable cause) {
        super(HttpURLConnection.HTTP_PRECON_FAILED, msg, cause);
    }

    /**
     * Constructor.
     *
     * @param cause reason for this exception
     */
    public GeniePreconditionException(final Throwable cause) {
        super(HttpURLConnection.HTTP_PRECON_FAILED, cause);
    }

    /**
     * Constructor.
     *
     * @param msg human readable message
     */
    public GeniePreconditionException(final String msg) {
        super(HttpURLConnection.HTTP_PRECON_FAILED, msg);
    }
}
