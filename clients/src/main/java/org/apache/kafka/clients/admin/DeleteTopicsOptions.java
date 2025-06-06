/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import java.util.Collection;

/**
 * Options for {@link Admin#deleteTopics(Collection)}.
 */
public class DeleteTopicsOptions extends AbstractOptions<DeleteTopicsOptions> {

    private boolean retryOnQuotaViolation = true;

    /**
     * Set the timeout in milliseconds for this operation or {@code null} if the default api timeout for the
     * AdminClient should be used.
     *
     */
    // This method is retained to keep binary compatibility with 0.11
    public DeleteTopicsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * Set to true if quota violation should be automatically retried.
     */
    public DeleteTopicsOptions retryOnQuotaViolation(boolean retryOnQuotaViolation) {
        this.retryOnQuotaViolation = retryOnQuotaViolation;
        return this;
    }

    /**
     * Returns true if quota violation should be automatically retried.
     */
    public boolean shouldRetryOnQuotaViolation() {
        return retryOnQuotaViolation;
    }
}
