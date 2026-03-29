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

package org.apache.kafka.clients.retry;

/**
 * Interface for retry strategies that calculate the backoff time for retries.
 */
public interface RetryStrategy {

    /**
     * Calculate the backoff time in milliseconds for the given attempt number.
     * Attempt starts from 0 for the first retry.
     *
     * @param attempt the attempt number (0-based)
     * @return the backoff time in milliseconds
     */
    long backoff(int attempt);
}