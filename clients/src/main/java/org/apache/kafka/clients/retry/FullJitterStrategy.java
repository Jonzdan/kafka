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

import java.util.concurrent.ThreadLocalRandom;

/**
 * Full jitter backoff.
 * Formula: random(0, initialInterval * multiplier ^ attempt)
 */
public class FullJitterStrategy implements RetryStrategy {
    private final long initialInterval;
    private final int multiplier;
    private final long maxInterval;

    public FullJitterStrategy(long initialInterval, int multiplier, long maxInterval) {
        this.initialInterval = Math.min(maxInterval, initialInterval);
        this.multiplier = multiplier;
        this.maxInterval = maxInterval;
    }

    @Override
    public long backoff(int attempt) {
        double term = initialInterval * Math.pow(multiplier, attempt);
        long maxForAttempt = (long) Math.min(term, maxInterval);
        return ThreadLocalRandom.current().nextLong(0, maxForAttempt + 1);
    }
}