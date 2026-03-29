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

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RetryStrategyTest {

    @Test
    public void testExponentialBackoff() {
        RetryStrategy strategy = new ExponentialBackoffStrategy(100, 2, 1000, 0.2);
        long backoff0 = strategy.backoff(0);
        assertTrue(backoff0 >= 80 && backoff0 <= 120);
        long backoff1 = strategy.backoff(1);
        assertTrue(backoff1 >= 160 && backoff1 <= 240);
    }

    @Test
    public void testFullJitter() {
        RetryStrategy strategy = new FullJitterStrategy(100, 2, 1000);
        long backoff0 = strategy.backoff(0);
        assertTrue(backoff0 >= 0 && backoff0 <= 100);
        long backoff1 = strategy.backoff(1);
        assertTrue(backoff1 >= 0 && backoff1 <= 200);
    }

    @Test
    public void testFixedDelay() {
        RetryStrategy strategy = new FixedDelayStrategy(500);
        assertTrue(strategy.backoff(0) == 500);
        assertTrue(strategy.backoff(1) == 500);
    }
}