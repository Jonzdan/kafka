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

import org.apache.kafka.common.errors.RetriableException;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Retry engine that executes operations with configurable retry strategies.
 */
public class RetryEngine {
    private final RetryStrategy strategy;
    private final int maxRetries;

    public RetryEngine(RetryStrategy strategy, int maxRetries) {
        this.strategy = strategy;
        this.maxRetries = maxRetries;
    }

    /**
     * Execute a callable operation with retries on RetriableException.
     */
    public <T> T executeWithRetry(Callable<T> operation) throws Exception {
        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return operation.call();
            } catch (RetriableException e) {
                lastException = e;
                if (attempt < maxRetries) {
                    long backoff = strategy.backoff(attempt);
                    Thread.sleep(backoff);
                }
            } catch (Exception e) {
                // Non-retriable exception, rethrow immediately
                throw e;
            }
        }
        throw lastException;
    }

    /**
     * Execute an async operation that returns a Future, with retries on retriable exceptions.
     * Assumes the operation is synchronous in the sense that we wait for the future.
     */
    public <T> T executeAsyncWithRetry(Callable<Future<T>> operation) throws Exception {
        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                Future<T> future = operation.call();
                return future.get(); // Wait for completion
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RetriableException) {
                    lastException = (Exception) cause;
                    if (attempt < maxRetries) {
                        long backoff = strategy.backoff(attempt);
                        Thread.sleep(backoff);
                    }
                } else {
                    // Non-retriable, rethrow
                    throw e;
                }
            } catch (Exception e) {
                // Other exceptions, rethrow
                throw e;
            }
        }
        throw lastException;
    }
}