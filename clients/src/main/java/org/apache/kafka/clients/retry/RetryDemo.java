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

/**
 * Demo class to show the retry strategies in action.
 * Run this to see the backoff calculations and retry behavior.
 */
public class RetryDemo {

    public static void main(String[] args) {
        System.out.println("=== Kafka Custom Retry Engine Demo ===\n");

        // Demo 1: Show backoff calculations for each strategy
        System.out.println("1. Backoff Calculations:");
        demoBackoffCalculations();

        System.out.println("\n" + "=".repeat(50) + "\n");

        // Demo 2: Show retry engine with simulated failures
        System.out.println("2. Retry Engine with Simulated Failures:");
        demoRetryEngine();

        System.out.println("\n" + "=".repeat(50) + "\n");

        // Demo 3: Show producer wrapper usage
        System.out.println("3. Producer Wrapper (simulated):");
        demoProducerWrapper();
    }

    private static void demoBackoffCalculations() {
        RetryStrategy[] strategies = {
            new ExponentialBackoffStrategy(100, 2, 10000, 0.2),
            new FullJitterStrategy(100, 2, 10000),
            new FixedDelayStrategy(500)
        };

        String[] names = {"ExponentialBackoff", "FullJitter", "FixedDelay"};

        for (int i = 0; i < strategies.length; i++) {
            System.out.println("\n" + names[i] + " Strategy:");
            for (int attempt = 0; attempt < 5; attempt++) {
                long backoff = strategies[i].backoff(attempt);
                System.out.printf("  Attempt %d: %d ms%n", attempt, backoff);
            }
        }
    }

    private static void demoRetryEngine() {
        // Simulate an operation that fails the first 2 times
        final int[] callCount = {0};

        Callable<String> failingOperation = () -> {
            callCount[0]++;
            if (callCount[0] <= 2) {
                System.out.println("  Operation failed (attempt " + callCount[0] + ")");
                throw new RetriableException("Simulated retriable failure") {};
            }
            System.out.println("  Operation succeeded (attempt " + callCount[0] + ")");
            return "Success!";
        };

        // Test with different strategies
        RetryStrategy[] strategies = {
            new ExponentialBackoffStrategy(100, 2, 1000, 0.2),
            new FullJitterStrategy(100, 2, 1000),
            new FixedDelayStrategy(200)
        };

        String[] names = {"ExponentialBackoff", "FullJitter", "FixedDelay"};

        for (int i = 0; i < strategies.length; i++) {
            System.out.println("\nTesting " + names[i] + " Strategy:");
            callCount[0] = 0; // Reset counter

            RetryEngine engine = new RetryEngine(strategies[i], 5);

            try {
                long startTime = System.currentTimeMillis();
                String result = engine.executeWithRetry(failingOperation);
                long endTime = System.currentTimeMillis();

                System.out.println("  Result: " + result);
                System.out.println("  Total time: " + (endTime - startTime) + " ms");
            } catch (Exception e) {
                System.out.println("  Final failure: " + e.getMessage());
            }
        }
    }

    private static void demoProducerWrapper() {
        System.out.println("This would create a RetryingKafkaProducer in a real application:");
        System.out.println();
        System.out.println("Properties props = new Properties();");
        System.out.println("props.put(\"bootstrap.servers\", \"localhost:9092\");");
        System.out.println("props.put(\"key.serializer\", \"org.apache.kafka.common.serialization.StringSerializer\");");
        System.out.println("props.put(\"value.serializer\", \"org.apache.kafka.common.serialization.StringSerializer\");");
        System.out.println();
        System.out.println("RetryStrategy strategy = new FullJitterStrategy(100, 2, 1000);");
        System.out.println("RetryingKafkaProducer<String, String> producer = new RetryingKafkaProducer<>(props, strategy, 3);");
        System.out.println();
        System.out.println("// Send with automatic retries on network errors");
        System.out.println("producer.send(new ProducerRecord<>(\"topic\", \"key\", \"value\"));");
        System.out.println("producer.close();");
    }
}