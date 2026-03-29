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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.PreparedTxnState;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A wrapper around KafkaProducer that implements custom retry strategies.
 * The underlying producer has retries disabled, and this class handles retries manually.
 */
public class RetryingKafkaProducer<K, V> implements Producer<K, V> {
    private final Producer<K, V> producer;
    private final RetryEngine retryEngine;

    public RetryingKafkaProducer(Properties props, RetryStrategy strategy, int maxRetries) {
        // Disable retries in the producer
        props.put("retries", 0);
        this.producer = new KafkaProducer<>(props);
        this.retryEngine = new RetryEngine(strategy, maxRetries);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        try {
            RecordMetadata metadata = retryEngine.executeAsyncWithRetry(() -> producer.send(record));
            return CompletableFuture.completedFuture(metadata);
        } catch (Exception e) {
            throw new KafkaException("Send failed after retries", e);
        }
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        try {
            RecordMetadata metadata = retryEngine.executeAsyncWithRetry(() -> producer.send(record));
            callback.onCompletion(metadata, null);
            return CompletableFuture.completedFuture(metadata);
        } catch (Exception e) {
            callback.onCompletion(null, e);
            throw new KafkaException("Send with callback failed after retries", e);
        }
    }

    @Override
    public void initTransactions(boolean keepPreparedTxn) {
        producer.initTransactions(keepPreparedTxn);
    }

    @Override
    public void beginTransaction() {
        producer.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) {
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public PreparedTxnState prepareTransaction() {
        return producer.prepareTransaction();
    }

    @Override
    public void commitTransaction() {
        producer.commitTransaction();
    }

    @Override
    public void abortTransaction() {
        producer.abortTransaction();
    }

    @Override
    public void completeTransaction(PreparedTxnState preparedTxnState) {
        producer.completeTransaction(preparedTxnState);
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        producer.registerMetricForSubscription(metric);
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        producer.unregisterMetricFromSubscription(metric);
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return producer.clientInstanceId(timeout);
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void close(Duration timeout) {
        producer.close(timeout);
    }
}