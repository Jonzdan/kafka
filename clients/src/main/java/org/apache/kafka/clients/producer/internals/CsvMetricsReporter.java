/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link MetricsReporter} that periodically writes all producer metrics to a CSV file.
 *
 * <p>Wire it up in your producer config:
 * <pre>
 *   metric.reporters=org.apache.kafka.clients.producer.internals.CsvMetricsReporter
 *   csv.metrics.output.dir=/tmp/kafka-metrics        # directory to write files into
 *   csv.metrics.interval.ms=500                      # how often to flush a row (default 500ms)
 *   csv.metrics.producer.id=producer-1               # label written into every row
 *   csv.metrics.retry.strategy=exponential-full-jitter  # label for the retry strategy under test
 * </pre>
 *
 * <p>One file is created per producer instance, named:
 * <pre>
 *   {output.dir}/{producer.id}_{retry.strategy}_{start-epoch-ms}.csv
 * </pre>
 *
 * <p>Each row has the columns:
 * <pre>
 *   timestamp_ms, producer_id, retry_strategy, group, metric_name, value
 * </pre>
 */
public class CsvMetricsReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(CsvMetricsReporter.class);

    public static final String OUTPUT_DIR_CONFIG      = "csv.metrics.output.dir";
    public static final String INTERVAL_MS_CONFIG     = "csv.metrics.interval.ms";
    public static final String PRODUCER_ID_CONFIG     = "csv.metrics.producer.id";
    public static final String RETRY_STRATEGY_CONFIG  = "csv.metrics.retry.strategy";

    private static final String DEFAULT_OUTPUT_DIR     = "/tmp/kafka-metrics";
    private static final long   DEFAULT_INTERVAL_MS    = 500L;
    private static final String DEFAULT_PRODUCER_ID    = "producer-unknown";
    private static final String DEFAULT_RETRY_STRATEGY = "default";

    private static final String CSV_HEADER =
        "timestamp_ms,producer_id,retry_strategy,group,metric_name,value";

    private final CopyOnWriteArrayList<KafkaMetric> metrics = new CopyOnWriteArrayList<>();

    private String outputDir;
    private long intervalMs;
    private String producerId;
    private String retryStrategy;
    private PrintWriter writer;
    private ScheduledExecutorService scheduler;

    @Override
    public void configure(Map<String, ?> configs) {
        outputDir     = configString(configs, OUTPUT_DIR_CONFIG,     DEFAULT_OUTPUT_DIR);
        intervalMs    = configLong  (configs, INTERVAL_MS_CONFIG,    DEFAULT_INTERVAL_MS);
        producerId    = configString(configs, PRODUCER_ID_CONFIG,    DEFAULT_PRODUCER_ID);
        retryStrategy = configString(configs, RETRY_STRATEGY_CONFIG, DEFAULT_RETRY_STRATEGY);
    }

    @Override
    public void init(List<KafkaMetric> initialMetrics) {
        try {
            Files.createDirectories(Paths.get(outputDir));
        } catch (IOException e) {
            throw new KafkaException("CsvMetricsReporter: could not create output directory: " + outputDir, e);
        }

        String filename = String.format("%s/%s_%s_%d.csv",
            outputDir, sanitize(producerId), sanitize(retryStrategy), System.currentTimeMillis());

        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, /* append= */ false)));
        } catch (IOException e) {
            throw new KafkaException("CsvMetricsReporter: could not open output file: " + filename, e);
        }

        writer.println(CSV_HEADER);
        writer.flush();

        metrics.addAll(initialMetrics);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "csv-metrics-reporter");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::flush, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        log.info("CsvMetricsReporter started — writing to {} every {}ms", filename, intervalMs);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        // Called when a metric is added or updated after init(); just ensure it's tracked.
        if (!metrics.contains(metric))
            metrics.add(metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        metrics.remove(metric);
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        flush();
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        log.info("CsvMetricsReporter closed.");
    }

    @Override
    public void contextChange(MetricsContext metricsContext) { }

    private void flush() {
        if (writer == null) return;

        long nowMs = Instant.now().toEpochMilli();

        // Snapshot the list so metricChange/metricRemoval don't race with iteration
        List<KafkaMetric> snapshot = new ArrayList<>(metrics);

        for (KafkaMetric metric : snapshot) {
            try {
                double value = metric.measurable().measure(metric.config(), nowMs);
                if (Double.isNaN(value) || Double.isInfinite(value)) continue;

                writer.printf("%d,%s,%s,%s,%s,%.6f%n",
                    nowMs,
                    escapeCsv(producerId),
                    escapeCsv(retryStrategy),
                    escapeCsv(metric.metricName().group()),
                    escapeCsv(metric.metricName().name()),
                    value);

            } catch (Exception e) {
                log.debug("CsvMetricsReporter: skipping metric {} due to error: {}",
                    metric.metricName(), e.getMessage());
            }
        }

        writer.flush();
    }

    /** Replace characters unsafe for filenames. */
    private static String sanitize(String s) {
        return s.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    }

    /** Wrap a field in quotes if it contains a comma, quote, or newline. */
    private static String escapeCsv(String s) {
        if (s == null) {
        	return "";
        }
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static String configString(Map<String, ?> configs, String key, String defaultValue) {
        Object v = configs.get(key);
        return v != null ? v.toString() : defaultValue;
    }

    private static long configLong(Map<String, ?> configs, String key, long defaultValue) {
        Object v = configs.get(key);
        if (v == null) {
        	return defaultValue;
        }
        
        try {
        	return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
        	return defaultValue;
        }
    }
}