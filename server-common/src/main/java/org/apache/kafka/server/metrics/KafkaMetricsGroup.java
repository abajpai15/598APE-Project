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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.utils.Sanitizer;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaMetricsGroup {
    private final String pkg;
    private final String simpleName;

    public KafkaMetricsGroup(Class<?> klass) {
        this(klass.getPackage() == null ? "" : klass.getPackage().getName(), klass.getSimpleName().replaceAll("\\$$", ""));
    }

    /**
     * This constructor allows caller to build metrics name with custom package and class name. This is useful to keep metrics
     * compatibility in migrating scala code, since the difference of either package or class name will impact the mbean name and
     * that will break the backward compatibility.
     */
    public KafkaMetricsGroup(String packageName, String simpleName) {
        this.pkg = packageName;
        this.simpleName = simpleName;
    }

    /**
     * Creates a new MetricName object for gauges, meters, etc. created for this
     * metrics group.
     * @param name Descriptive name of the metric.
     * @param tags Additional attributes which mBean will have.
     * @return Sanitized metric name object.
     */
    public MetricName metricName(String name, Map<String, String> tags) {
        return explicitMetricName(this.pkg, this.simpleName, name, tags);
    }

    private static MetricName explicitMetricName(String group, String typeName,
                                                String name, Map<String, String> tags) {
        StringBuilder nameBuilder = new StringBuilder(100);
        nameBuilder.append(group);
        nameBuilder.append(":type=");
        nameBuilder.append(typeName);

        if (!name.isEmpty()) {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }

        String scope = toScope(tags).orElse(null);
        Optional<String> tagsName = toMBeanName(tags);
        tagsName.ifPresent(s -> nameBuilder.append(",").append(s));

        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    public <T> Gauge<T> newGauge(String name, Supplier<T> metric, Map<String, String> tags) {
        return newGauge(metricName(name, tags), metric);
    }

    public <T> Gauge<T> newGauge(String name, Supplier<T> metric) {
        return newGauge(name, metric, Map.of());
    }

    public <T> Gauge<T> newGauge(MetricName name, Supplier<T> metric) {
        return KafkaYammerMetrics.defaultRegistry().newGauge(name, new Gauge<T>() {
            @Override
            public T value() {
                return metric.get();
            }
        });
    }

    public final Meter newMeter(String name, String eventType,
                                TimeUnit timeUnit, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit);
    }

    public final Meter newMeter(String name, String eventType,
                                TimeUnit timeUnit) {
        return newMeter(name, eventType, timeUnit, Map.of());
    }

    public final Meter newMeter(MetricName metricName, String eventType, TimeUnit timeUnit) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName, eventType, timeUnit);
    }

    public final Histogram newHistogram(String name, boolean biased, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased);
    }

    public final Histogram newHistogram(String name) {
        return newHistogram(name, true, Map.of());
    }

    public final Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit);
    }

    public final Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit) {
        return newTimer(name, durationUnit, rateUnit, Map.of());
    }

    public final Timer newTimer(MetricName metricName, TimeUnit durationUnit, TimeUnit rateUnit) {
        return KafkaYammerMetrics.defaultRegistry().newTimer(metricName, durationUnit, rateUnit);
    }

    public final void removeMetric(String name, Map<String, String> tags) {
        KafkaYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags));
    }

    public final void removeMetric(String name) {
        removeMetric(name, Map.of());
    }

    public final void removeMetric(MetricName metricName) {
        KafkaYammerMetrics.defaultRegistry().removeMetric(metricName);
    }

    private static Optional<String> toMBeanName(Map<String, String> tags) {
        List<Map.Entry<String, String>> filteredTags = tags.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .toList();
        if (!filteredTags.isEmpty()) {
            String tagsString = filteredTags.stream()
                    .map(entry -> entry.getKey() + "=" + Sanitizer.jmxSanitize(entry.getValue()))
                    .collect(Collectors.joining(","));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }

    private static Optional<String> toScope(Map<String, String> tags) {
        List<Map.Entry<String, String>> filteredTags = tags.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .toList();
        if (!filteredTags.isEmpty()) {
            // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
            String tagsString = filteredTags.stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + "." + entry.getValue().replaceAll("\\.", "_"))
                    .collect(Collectors.joining("."));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }
}
