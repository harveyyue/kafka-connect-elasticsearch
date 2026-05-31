/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch.strategy;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeBasedIndexNamingStrategy extends AbstractIndexNamingStrategy {

  private static final Logger log = LoggerFactory.getLogger(TimeBasedIndexNamingStrategy.class);

  private static final long SECONDS_UPPER_BOUND = 9_999_999_999L;
  private static final long MILLIS_UPPER_BOUND = 9_999_999_999_999L;
  private static final long MICROS_UPPER_BOUND = 9_999_999_999_999_999L;

  private static final String DEFAULT_PATTERN = "yyyyMMdd";

  private String timezone;
  private Map<String, String> dateFieldMap;
  private Map<String, String> patternMap;
  private final Map<String, DateTimeFormatter> formatterCache = new ConcurrentHashMap<>();

  @Override
  public void configure(ElasticsearchSinkConnectorConfig config) {
    super.configure(config);
    this.timezone = config.indexShardDateTimezone();
    this.dateFieldMap = parseMap(config.indexShardDateFieldMap());
    this.patternMap = parseMap(config.indexShardDatePatternMap());

    if (dateFieldMap.isEmpty()) {
      throw new ConfigException(
          ElasticsearchSinkConnectorConfig.INDEX_SHARD_DATE_FIELD_MAP_CONFIG,
          config.indexShardDateFieldMap(),
          "Must be configured (format: topic1:field1,topic2:field2) when using "
              + TimeBasedIndexNamingStrategy.class.getName() + "."
      );
    }
    if (patternMap.isEmpty()) {
      throw new ConfigException(
          ElasticsearchSinkConnectorConfig.INDEX_SHARD_DATE_PATTERN_MAP_CONFIG,
          config.indexShardDatePatternMap(),
          "Must be configured (format: topic1:pattern1,topic2:pattern2) when using "
              + TimeBasedIndexNamingStrategy.class.getName() + "."
      );
    }
  }

  @Override
  public String indexName(SinkRecord sinkRecord) {
    String topic = sinkRecord.topic();
    String dateField = dateFieldMap.get(topic);
    if (dateField == null || dateField.isEmpty()) {
      throw new ConnectException(String.format(
          "Topic '%s' has no date field configured in '%s'. Every topic consumed by this "
              + "task must declare a date field when using %s. Check your connector "
              + "configuration.",
          topic,
          ElasticsearchSinkConnectorConfig.INDEX_SHARD_DATE_FIELD_MAP_CONFIG,
          TimeBasedIndexNamingStrategy.class.getName()
      ));
    }

    String base = topicToIndexCache.computeIfAbsent(topic, this::createIndexName);
    Long timestamp = extractTimestamp(sinkRecord, dateField);
    if (timestamp != null) {
      long millis = normalizeToMillis(timestamp);
      DateTimeFormatter formatter = getFormatter(topic);
      return base + "_" + formatter.format(Instant.ofEpochMilli(millis));
    }

    // Timestamp could not be read. For delete tombstones (null value) fall back
    // to a wildcard pattern so the sink task can resolve the concrete shard by
    // _id before issuing the delete.
    if (sinkRecord.value() == null) {
      return base + IndexNamingStrategy.WILDCARD_SUFFIX;
    }
    return base;
  }

  private DateTimeFormatter getFormatter(String topic) {
    return formatterCache.computeIfAbsent(topic, t -> {
      String pattern = patternMap.getOrDefault(t, DEFAULT_PATTERN);
      return DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of(timezone));
    });
  }

  private static long normalizeToMillis(long timestamp) {
    long abs = Math.abs(timestamp);
    if (abs <= SECONDS_UPPER_BOUND) {
      return timestamp * 1000;
    } else if (abs <= MILLIS_UPPER_BOUND) {
      return timestamp;
    } else if (abs <= MICROS_UPPER_BOUND) {
      return timestamp / 1000;
    } else {
      return timestamp / 1_000_000;
    }
  }

  private static Long extractTimestamp(SinkRecord record, String dateField) {
    if (dateField == null || dateField.isEmpty()) {
      return null;
    }

    Object value = record.value();
    Object fieldValue = null;

    if (value instanceof Struct) {
      fieldValue = ((Struct) value).get(dateField);
    } else if (value instanceof Map) {
      fieldValue = ((Map<String, Object>) value).get(dateField);
    }

    if (fieldValue instanceof Long) {
      return (Long) fieldValue;
    } else if (fieldValue instanceof Integer) {
      return ((Integer) fieldValue).longValue();
    } else if (fieldValue instanceof Number) {
      return ((Number) fieldValue).longValue();
    }

    return null;
  }

  private static Map<String, String> parseMap(String mapStr) {
    Map<String, String> map = new HashMap<>();
    if (mapStr == null || mapStr.trim().isEmpty()) {
      return map;
    }
    for (String entry : mapStr.split(",")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      String[] parts = trimmed.split(":", 2);
      if (parts.length == 2 && !parts[0].trim().isEmpty() && !parts[1].trim().isEmpty()) {
        map.put(parts[0].trim(), parts[1].trim());
      } else {
        log.warn("Invalid map entry: '{}', skipping.", trimmed);
      }
    }
    return map;
  }
}
