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

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.INDEX_SHARD_DATE_FIELD_MAP_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.INDEX_SHARD_DATE_PATTERN_MAP_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.INDEX_SHARD_DATE_TIMEZONE_CONFIG;
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimeBasedIndexNamingStrategyTest {

  // 2025-05-08 00:00:00 UTC
  private static final long TS_MILLIS = 1746662400000L;
  private static final long TS_SECONDS = 1746662400L;
  private static final long TS_MICROS = 1746662400000000L;
  private static final long TS_NANOS = 1746662400000000000L;

  private TimeBasedIndexNamingStrategy strategy;
  private Map<String, String> props;

  @BeforeEach
  public void setup() {
    props = new HashMap<>();
    props.put(CONNECTION_URL_CONFIG, "http://localhost:9200");
    props.put(INDEX_SHARD_DATE_FIELD_MAP_CONFIG, "orders:createTime");
    // configure() validates this map is non-empty; individual tests may override.
    props.put(INDEX_SHARD_DATE_PATTERN_MAP_CONFIG, "orders:yyyyMMdd");
    strategy = new TimeBasedIndexNamingStrategy();
  }

  private void configure() {
    strategy.configure(
        new ElasticsearchSinkConnectorConfig(props)
    );
  }

  private SinkRecord structRecord(
      String topic, String field, Object value
  ) {
    Schema schema = SchemaBuilder.struct()
        .field(field, Schema.INT64_SCHEMA)
        .build();
    Struct struct = new Struct(schema).put(field, value);
    return new SinkRecord(
        topic, 0, null, null, schema, struct, 0
    );
  }

  private SinkRecord mapRecord(
      String topic, String field, Object value
  ) {
    Map<String, Object> map = new HashMap<>();
    map.put(field, value);
    return new SinkRecord(
        topic, 0, null, null, null, map, 0
    );
  }

  private SinkRecord emptyRecord(String topic) {
    return new SinkRecord(
        topic, 0, null, null, null, null, 0
    );
  }

  // --- Pattern tests ---

  @Test
  public void testDefaultPatternYyyyMMdd() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testPatternYyyy() {
    props.put(INDEX_SHARD_DATE_PATTERN_MAP_CONFIG, "orders:yyyy");
    configure();
    assertEquals("orders_2025",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testPatternYyyyMM() {
    props.put(INDEX_SHARD_DATE_PATTERN_MAP_CONFIG, "orders:yyyyMM");
    configure();
    assertEquals("orders_202505",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  // --- Timestamp precision auto-detection ---

  @Test
  public void testTimestampInSeconds() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_SECONDS)));
  }

  @Test
  public void testTimestampInMillis() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testTimestampInMicros() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MICROS)));
  }

  @Test
  public void testTimestampInNanos() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_NANOS)));
  }

  // --- Value types: Struct vs Map ---

  @Test
  public void testExtractTimestampFromStruct() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testExtractTimestampFromMap() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(mapRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testExtractTimestampFromMapWithInteger() {
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(
            mapRecord("orders", "createTime", (int) TS_SECONDS)
        ));
  }

  // --- Fallback when field missing ---

  @Test
  public void testFallbackToTopicNameWhenFieldMissing() {
    configure();
    assertEquals("orders",
        strategy.indexName(mapRecord("orders", "otherField", TS_MILLIS)));
  }

  @Test
  public void testWildcardPatternWhenValueNull() {
    // Delete tombstones arrive with value == null, so the configured date
    // field cannot be read. The strategy should return a wildcard pattern
    // so the sink task can resolve the concrete shard by _id.
    configure();
    assertEquals("orders_*", strategy.indexName(emptyRecord("orders")));
  }

  @Test
  public void testThrowsWhenTopicNotInFieldMap() {
    // Every topic consumed by the task must be declared in the date field map.
    // Records from an unconfigured topic should surface a config error rather
    // than silently falling back to a non-sharded index.
    configure();
    ConnectException ex = assertThrows(ConnectException.class,
        () -> strategy.indexName(emptyRecord("unknown")));
    assertEquals(true,
        ex.getMessage().contains(INDEX_SHARD_DATE_FIELD_MAP_CONFIG));
  }

  // --- Timezone ---

  @Test
  public void testTimezoneAsiaShanghaiSameDate() {
    props.put(INDEX_SHARD_DATE_TIMEZONE_CONFIG, "Asia/Shanghai");
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testTimezoneCrossDateBoundary() {
    props.put(INDEX_SHARD_DATE_TIMEZONE_CONFIG, "Asia/Shanghai");
    configure();
    // 2025-05-07 20:00:00 UTC = 2025-05-08 04:00:00 CST
    long utcTimestamp = TS_MILLIS - 4 * 3600 * 1000L;
    assertEquals("orders_20250508",
        strategy.indexName(
            structRecord("orders", "createTime", utcTimestamp)
        ));
  }

  // --- topic.index.map ---

  @Test
  public void testTopicIndexMapWithTimeSuffix() {
    props.put(TOPIC_INDEX_MAP_CONFIG, "orders:es_orders");
    configure();
    assertEquals("es_orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testTopicIndexMapFallbackForUnmappedTopic() {
    props.put(TOPIC_INDEX_MAP_CONFIG, "other:es_other");
    configure();
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testTopicIndexMapWildcardOnNullValue() {
    // For a delete tombstone (null value) on a topic that is both mapped and
    // time-sharded, the wildcard is applied to the mapped index name so the
    // sink task can resolve the concrete shard by _id.
    props.put(TOPIC_INDEX_MAP_CONFIG, "orders:es_orders");
    configure();
    assertEquals("es_orders_*",
        strategy.indexName(emptyRecord("orders")));
  }

  // --- Per-topic date field map ---

  @Test
  public void testMultipleTopicsInFieldMap() {
    props.put(INDEX_SHARD_DATE_FIELD_MAP_CONFIG, "orders:createTime,events:event_time");
    configure();
    assertEquals("events_20250508",
        strategy.indexName(structRecord("events", "event_time", TS_MILLIS)));
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  @Test
  public void testThrowsForIndexedRecordOfUnknownTopic() {
    configure();
    // "unknown" topic is not in the field map → must fail fast instead of
    // silently writing to a non-sharded index.
    assertThrows(ConnectException.class,
        () -> strategy.indexName(structRecord("unknown", "createTime", TS_MILLIS)));
  }

  @Test
  public void testPerTopicDateFieldMapFallbackWhenFieldMissing() {
    props.put(INDEX_SHARD_DATE_FIELD_MAP_CONFIG, "orders:createTime,events:event_time");
    configure();
    // events topic but record has "createTime" field → configured field "event_time" not found → fallback
    assertEquals("events",
        strategy.indexName(mapRecord("events", "createTime", TS_MILLIS)));
  }

  // --- Per-topic date pattern map ---

  @Test
  public void testPerTopicDatePatternMap() {
    props.put(INDEX_SHARD_DATE_FIELD_MAP_CONFIG, "orders:createTime,events:createTime");
    props.put(INDEX_SHARD_DATE_PATTERN_MAP_CONFIG, "events:yyyyMM");
    configure();
    assertEquals("events_202505",
        strategy.indexName(structRecord("events", "createTime", TS_MILLIS)));
    // orders uses the default pattern "yyyyMMdd"
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }

  // --- Configuration validation ---

  @Test
  public void testConfigureFailsWhenFieldMapEmpty() {
    props.put(INDEX_SHARD_DATE_FIELD_MAP_CONFIG, "");
    ConfigException ex = assertThrows(ConfigException.class, this::configure);
    assertEquals(true,
        ex.getMessage().contains(INDEX_SHARD_DATE_FIELD_MAP_CONFIG));
  }

  @Test
  public void testConfigureFailsWhenPatternMapEmpty() {
    props.put(INDEX_SHARD_DATE_PATTERN_MAP_CONFIG, "");
    ConfigException ex = assertThrows(ConfigException.class, this::configure);
    assertEquals(true,
        ex.getMessage().contains(INDEX_SHARD_DATE_PATTERN_MAP_CONFIG));
  }

  // --- Combined per-topic overrides ---

  @Test
  public void testCombinedPerTopicOverrides() {
    props.put(INDEX_SHARD_DATE_FIELD_MAP_CONFIG, "orders:createTime,events:event_time,logs:ts");
    props.put(INDEX_SHARD_DATE_PATTERN_MAP_CONFIG, "events:yyyyMM,logs:yyyy");
    configure();

    assertEquals("events_202505",
        strategy.indexName(structRecord("events", "event_time", TS_MILLIS)));
    assertEquals("logs_2025",
        strategy.indexName(structRecord("logs", "ts", TS_MILLIS)));
    assertEquals("orders_20250508",
        strategy.indexName(structRecord("orders", "createTime", TS_MILLIS)));
  }
}
