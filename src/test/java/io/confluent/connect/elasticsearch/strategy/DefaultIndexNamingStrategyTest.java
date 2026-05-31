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
import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultIndexNamingStrategyTest {

  private DefaultIndexNamingStrategy strategy;
  private Map<String, String> props;

  @BeforeEach
  public void setup() {
    props = new HashMap<>();
    props.put(CONNECTION_URL_CONFIG, "http://localhost:9200");
    strategy = new DefaultIndexNamingStrategy();
  }

  private void configure() {
    strategy.configure(new ElasticsearchSinkConnectorConfig(props));
  }

  private SinkRecord record(String topic) {
    return new SinkRecord(topic, 0, null, null, null, null, 0);
  }

  @Test
  public void testIndexNameFromTopic() {
    configure();
    assertEquals("server.db.my_topic", strategy.indexName(record("server.db.my_topic")));
  }

  @Test
  public void testTopicNameConvertedToLowerCase() {
    configure();
    assertEquals("my_topic", strategy.indexName(record("MY_TOPIC")));
  }

  @Test
  public void testTopicNameTruncatedTo255() {
    configure();
    String longTopic = new String(new char[300]).replace('\0', 'a');
    String result = strategy.indexName(record(longTopic));
    assertEquals(255, result.length());
  }

  @Test
  public void testTopicNameStripsLeadingDash() {
    configure();
    assertEquals("topic", strategy.indexName(record("-topic")));
  }

  @Test
  public void testTopicNameStripsLeadingUnderscore() {
    configure();
    assertEquals("topic", strategy.indexName(record("_topic")));
  }

  @Test
  public void testTopicNameDotReplacedWithDot() {
    configure();
    assertEquals("dot", strategy.indexName(record(".")));
  }

  @Test
  public void testTopicIndexMapOverridesTopic() {
    props.put(TOPIC_INDEX_MAP_CONFIG, "orders:es_orders");
    configure();
    assertEquals("es_orders", strategy.indexName(record("orders")));
  }

  @Test
  public void testTopicIndexMapMultipleEntries() {
    props.put(TOPIC_INDEX_MAP_CONFIG, "orders:es_orders, users:es_users");
    configure();
    assertEquals("es_orders", strategy.indexName(record("orders")));
    assertEquals("es_users", strategy.indexName(record("users")));
  }

  @Test
  public void testTopicIndexMapFallbackForUnmappedTopic() {
    props.put(TOPIC_INDEX_MAP_CONFIG, "orders:es_orders");
    configure();
    assertEquals("other_topic",
        strategy.indexName(record("other_topic")));
  }

  @Test
  public void testTopicIndexMapEmptyStringNoEffect() {
    props.put(TOPIC_INDEX_MAP_CONFIG, "");
    configure();
    assertEquals("my_topic", strategy.indexName(record("my_topic")));
  }

  @Test
  public void testIndexNameCached() {
    configure();
    String first = strategy.indexName(record("my_topic"));
    String second = strategy.indexName(record("my_topic"));
    assertEquals(first, second);
  }
}
