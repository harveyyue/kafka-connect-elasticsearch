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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractIndexNamingStrategy implements IndexNamingStrategy {

  private static final Logger log = LoggerFactory.getLogger(AbstractIndexNamingStrategy.class);

  protected ElasticsearchSinkConnectorConfig config;
  protected Map<String, String> topicToIndexCache;
  protected Map<String, String> topicToIndexMap;

  @Override
  public void configure(ElasticsearchSinkConnectorConfig config) {
    this.config = config;
    this.topicToIndexCache = new HashMap<>();
    this.topicToIndexMap = parseTopicIndexMap(config.topicIndexMap());
  }

  private static Map<String, String> parseTopicIndexMap(String mapStr) {
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
        log.warn("Invalid topic.index.map entry: '{}', skipping.", trimmed);
      }
    }
    return map;
  }

  /**
   * Returns the converted index name from a given topic name. If writing to a data stream,
   * returns the index name in the form {type}-{dataset}-{topic}. For both cases, Elasticsearch
   * accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   * (<a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">ref</a>_.)
   */
  public String createIndexName(String topic) {
    String mappedIndex = topicToIndexMap.get(topic);
    if (mappedIndex != null) {
      return mappedIndex;
    }
    return config.isDataStream()
        ? convertTopicToDataStreamName(topic)
        : convertTopicToIndexName(topic);
  }

  /**
   * Returns the converted index name from a given topic name. Elasticsearch accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>less than 256 bytes</li>
   *   <li>does not start with - or _</li>
   *   <li>is not . or ..</li>
   * </ul>
   * (<a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">ref</a>_.)
   */
  private String convertTopicToIndexName(String topic) {
    String index = topic.toLowerCase();
    if (index.length() > 255) {
      index = index.substring(0, 255);
    }

    if (index.startsWith("-") || index.startsWith("_")) {
      index = index.substring(1);
    }

    if (index.equals(".") || index.equals("..")) {
      index = index.replace(".", "dot");
      log.warn("Elasticsearch cannot have indices named {}. Index will be named {}.", topic, index);
    }

    if (!topic.equals(index)) {
      log.trace("Topic '{}' was translated to index '{}'.", topic, index);
    }

    return index;
  }

  /**
   * Returns the converted index name from a given topic name in the form {type}-{dataset}-{topic}.
   * For the <code>topic</code>, Elasticsearch accepts:
   * <ul>
   *   <li>all lowercase</li>
   *   <li>no longer than 100 bytes</li>
   * </ul>
   * (<a href="https://github.com/elastic/ecs/blob/master/rfcs/text/0009-data_stream-fields.md#restrictions-on-values">ref</a>_.)
   */
  private String convertTopicToDataStreamName(String topic) {
    topic = topic.toLowerCase();
    if (topic.length() > 100) {
      topic = topic.substring(0, 100);
    }
    return String.format(
        "%s-%s-%s",
        config.dataStreamType().name().toLowerCase(),
        config.dataStreamDataset(),
        topic
    );
  }
}
