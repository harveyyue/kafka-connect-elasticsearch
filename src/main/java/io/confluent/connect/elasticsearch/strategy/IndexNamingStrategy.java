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
import org.apache.kafka.connect.sink.SinkRecord;


public interface IndexNamingStrategy {

  String WILDCARD_SUFFIX = "_*";

  void configure(ElasticsearchSinkConnectorConfig config);

  String indexName(SinkRecord sinkRecord);

  /**
   * Whether the given index name is a wildcard pattern that still needs
   * to be resolved to a concrete index at write time (e.g. for a delete
   * tombstone whose target shard cannot be computed from the value).
   */
  static boolean isWildcard(String indexName) {
    return indexName != null && indexName.indexOf('*') >= 0;
  }
}
