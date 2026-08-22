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

package io.confluent.connect.elasticsearch;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.elasticsearch.action.DocWriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.BehaviorOnNullValues;
import io.confluent.connect.elasticsearch.strategy.IndexNamingStrategy;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ElasticsearchSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);

  private DataConverter converter;
  private ElasticsearchClient client;
  private ElasticsearchSinkConnectorConfig config;
  private ErrantRecordReporter reporter;
  private Map<String, Schema> existingMappings;
  private Set<String> indexCache;
  private OffsetTracker offsetTracker;
  private PartitionPauser partitionPauser;
  private ElasticsearchSinkMetrics esSinkMetrics;
  private IndexNamingStrategy indexNamingStrategy;

  @Override
  public void start(Map<String, String> props) {
    start(props, null);
  }

  // visible for testing
  protected void start(Map<String, String> props, ElasticsearchClient client) {
    log.info("Starting ElasticsearchSinkTask.");

    this.config = new ElasticsearchSinkConnectorConfig(props);
    this.converter = new DataConverter(config);
    this.existingMappings = new HashMap<>();
    this.indexCache = new HashSet<>();
    this.indexNamingStrategy = createIndexNamingStrategy();
    int offsetHighWaterMark = config.maxBufferedRecords() * 10;
    int offsetLowWaterMark = config.maxBufferedRecords() * 5;
    this.partitionPauser = new PartitionPauser(context,
        () -> offsetTracker.numOffsetStateEntries() > offsetHighWaterMark,
        () -> offsetTracker.numOffsetStateEntries() <= offsetLowWaterMark);
    this.reporter = null;
    try {
      if (context.errantRecordReporter() == null) {
        log.info("Errant record reporter not configured.");
      }
      // may be null if DLQ not enabled
      reporter = context.errantRecordReporter();
    } catch (NoClassDefFoundError | NoSuchMethodError e) {
      // Will occur in Connect runtimes earlier than 2.6
      log.warn("AK versions prior to 2.6 do not support the errant record reporter.");
    }
    Runnable afterBulkCallback = () -> offsetTracker.updateOffsets();
    this.esSinkMetrics = new ElasticsearchSinkMetrics(config);
    this.esSinkMetrics.register();
    this.client = client != null ? client
        : new ElasticsearchClient(config, reporter, afterBulkCallback, esSinkMetrics);

    if (!config.flushSynchronously()) {
      this.offsetTracker = new AsyncOffsetTracker(context);
    } else {
      this.offsetTracker = new SyncOffsetTracker(this.client);
    }

    log.info("Started ElasticsearchSinkTask. Connecting to ES server version: {}",
        this.client.version());
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    if (records.isEmpty()) {
      return;
    }
    log.debug("Putting {} records to Elasticsearch.", records.size());

    client.throwIfFailed();
    partitionPauser.maybeResumePartitions();

    for (SinkRecord record : records) {
      OffsetState offsetState = offsetTracker.addPendingRecord(record);
      if (shouldSkipRecord(record)) {
        logTrace("Ignoring {} with null value.", record);
        offsetState.markProcessed();
        reportBadRecord(record, new ConnectException("Cannot write null valued record."));
        continue;
      }
      logTrace("Writing {} to Elasticsearch.", record);

      tryWriteRecord(record, offsetState);
    }
    esSinkMetrics.setEventTimestamp(records.stream().findFirst().get().timestamp());
    partitionPauser.maybePausePartitions();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    try {
      // This will just trigger an asynchronous execution of any buffered records
      client.flush();
    } catch (IllegalStateException e) {
      log.debug("Tried to flush data to Elasticsearch, but BulkProcessor is already closed.", e);
    }
    Map<TopicPartition, OffsetAndMetadata> offsets = offsetTracker.offsets(currentOffsets);
    log.debug("preCommitting offsets {}", offsets);
    return offsets;
  }

  @Override
  public void stop() {
    log.debug("Stopping Elasticsearch client.");
    client.close();
    esSinkMetrics.unregister();
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  private void checkMapping(String index, SinkRecord record) {
    if (config.shouldIgnoreSchema(record.topic())) {
      return;
    }

    Schema valueSchema = record.valueSchema();
    Schema cachedSchema = existingMappings.get(index);
    // no schema changed
    if (cachedSchema != null && Objects.equals(cachedSchema, valueSchema)) {
      return;
    }

    if (!client.hasMapping(index)) {
      client.createMapping(index, valueSchema);
    } else if (cachedSchema != null && !Objects.equals(cachedSchema, valueSchema)
        && hasNewFields(cachedSchema, valueSchema)) {
      log.info("Schema changed for index '{}', updating mapping for new fields.", index);
      client.createMapping(index, valueSchema);
    } else if (cachedSchema == null && hasNewFieldsOverMapping(index, valueSchema)) {
      log.info("Task restarted, index '{}' has new fields in schema, updating mapping.", index);
      client.createMapping(index, valueSchema);
    }
    existingMappings.put(index, valueSchema);
  }

  private boolean hasNewFields(Schema cachedSchema, Schema newSchema) {
    if (cachedSchema.fields() == null || newSchema.fields() == null) {
      return false;
    }
    Set<String> cachedFieldNames = new HashSet<>();
    cachedSchema.fields().forEach(f -> cachedFieldNames.add(f.name()));
    return newSchema.fields().stream().anyMatch(f -> !cachedFieldNames.contains(f.name()));
  }

  private boolean hasNewFieldsOverMapping(String index, Schema newSchema) {
    if (newSchema.fields() == null) {
      return false;
    }
    Set<String> mappingFieldNames = client.getMappingFieldNames(index);
    return newSchema.fields().stream().anyMatch(f -> !mappingFieldNames.contains(f.name()));
  }

  private IndexNamingStrategy createIndexNamingStrategy() {
    String className = config.indexNamingStrategy();
    try {
      Class<?> clazz = Class.forName(className);
      IndexNamingStrategy strategy =
          (IndexNamingStrategy) clazz.getDeclaredConstructor().newInstance();
      strategy.configure(config);
      return strategy;
    } catch (Exception e) {
      throw new ConnectException("Failed to instantiate index naming strategy: " + className, e);
    }
  }

  private void ensureIndexExists(String index) {
    if (!indexCache.contains(index)) {
      log.info("Creating index {}.", index);
      client.createIndexOrDataStream(index);
      indexCache.add(index);
    }
  }

  private void logTrace(String formatMsg, SinkRecord record) {
    if (log.isTraceEnabled()) {
      log.trace(formatMsg, recordString(record));
    }
  }

  private void reportBadRecord(SinkRecord record, Throwable error) {
    if (reporter != null) {
      // No need to wait for the futures (synchronously or async), the framework will wait for
      // all these futures before calling preCommit
      reporter.report(record, error);
    }
  }

  private boolean shouldSkipRecord(SinkRecord record) {
    return record.value() == null && config.behaviorOnNullValues() == BehaviorOnNullValues.IGNORE;
  }

  private void tryWriteRecord(SinkRecord sinkRecord, OffsetState offsetState) {
    String indexName = indexNamingStrategy.indexName(sinkRecord);

    if (IndexNamingStrategy.isWildcard(indexName)) {
      String resolved = resolveWildcardIndex(indexName, sinkRecord);
      if (resolved == null) {
        offsetState.markProcessed();
        return;
      }
      indexName = resolved;
    }

    ensureIndexExists(indexName);
    checkMapping(indexName, sinkRecord);

    DocWriteRequest<?> docWriteRequest = null;
    try {
      docWriteRequest = converter.convertRecord(sinkRecord, indexName);
    } catch (DataException convertException) {
      reportBadRecord(sinkRecord, convertException);

      if (config.dropInvalidMessage()) {
        log.error("Can't convert {}.", recordString(sinkRecord), convertException);
        offsetState.markProcessed();
      } else {
        throw convertException;
      }
    }

    if (docWriteRequest != null) {
      logTrace("Adding {} to bulk processor.", sinkRecord);
      client.index(sinkRecord, docWriteRequest, offsetState);
    }
  }

  /**
   * Resolves a wildcard index pattern (e.g. {@code base_*}) returned by a
   * time-based naming strategy for a delete tombstone whose value is null.
   * Searches by doc id across matching shards and returns the concrete index,
   * or null if the record should be skipped.
   */
  private String resolveWildcardIndex(String pattern, SinkRecord sinkRecord) {
    if (sinkRecord.value() != null) {
      throw new ConnectException(String.format(
          "Index naming strategy returned wildcard pattern '%s' for a non-delete %s. "
              + "Wildcard resolution is only supported for delete tombstones.",
          pattern, recordString(sinkRecord)));
    }
    if (sinkRecord.key() == null) {
      log.trace("Ignoring delete {} with null key under wildcard pattern {}.",
          recordString(sinkRecord), pattern);
      return null;
    }
    String id = converter.documentId(sinkRecord);
    String resolved = client.resolveIndexForDocId(pattern, id);
    if (resolved == null) {
      log.info("No matching document for id '{}' under pattern '{}'; skipping delete for {}.",
          id, pattern, recordString(sinkRecord));
      return null;
    }
    log.debug("Resolved wildcard pattern '{}' to concrete index '{}' for delete of id '{}'.",
        pattern, resolved, id);
    return resolved;
  }

  private static String recordString(SinkRecord record) {
    return String.format(
        "record from topic=%s partition=%s offset=%s",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset()
    );
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    offsetTracker.closePartitions(partitions);
  }

  // Visible for testing
  static class PartitionPauser {

    // Kafka consumer poll timeout to set when partitions are paused, to avoid waiting for a long
    // time (default poll timeout) to resume it.
    private static final long PAUSE_POLL_TIMEOUT_MS = 100;

    private final SinkTaskContext context;
    private final BooleanSupplier pauseCondition;
    private final BooleanSupplier resumeCondition;
    private boolean partitionsPaused;

    public PartitionPauser(SinkTaskContext context,
                           BooleanSupplier pauseCondition,
                           BooleanSupplier resumeCondition) {
      this.context = context;
      this.pauseCondition = pauseCondition;
      this.resumeCondition = resumeCondition;
    }

    /**
     * Resume partitions if they are paused and resume condition is met.
     * Has to be run in the task thread.
     */
    void maybeResumePartitions() {
      if (partitionsPaused) {
        if (resumeCondition.getAsBoolean()) {
          log.debug("Resuming all partitions");
          context.resume(context.assignment().toArray(new TopicPartition[0]));
          partitionsPaused = false;
        } else {
          context.timeout(PAUSE_POLL_TIMEOUT_MS);
        }
      }
    }

    /**
     * Pause partitions if they are not paused and pause condition is met.
     * Has to be run in the task thread.
     */
    void maybePausePartitions() {
      if (!partitionsPaused && pauseCondition.getAsBoolean()) {
        log.debug("Pausing all partitions");
        context.pause(context.assignment().toArray(new TopicPartition[0]));
        context.timeout(PAUSE_POLL_TIMEOUT_MS);
        partitionsPaused = true;
      }
    }
  }
}
