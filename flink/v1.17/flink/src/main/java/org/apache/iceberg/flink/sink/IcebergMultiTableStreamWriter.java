/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import org.apache.commons.collections.MapUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.EnrichedTableIdentifier;
import org.apache.iceberg.flink.data.TableAwareWriteResult;
import org.apache.iceberg.flink.data.TableIdentifierProperties;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class IcebergMultiTableStreamWriter<T> extends AbstractStreamOperator<TableAwareWriteResult>
    implements OneInputStreamOperator<T, TableAwareWriteResult>, BoundedOneInput {
  private static final long serialVersionUID = 1L;

  private transient Map<TableIdentifier, TaskWriterFactory<T>> taskWriterFactories;
  private final PayloadTableSinkProvider<T> payloadTableSinkProvider;
  private final CatalogLoader catalogLoader;
  private List<String> equalityFieldColumns;
  private final Map<String, String> writeOptions;
  private final ReadableConfig readableConfig;

  private transient Map<TableIdentifier, TaskWriter<T>> writers;
  private transient int subTaskId;
  private transient int attemptId;
  private transient Map<TableIdentifier, IcebergStreamWriterMetrics> writerMetrics;
  private transient Map<TableIdentifier, TableIdentifierProperties> tableIdentifierProperties;
  private transient int currentElemCnt = 0;

  IcebergMultiTableStreamWriter(
      PayloadTableSinkProvider<T> payloadTableSinkProvider,
      CatalogLoader catalogLoader,
      List<String> equalityFieldColumns,
      Map<String, String> writeOptions,
      ReadableConfig readableConfig) {
    this.payloadTableSinkProvider = payloadTableSinkProvider;
    this.catalogLoader = catalogLoader;
    this.equalityFieldColumns = equalityFieldColumns;
    this.writeOptions = writeOptions;
    this.readableConfig = readableConfig;
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();
    this.writers = Maps.newHashMap();
    this.writerMetrics = Maps.newHashMap();
    this.taskWriterFactories = Maps.newHashMap();
    this.tableIdentifierProperties = Maps.newHashMap();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    LOG.info("[DEBUG-IcebergMultiTableStreamWriter]Got {} elements in checkpoint : {}", currentElemCnt, checkpointId);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    LOG.info("[DEBUG-IcebergMultiTableStreamWriter] snapshotState Got {} elements", currentElemCnt);
    super.snapshotState(context);
    flush();
    currentElemCnt = 0;
  }

  @Override
  public void processElement(StreamRecord<T> element) throws Exception {
    ++currentElemCnt;
    TableIdentifier tableIdentifier = payloadTableSinkProvider.getTableIdentifier(element);
    LOG.debug("[IcebergMultiTableStreamWriter] Got Table Identifier: {}", tableIdentifier.toString());
    EnrichedTableIdentifier enrichedTableIdentifier = getEnrichedTableIdentifier(tableIdentifier, element);
    initializeOrRefreshTable(enrichedTableIdentifier);
    LOG.debug("[IcebergMultiTableStreamWriter] Initialized EnrichedTableIdentifier: {}", enrichedTableIdentifier.toString());
    try {
      writers.get(tableIdentifier).write(element.getValue());
    } catch (Exception e) {
      --currentElemCnt;
      LOG.error("Error occurred while writing the payload {}: ", element.getValue().toString(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    Iterator<Map.Entry<TableIdentifier, TaskWriter<T>>> writeIterator =
        writers.entrySet().iterator();
    while (writeIterator.hasNext()) {
      Map.Entry<TableIdentifier, TaskWriter<T>> writer = writeIterator.next();
      try {
        writer.getValue().close();
      } catch (Exception exception) {
        LOG.error("Error occurred while closing the writer {}: ", writer.getValue(), exception);
      }
    }
    writers.clear();
  }

  @Override
  public void endInput() throws IOException {
    // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the
    // remaining completed files to downstream before closing the writer so that we won't miss any
    // of them.
    // Note that if the task is not closed after calling endInput, checkpoint may be triggered again
    // causing files to be sent repeatedly, the writer is marked as null after the last file is sent
    // to guard against duplicated writes.
    flush();
  }

  @Override
  public String toString() {
    // TODO: Modify if required
    String tableList =
        writers.keySet().size() > 0
            ? String.join(
                ";",
                writers.keySet().stream().map(TableIdentifier::name).collect(Collectors.toList()))
            : "empty_writer";
    return MoreObjects.toStringHelper(this)
        .add("tables", tableList)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  private EnrichedTableIdentifier getEnrichedTableIdentifier(TableIdentifier tableIdentifier, StreamRecord<T> element)
    throws IOException {
    EnrichedTableIdentifier enrichedTableIdentifier = null;
    if(MapUtils.isNotEmpty(tableIdentifierProperties) && tableIdentifierProperties.containsKey(tableIdentifier)) {
      enrichedTableIdentifier = payloadTableSinkProvider.createOrRefreshTable(tableIdentifier,
              Optional.of(tableIdentifierProperties.get(tableIdentifier)), element);
    } else {
      enrichedTableIdentifier = payloadTableSinkProvider.createOrRefreshTable(tableIdentifier,
              Optional.empty(), element);
    }

    return enrichedTableIdentifier;
  }

  private void initializeOrRefreshTable(EnrichedTableIdentifier enrichedTableIdentifier) throws IOException {
    LOG.debug("checking if writer does not exist or needs to be updated: {}", enrichedTableIdentifier.toString());

    /**
     * Writer will be upserted only if:
     *  Either writer did not exist for this table identifier
     *  Or Properties map is empty
     *  Or Properties do not exist for this table identifier
     *  Or Properties exist for the table identifier but enriched table identifier properties are different
     */
    if (!writers.containsKey(enrichedTableIdentifier.getTableIdentifier()) ||
            MapUtils.isEmpty(tableIdentifierProperties) ||
            !tableIdentifierProperties.containsKey(enrichedTableIdentifier.getTableIdentifier()) ||
            !tableIdentifierProperties.get(enrichedTableIdentifier.getTableIdentifier())
                    .equals(enrichedTableIdentifier.getTableIdentifierProperties())) {
      String tableName = enrichedTableIdentifier.getTableIdentifier().name();

      LOG.info("Writer needs to be upserted for table: {}", tableName);

      if(MapUtils.isNotEmpty(writers) && writers.containsKey(enrichedTableIdentifier.getTableIdentifier())) {
        LOG.info("Flushing old writer for table: {}", enrichedTableIdentifier.getTableIdentifier().name());
        flushWriter(enrichedTableIdentifier.getTableIdentifier(), writers.get(enrichedTableIdentifier.getTableIdentifier()));
      }

      TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, enrichedTableIdentifier.getTableIdentifier());
      tableLoader.open();
      Table sinkTable = tableLoader.loadTable();
      FlinkWriteConf flinkWriteConf = new FlinkWriteConf(sinkTable, writeOptions, readableConfig);

      TaskWriterFactory taskWriterFactory =
              new RowDataTaskWriterFactory(
                sinkTable,
                FlinkSink.toFlinkRowType(sinkTable.schema(), null),
                flinkWriteConf.targetDataFileSize(),
                flinkWriteConf.dataFileFormat(),
                TablePropertyUtil.writeProperties(
                    sinkTable, flinkWriteConf.dataFileFormat(), flinkWriteConf),
                TablePropertyUtil.checkAndGetEqualityFieldIds(sinkTable, equalityFieldColumns),
                flinkWriteConf.upsertMode());

      taskWriterFactory.initialize(subTaskId, attemptId);
      taskWriterFactories.put(enrichedTableIdentifier.getTableIdentifier(), taskWriterFactory);
      TaskWriter<T> taskWriter = taskWriterFactories.get(enrichedTableIdentifier.getTableIdentifier()).create();
      writers.put(enrichedTableIdentifier.getTableIdentifier(), taskWriter);
      tableIdentifierProperties.put(enrichedTableIdentifier.getTableIdentifier(),
              enrichedTableIdentifier.getTableIdentifierProperties());
      IcebergStreamWriterMetrics tableWriteMetrics =
          new IcebergStreamWriterMetrics(super.metrics, tableName);
      writerMetrics.put(enrichedTableIdentifier.getTableIdentifier(), tableWriteMetrics);

      LOG.info("Putting writer with key: {}", enrichedTableIdentifier.getTableIdentifier().toString());
    }
  }

  /** close all open files and emit files to downstream committer operator */
  private void flush() throws IOException {
    if (writers.isEmpty()) {
      return;
    }

    Iterator<Map.Entry<TableIdentifier, TaskWriter<T>>> iterator = writers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<TableIdentifier, TaskWriter<T>> writerMap = iterator.next();
      TableIdentifier tableIdentifier = writerMap.getKey();
      TaskWriter<T> writer = writerMap.getValue();
      flushWriter(tableIdentifier, writer);
    }

    // Set writer to null to prevent duplicate flushes in the corner case of
    // prepareSnapshotPreBarrier happening after endInput.
    writers.clear();
  }

  private void flushWriter(TableIdentifier tableIdentifier, TaskWriter<T> writer) throws IOException {
    long startNano = System.nanoTime();
    WriteResult result = writer.complete();
    TableAwareWriteResult tableAwareWriteResult =
            new TableAwareWriteResult(result, tableIdentifier);
    IcebergStreamWriterMetrics metrics = writerMetrics.get(tableIdentifier);
    output.collect(new StreamRecord<>(tableAwareWriteResult));
    metrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
  }
}
