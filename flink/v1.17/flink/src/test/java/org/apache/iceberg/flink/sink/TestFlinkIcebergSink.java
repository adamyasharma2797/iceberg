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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogResource;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.data.EnrichedTableIdentifier;
import org.apache.iceberg.flink.data.TableIdentifierProperties;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.TestFixtures.*;
import static org.apache.iceberg.flink.sink.IcebergMultiTableFilesCommitter.SINGLE_PHASE_COMMIT_ENABLED;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSink extends TestFlinkIcebergSinkBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopCatalogResource catalogResource =
      new HadoopCatalogResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE);
  @Rule
  public final HadoopCatalogResource catalogResourceSPC =
          new HadoopCatalogResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE_SPC);

  private TableLoader tableLoader;
  private TableLoader tableLoaderSPC;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;

  @Parameterized.Parameters(name = "format={0}, parallelism = {1}, partitioned = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"avro", 1, true},
      {"avro", 1, false},
      {"avro", 2, true},
      {"avro", 2, false},
      {"parquet", 1, true},
      {"parquet", 1, false},
      {"parquet", 2, true},
      {"parquet", 2, false},
      {"orc", 1, true},
      {"orc", 1, false},
      {"orc", 2, true},
      {"orc", 2, false}
    };
  }

  public TestFlinkIcebergSink(String format, int parallelism, boolean partitioned) {
    this.format = FileFormat.fromString(format);
    this.parallelism = parallelism;
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    table =
        catalogResource
            .catalog()
            .createTable(
                TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name(),
                        SINGLE_PHASE_COMMIT_ENABLED, "false"));

    tableSPC =
        catalogResourceSPC
            .catalog()
            .createTable(
                TABLE_SPC_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name(),
                        SINGLE_PHASE_COMMIT_ENABLED, "true"));

    table1 = catalogResource
            .catalog()
            .createTable(
                    SECONDARY_TABLE_IDENTIFIER,
                    SimpleDataUtil.SCHEMA,
                    partitioned
                            ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                            : PartitionSpec.unpartitioned(),
                    ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name(),
                            SINGLE_PHASE_COMMIT_ENABLED, "false"));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    tableLoader = catalogResource.tableLoader();
    tableLoaderSPC = catalogResourceSPC.tableLoader();
  }

  @Test
  public void testWriteRowData() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  @Test
  public void testMultiTableWriteRowDataOnSingleTable() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
            env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
                    .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
            .table(table)
            .readMultitableWriter(true)
            .setPayloadTableSinkProvider(new SimpleProvider())
            .setCatalogLoader(catalogResource.catalogLoader())
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .distributionMode(DistributionMode.NONE)
            .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  @Test
  public void testMultiTableWriteRowDataOnMultiTable() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    List<Row> odd = Lists.newArrayList(Row.of(1, "hello"), Row.of(3, "foo"));
    List<Row> even = Lists.newArrayList(Row.of(2, "world"));
    DataStream<RowData> dataStream =
            env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
                    .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
            .table(table)
            .readMultitableWriter(true)
            .setPayloadTableSinkProvider(new ComplexProvider())
            .setCatalogLoader(catalogResource.catalogLoader())
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .distributionMode(DistributionMode.NONE)
            .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(even));
    SimpleDataUtil.assertTableRows(table1, convertToRowData(odd));
  }

  @Test
  public void testMultiTableWriteRowDataOnMultiTableWithAlteringProps() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    List<Row> odd = Lists.newArrayList(Row.of(1, "hello"), Row.of(3, "foo"));
    List<Row> even = Lists.newArrayList(Row.of(2, "world"));
    DataStream<RowData> dataStream =
            env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
                    .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
            .table(table)
            .readMultitableWriter(true)
            .setPayloadTableSinkProvider(new OddEvenWithPropsAlteringProvider())
            .setCatalogLoader(catalogResource.catalogLoader())
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .distributionMode(DistributionMode.NONE)
            .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(even));
    SimpleDataUtil.assertTableRows(table1, convertToRowData(odd));
  }

  private void testWriteRow(TableSchema tableSchema, DistributionMode distributionMode)
      throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .tableSchema(tableSchema)
        .writeParallelism(parallelism)
        .distributionMode(distributionMode)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  private int partitionFiles(String partition) throws IOException {
    return SimpleDataUtil.partitionDataFiles(table, ImmutableMap.of("data", partition)).size();
  }

  @Test
  public void testWriteRow() throws Exception {
    testWriteRow(null, DistributionMode.NONE);
  }

  @Test
  public void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.NONE);
  }

  @Test
  public void testJobNoneDistributeMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(null, DistributionMode.NONE);

    if (parallelism > 1) {
      if (partitioned) {
        int files = partitionFiles("aaa") + partitionFiles("bbb") + partitionFiles("ccc");
        Assert.assertTrue("Should have more than 3 files in iceberg table.", files > 3);
      }
    }
  }

  @Test
  public void testJobHashDistributionMode() {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    Assertions.assertThatThrownBy(() -> testWriteRow(null, DistributionMode.RANGE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Flink does not support 'range' write distribution mode now.");
  }

  @Test
  public void testJobNullDistributionMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(null, null);

    if (partitioned) {
      Assert.assertEquals(
          "There should be only 1 data file in partition 'aaa'", 1, partitionFiles("aaa"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'bbb'", 1, partitionFiles("bbb"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'ccc'", 1, partitionFiles("ccc"));
    }
  }

  @Test
  public void testPartitionWriteMode() throws Exception {
    testWriteRow(null, DistributionMode.HASH);
    if (partitioned) {
      Assert.assertEquals(
          "There should be only 1 data file in partition 'aaa'", 1, partitionFiles("aaa"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'bbb'", 1, partitionFiles("bbb"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'ccc'", 1, partitionFiles("ccc"));
    }
  }

  @Test
  public void testShuffleByPartitionWithSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.HASH);
    if (partitioned) {
      Assert.assertEquals(
          "There should be only 1 data file in partition 'aaa'", 1, partitionFiles("aaa"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'bbb'", 1, partitionFiles("bbb"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'ccc'", 1, partitionFiles("ccc"));
    }
  }

  @Test
  public void testTwoSinksInDisjointedDAG() throws Exception {
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());

    Table leftTable =
        catalogResource
            .catalog()
            .createTable(
                TableIdentifier.of("left"),
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                props);
    TableLoader leftTableLoader =
        TableLoader.fromCatalog(catalogResource.catalogLoader(), TableIdentifier.of("left"));

    Table rightTable =
        catalogResource
            .catalog()
            .createTable(
                TableIdentifier.of("right"),
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                props);
    TableLoader rightTableLoader =
        TableLoader.fromCatalog(catalogResource.catalogLoader(), TableIdentifier.of("right"));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);
    env.getConfig().disableAutoGeneratedUIDs();

    List<Row> leftRows = createRows("left-");
    DataStream<Row> leftStream =
        env.fromCollection(leftRows, ROW_TYPE_INFO)
            .name("leftCustomSource")
            .uid("leftCustomSource");
    FlinkSink.forRow(leftStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(leftTable)
        .tableLoader(leftTableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .distributionMode(DistributionMode.NONE)
        .uidPrefix("leftIcebergSink")
        .append();

    List<Row> rightRows = createRows("right-");
    DataStream<Row> rightStream =
        env.fromCollection(rightRows, ROW_TYPE_INFO)
            .name("rightCustomSource")
            .uid("rightCustomSource");
    FlinkSink.forRow(rightStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(rightTable)
        .tableLoader(rightTableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .distributionMode(DistributionMode.HASH)
        .uidPrefix("rightIcebergSink")
        .setSnapshotProperty("flink.test", TestFlinkIcebergSink.class.getName())
        .setSnapshotProperties(Collections.singletonMap("direction", "rightTable"))
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(leftTable, convertToRowData(leftRows));
    SimpleDataUtil.assertTableRows(rightTable, convertToRowData(rightRows));

    leftTable.refresh();
    Assert.assertNull(leftTable.currentSnapshot().summary().get("flink.test"));
    Assert.assertNull(leftTable.currentSnapshot().summary().get("direction"));
    rightTable.refresh();
    Assert.assertEquals(
        TestFlinkIcebergSink.class.getName(),
        rightTable.currentSnapshot().summary().get("flink.test"));
    Assert.assertEquals("rightTable", rightTable.currentSnapshot().summary().get("direction"));
  }

  @Test
  public void testOverrideWriteConfigWithUnknownDistributionMode() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .setAll(newProps);

    Assertions.assertThatThrownBy(builder::append)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid distribution mode: UNRECOGNIZED");
  }

  @Test
  public void testOverrideWriteConfigWithUnknownFileFormat() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.WRITE_FORMAT.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .setAll(newProps);

    Assertions.assertThatThrownBy(builder::append)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file format: UNRECOGNIZED");
  }

  @Test
  public void testWriteRowWithTableRefreshInterval() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    Configuration flinkConf = new Configuration();
    flinkConf.setString(FlinkWriteOptions.TABLE_REFRESH_INTERVAL.key(), "100ms");

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .flinkConf(flinkConf)
        .writeParallelism(parallelism)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  @Test
  public void testWriteRowWithSinglePhaseCommitEnabled() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
            env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
                    .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
            .table(tableSPC)
            .tableLoader(tableLoaderSPC)
            .writeParallelism(parallelism)
            .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(tableSPC, convertToRowData(rows));
  }

  private static class SimpleProvider implements PayloadTableSinkProvider<RowData>, Serializable {
    @Override
    public TableIdentifier getTableIdentifier(StreamRecord<RowData> record) {
      return TABLE_IDENTIFIER;
    }

    @Override
    public EnrichedTableIdentifier createOrRefreshTable(TableIdentifier tableIdentifier,
                                                        Optional<TableIdentifierProperties> currentProps,
                                                        StreamRecord<RowData> record) {
      return currentProps.map(tableIdentifierProperties ->
              EnrichedTableIdentifier.of(TABLE_IDENTIFIER, tableIdentifierProperties))
              .orElseGet(() -> EnrichedTableIdentifier.of(TABLE_IDENTIFIER,
                      new TableIdentifierProperties()));
    }
  }


  private static class ComplexProvider implements PayloadTableSinkProvider<RowData>, Serializable {
    @Override
    public TableIdentifier getTableIdentifier(StreamRecord<RowData> record) {
      int val = record.getValue().getInt(0);
      if (val%2 == 0) {
        return TABLE_IDENTIFIER;
      }
      return SECONDARY_TABLE_IDENTIFIER;
    }

    @Override
    public EnrichedTableIdentifier createOrRefreshTable(TableIdentifier tableIdentifier,
                                                        Optional<TableIdentifierProperties> currentProps,
                                                        StreamRecord<RowData> record) {
      if(!currentProps.isPresent()) {
        TableIdentifierProperties props = new TableIdentifierProperties();
        props.put("testKey", "oldVal");
        return EnrichedTableIdentifier.of(tableIdentifier, props);
      }
      int val = record.getValue().getInt(0);
      if (val%2 == 0) {
        return EnrichedTableIdentifier.of(tableIdentifier, currentProps.get());
      }
      TableIdentifierProperties newProps = new TableIdentifierProperties();
      newProps.put("testKey", "oldVal");
      return EnrichedTableIdentifier.of(tableIdentifier, newProps);
    }
  }

  private static class OddEvenWithPropsAlteringProvider implements PayloadTableSinkProvider<RowData>, Serializable {
    @Override
    public TableIdentifier getTableIdentifier(StreamRecord<RowData> record) {
      int val = record.getValue().getInt(0);
      if (val%2 == 0) {
        return TABLE_IDENTIFIER;
      }
      return SECONDARY_TABLE_IDENTIFIER;
    }

    @Override
    public EnrichedTableIdentifier createOrRefreshTable(TableIdentifier tableIdentifier,
                                                        Optional<TableIdentifierProperties> currentProps,
                                                        StreamRecord<RowData> record) {
      if(!currentProps.isPresent()) {
        TableIdentifierProperties props = new TableIdentifierProperties();
        props.put("testKey", "oldVal");
        return EnrichedTableIdentifier.of(tableIdentifier, props);
      }
      int val = record.getValue().getInt(0);
      if (val%2 == 0) {
        return EnrichedTableIdentifier.of(tableIdentifier, currentProps.get());
      }
      TableIdentifierProperties newProps = new TableIdentifierProperties();
      newProps.put("testKey", "newVal");
      return EnrichedTableIdentifier.of(tableIdentifier, newProps);
    }
  }
}
