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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.data.EnrichedTableIdentifier;
import org.apache.iceberg.flink.data.TableIdentifierProperties;

import java.util.Optional;

public class TestPayloadSinkProvider implements PayloadTableSinkProvider<RowData> {

  @Override
  public TableIdentifier getTableIdentifier(StreamRecord<RowData> record) {
    RowData value = record.getValue();
    String identifier = value.getString(1).toString().split("\\.")[0];
    return TableIdentifier.of(sinkDatabaseName(), baseTable() + "_" + identifier);
  }

  @Override
  public EnrichedTableIdentifier createOrRefreshTable(TableIdentifier tableIdentifier,
                                                      Optional<TableIdentifierProperties> currentProps,
                                                      StreamRecord<RowData> record) {
    // Do not change any properties and identifier
    return EnrichedTableIdentifier.of(tableIdentifier, currentProps.orElse(new TableIdentifierProperties()));
  }

  private String sinkDatabaseName() {
    return "test_raw";
  }

  private String baseTable() {
    return "test_table";
  }
}