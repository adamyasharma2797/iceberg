/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.data;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.Objects;

public class EnrichedTableIdentifier {
    private TableIdentifier tableIdentifier;
    private TableIdentifierProperties tableIdentifierProperties;

    public static EnrichedTableIdentifier of(TableIdentifier tableIdentifier,
                                             TableIdentifierProperties tableIdentifierProperties) {
        return new EnrichedTableIdentifier(tableIdentifier, tableIdentifierProperties);
    }

    private EnrichedTableIdentifier(TableIdentifier tableIdentifier,
                                    TableIdentifierProperties tableIdentifierProperties) {
        Preconditions.checkArgument(
                tableIdentifier != null, "Invalid table identifier: null");
        Preconditions.checkArgument(tableIdentifier.name() != null, "Invalid table name: null");
        this.tableIdentifier = tableIdentifier;
        this.tableIdentifierProperties = tableIdentifierProperties;
    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public TableIdentifierProperties getTableIdentifierProperties() {
        return tableIdentifierProperties;
    }
}
