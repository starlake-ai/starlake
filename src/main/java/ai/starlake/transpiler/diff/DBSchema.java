/**
 * Starlake.AI JSQLTranspiler is a SQL to DuckDB Transpiler.
 * Copyright (C) 2025 Starlake.AI (hayssam.saleh@starlake.ai)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.starlake.transpiler.diff;

import ai.starlake.transpiler.schema.CaseInsensitiveLinkedHashMap;

import java.util.Arrays;
import java.util.Collection;

public class DBSchema {
    String catalogName;
    String schemaName;
    CaseInsensitiveLinkedHashMap<Collection<Attribute>> tables;

    public DBSchema(String catalogName, String schemaName) {
        this.catalogName = catalogName == null ? "" : catalogName;
        this.schemaName = schemaName == null ? "" : schemaName;
        this.tables = new CaseInsensitiveLinkedHashMap<>();
    }

    public DBSchema(String catalogName, String schemaName,
                    CaseInsensitiveLinkedHashMap<Collection<Attribute>> tables) {
        this.catalogName = catalogName == null ? "" : catalogName;
        this.schemaName = schemaName == null ? "" : schemaName;
        this.tables = tables;
    }

    public DBSchema(String catalogName, String schemaName, String tableName,
                    Collection<Attribute> attributes) {
        this.catalogName = catalogName == null ? "" : catalogName;
        this.schemaName = schemaName == null ? "" : schemaName;
        this.tables = new CaseInsensitiveLinkedHashMap<>();
        this.tables.put(tableName, attributes);
    }

    public DBSchema(String catalogName, String schemaName, String tableName,
                    Attribute... attributes) {
        this.catalogName = catalogName == null ? "" : catalogName;
        this.schemaName = schemaName == null ? "" : schemaName;
        this.tables = new CaseInsensitiveLinkedHashMap<>();
        this.tables.put(tableName, Arrays.asList(attributes));
    }

    public DBSchema put(String tableName, Attribute... attributes) {
        this.tables.put(tableName, Arrays.asList(attributes));
        return this;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public DBSchema setCatalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public DBSchema setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public CaseInsensitiveLinkedHashMap<Collection<Attribute>> getTables() {
        return tables;
    }

    public DBSchema setTables(CaseInsensitiveLinkedHashMap<Collection<Attribute>> tables) {
        this.tables = tables;
        return this;
    }
}