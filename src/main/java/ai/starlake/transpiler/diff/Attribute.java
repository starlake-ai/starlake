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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;


public class Attribute {
    final String name;
    final String type;
    final boolean isArray;
    AttributeStatus status;

    ArrayList<Attribute> attributes; // present only if typ is "struct"

    /*public boolean isNestedField() {
        return "struct".equalsIgnoreCase(type) && attributes != null && !attributes.isEmpty();
    }*/

    public boolean isArray() {
        return isArray;
    }

    public Attribute(String name, String type, boolean array, Collection<Attribute> attributes,
                     AttributeStatus status) {
        this.name = name;
        this.type = type;
        this.isArray = array;
        this.attributes = attributes != null ? new ArrayList<>(attributes) : null;
        this.status = status;
    }

    public Attribute(String name, String type) {
        this.name = name;
        this.type = type;
        this.isArray = false;
        this.attributes = null;
        this.status = AttributeStatus.UNCHANGED;
    }

    public Attribute(String name, Class<?> type) {
        this.name = name;
        this.type = type.getSimpleName().toLowerCase();
        this.isArray = false;
        this.attributes = null;
        this.status = AttributeStatus.UNCHANGED;
    }

    public Attribute(String name, String type, AttributeStatus status) {
        this.name = name;
        this.type = type;
        this.isArray = false;
        this.attributes = null;
        this.status = status;
    }

    public Attribute(String name, Class<?> type, AttributeStatus status) {
        this.name = name;
        this.type = type.getSimpleName().toLowerCase();
        this.isArray = false;
        this.attributes = null;
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public AttributeStatus getStatus() {
        return status;
    }

    public Attribute setStatus(AttributeStatus status) {
        this.status = status;
        return this;
    }

    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

    public Attribute setAttributes(ArrayList<Attribute> attributes) {
        this.attributes = attributes;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof Attribute)) {
            return false;
        }

        Attribute attribute = (Attribute) o;
        return isArray == attribute.isArray && name.equalsIgnoreCase(attribute.name)
                && Objects.equals(type, attribute.type);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + Boolean.hashCode(isArray);
        return result;
    }

    @Override
    public String toString() {
        return "Attribute{" + "name='" + name + '\'' + ", type='" + type + '\'' + ", isArray=" + isArray
                + ", status=" + status + ", attributes=" + attributes + '}';
    }
}