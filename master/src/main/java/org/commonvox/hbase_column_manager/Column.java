/*
 * Copyright (C) 2016 Daniel Vimont
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.commonvox.hbase_column_manager;

/**
 * Abstract class extended by Column-oriented entities; created to clarify/simplify access to Column
 * Qualifier name. Without the methods of this abstract class, a ColumnManager end-user would have
 * to use the ambiguously-named methods #getName and #getNameAsString.
 *
 * @author Daniel Vimont
 */
abstract class Column extends MetadataEntity {

  Column(byte entityType, byte[] name) {
    super(entityType, name);
  }

  Column(byte entityType, String name) {
    super(entityType, name);
  }

  /**
   * Get Column Qualifier.
   *
   * @return Column Qualifier as byte-array
   */
  public byte[] getColumnQualifier() {
    return this.getName();
  }

  /**
   * Get Column Qualifier as a String.
   *
   * @return Column Qualifier as String
   */
  public String getColumnQualifierAsString() {
    return this.getNameAsString();
  }
}
