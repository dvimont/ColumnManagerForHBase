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

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Enum for all the types of metadata entities tracked and managed in the ColumnManager repository;
 * used publicly in {@link ChangeEvent} reporting.
 */
public enum EntityType {

  /**
   * Namespace
   */
  NAMESPACE((byte) 'N', "Namespace"),
  /**
   * Table
   */
  TABLE((byte) 'T', "Table"),
  /**
   * Column Family
   */
  COLUMN_FAMILY((byte) 'F', "ColumnFamily"),
  /**
   * Column Auditor
   */
  COLUMN_AUDITOR((byte) 'C', "ColumnAuditor"),
  /**
   * Column Definition
   */
  COLUMN_DEFINITION((byte) 'D', "ColumnDefinition");

  static final Map<Byte, EntityType> ENTITY_TYPE_BYTE_TO_ENUM_MAP = new TreeMap<>();
  static final Map<String, Byte> ENTITY_TYPE_LABEL_TO_BYTE_MAP = new TreeMap<>();

  static {
    ENTITY_TYPE_BYTE_TO_ENUM_MAP.put(EntityType.NAMESPACE.getRecordType(), EntityType.NAMESPACE);
    ENTITY_TYPE_BYTE_TO_ENUM_MAP.put(EntityType.TABLE.getRecordType(), EntityType.TABLE);
    ENTITY_TYPE_BYTE_TO_ENUM_MAP.put(EntityType.COLUMN_FAMILY.getRecordType(), EntityType.COLUMN_FAMILY);
    ENTITY_TYPE_BYTE_TO_ENUM_MAP.put(EntityType.COLUMN_AUDITOR.getRecordType(), EntityType.COLUMN_AUDITOR);
    ENTITY_TYPE_BYTE_TO_ENUM_MAP.put(EntityType.COLUMN_DEFINITION.getRecordType(), EntityType.COLUMN_DEFINITION);

    for (Entry<Byte, EntityType> entry : ENTITY_TYPE_BYTE_TO_ENUM_MAP.entrySet()) {
      ENTITY_TYPE_LABEL_TO_BYTE_MAP.put(entry.getValue().toString(), entry.getKey());
    }
  }

  private final byte recordType;   // in kilograms
  private final String entityTypeLabel; // in meters

  EntityType(byte recordType, String entityTypeLabel) {
    this.recordType = recordType;
    this.entityTypeLabel = entityTypeLabel;
  }

  /**
   * Get the record type value used for the {@code EntityType} in internal ColumnManager processing.
   *
   * @return record type code for {@code EntityType}
   */
  byte getRecordType() {
    return recordType;
  }

  /**
   * Get the String label for the {@code EntityType} used in {@link ChangeEvent} reporting, etc.
   *
   * @return String label for {@code EntityType}
   */
  @Override
  public String toString() {
    return entityTypeLabel;
  }
}
