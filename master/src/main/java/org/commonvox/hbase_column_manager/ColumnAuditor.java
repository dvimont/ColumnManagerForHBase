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

import java.util.Map.Entry;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * A <b>ColumnAuditor</b> object (obtained via a {@code RepositoryAdmin}'s  {@link RepositoryAdmin#getColumnAuditors(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
 * getColumnAuditors} method) contains captured* or discovered** metadata pertaining to a specific
 * <i>Column Qualifier</i> actively stored in HBase as part of a <i>Column Family</i> of a
 * <a href="package-summary.html#config">ColumnManager-included</a> <i>Table</i>.
 * <br><br>
 * *When <a href="package-summary.html#activate">ColumnManager is activated</a>,
 * {@code ColumnAuditor} metadata is gathered at runtime as
 * {@link org.apache.hadoop.hbase.client.Mutation}s are submitted via the
 * HBase API to any
 * <a href="package-summary.html#config">ColumnManager-included</a> <i>Table</i>.
 * <br><br> **{@code ColumnAuditor} metadata may also be gathered for already-existing
 * <i>Column</i>s via the method {@link RepositoryAdmin#discoverColumnMetadata(boolean)}.
 */
public class ColumnAuditor extends Column {

  /**
   * Key for the MAX_VALUE_LENGTH_KEY attribute.
   */
  static final String MAX_VALUE_LENGTH_KEY = "MAX_VALUE_LENGTH_FOUND";

  /**
   * @param columnQualifier Column Qualifier
   */
  ColumnAuditor(byte[] columnQualifier) {
    super(SchemaEntityType.COLUMN_AUDITOR.getRecordType(), columnQualifier);
  }

  /**
   * @param columnQualifier Column Qualifier
   */
  ColumnAuditor(String columnQualifier) {
    super(SchemaEntityType.COLUMN_AUDITOR.getRecordType(), columnQualifier);
  }

  /**
   * This constructor accessed during deserialization process (i.e., building of objects by pulling
   * schema components from Repository or from external archive).
   *
   * @param entity SchemaEntity to be deserialized into a Column
   */
  ColumnAuditor(SchemaEntity entity) {
    super(SchemaEntityType.COLUMN_AUDITOR.getRecordType(), entity.getName());
    this.setForeignKey(entity.getForeignKey());
    for (Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry
            : entity.getValues().entrySet()) {
      this.setValue(valueEntry.getKey(), valueEntry.getValue());
    }
    for (Entry<String, String> configEntry : entity.getConfiguration().entrySet()) {
      this.setConfiguration(configEntry.getKey(), configEntry.getValue());
    }
  }

  /**
   * Setter for adding value entry to value map
   *
   * @param key Value key
   * @param value Value value
   * @return this object to allow method chaining
   */
  @Override
  final ColumnAuditor setValue(String key, String value) {
    super.setValue(key, value);
    return this;
  }

  /**
   * Setter for adding value entry to value map
   *
   * @param key Value key
   * @param value Value value
   * @return this object to allow method chaining
   */
  @Override
  final ColumnAuditor setValue(byte[] key, byte[] value) {
    super.setValue(key, value);
    return this;
  }

  /**
   * Setter for adding value entry to value map
   *
   * @param key Value key
   * @param value Value value
   * @return this object to allow method chaining
   */
  @Override
  final ColumnAuditor setValue(final ImmutableBytesWritable key, final ImmutableBytesWritable value) {
    super.setValue(key, value);
    return this;
  }

  /**
   * Setter for adding configuration entry to configuration map
   *
   * @param key Configuration key
   * @param value Configuration value
   * @return this object to allow method chaining
   */
  @Override
  final ColumnAuditor setConfiguration(String key, String value) {
    super.setConfiguration(key, value);
    return this;
  }

  ColumnAuditor setMaxValueLengthFound(long maxValueLengthFound) {
    return setValue(MAX_VALUE_LENGTH_KEY, Long.toString(maxValueLengthFound));
  }

  /**
   * Get the length of the longest value found in HBase for this column. Note that
   * MaxValueLengthFound for a given column is incremented upward only when a value longer than that
   * of any previously submitted/discovered value is found (either in the context of real-time
   * metadata capture from a submitted Table {@link org.apache.hadoop.hbase.client.Mutation},
   * or in the process of
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean) column metadata discovery}).
   * MaxValueLengthFound is never
   * decremented, even when the longest value for the column is deleted from HBase, so the
   * MaxValueLengthFound actually represents the maximum length EVER recorded for the value of a
   * specific column, not necessarily the longest value CURRENTLY stored in HBase for the column.
   *
   * @return maximum value length found in HBase for this column
   */
  public long getMaxValueLengthFound() {
    String value = getValue(MAX_VALUE_LENGTH_KEY);
    return (value == null) ? 0 : Long.valueOf(value);
  }
}
