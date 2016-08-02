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
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A <b>ColumnAuditor</b> object (obtained via a {@code RepositoryAdmin}'s
 * {@link RepositoryAdmin#getColumnAuditors(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
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
 * <i>Column</i>s via the method {@link RepositoryAdmin#discoverColumnMetadata(boolean, boolean)}.
 */
public class ColumnAuditor extends Column {

  /**
   * Key for the MAX_VALUE_LENGTH_KEY attribute.
   */
  static final String MAX_VALUE_LENGTH_KEY = "MAX_VALUE_LENGTH_FOUND";
  static final String COL_COUNTER_KEY = Bytes.toString(Repository.COL_COUNTER_QUALIFIER);
  static final String CELL_COUNTER_KEY = Bytes.toString(Repository.CELL_COUNTER_QUALIFIER);
  static final String COL_COUNTER_TIMESTAMP_KEY_STRING
          = Bytes.toString(Repository.COL_COUNTER_TIMESTAMP_KEY);
  static final String CELL_COUNTER_TIMESTAMP_KEY_STRING
          = Bytes.toString(Repository.CELL_COUNTER_TIMESTAMP_KEY);

  /**
   * Create {@code ColumnAuditor} instance for persisting user-specified
   * {@link #setValue(byte[], byte[]) values} and/or
   * {@link #setConfiguration(java.lang.String, java.lang.String) configurations}.
   *
   * @param columnQualifier Column Qualifier
   */
  public ColumnAuditor(byte[] columnQualifier) {
    super(SchemaEntityType.COLUMN_AUDITOR.getRecordType(), columnQualifier);
  }

  /**
   * Create {@code ColumnAuditor} instance for persisting user-specified
   * {@link #setValue(byte[], byte[]) values} and/or
   * {@link #setConfiguration(java.lang.String, java.lang.String) configurations}.
   *
   * @param columnQualifier Column Qualifier
   */
  public ColumnAuditor(String columnQualifier) {
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
    for (Entry<String, String> valueEntry : entity.getValuesStringMap().entrySet()) {
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
  public final ColumnAuditor setValue(String key, String value) {
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
  public final ColumnAuditor setValue(byte[] key, byte[] value) {
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
  public final ColumnAuditor setValue(
          final ImmutableBytesWritable key, final ImmutableBytesWritable value) {
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
  public final ColumnAuditor setConfiguration(String key, String value) {
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
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean, boolean) column metadata discovery}).
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

  /**
   * Get the count of rows in the Table in which this ColumnAuditor's column-qualifier appears.
   * This method returns a value of -1 if
   * {@link RepositoryAdmin#discoverColumnMetadata(org.apache.hadoop.hbase.TableName, boolean, boolean)
   * column discovery} has not been run.
   *
   * @return count of rows in which this ColumnAuditor's column-qualifier appears, or -1 if
   * {@link RepositoryAdmin#discoverColumnMetadata(org.apache.hadoop.hbase.TableName, boolean, boolean)
   * column discovery} has not been run
   */
  public long getColumnOccurrencesCount() {
    String value = getValue(COL_COUNTER_KEY);
    return (value == null) ? -1 : Long.valueOf(value);
  }

  /**
   * Get timestamp of latest invocation of
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean, boolean) column metadata discovery}
   * which incremented column occurrences count for this {@code ColumnAuditor}.
   *
   * @return timestamp of latest
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean, boolean) discovery invocation}
   * which incremented column occurrences count for this {@code ColumnAuditor}
   * {@code ColumnAuditor}; returns 0 if
   * {@link RepositoryAdmin#discoverColumnMetadata(org.apache.hadoop.hbase.TableName, boolean, boolean)
   * column discovery} has not been run
   */
  public long getColumnOccurrencesTimestamp() {
    String value = getValue(COL_COUNTER_TIMESTAMP_KEY_STRING);
    return (value == null) ? 0 : Long.valueOf(value);
  }

  /**
   * Get the count of all the cells in all the columns which have this ColumnAuditor's
   * column-qualifier.
   * This method returns a value of -1 if
   * {@link RepositoryAdmin#discoverColumnMetadata(org.apache.hadoop.hbase.TableName, boolean, boolean)
   * column discovery} has not been run. For a <b>complete</b> count of all cells, column discovery
   * must be run with the {@code includeAllCells} parameter set to {@code true}.
   *
   * @return count of all the cells in all the columns which have this ColumnAuditor's
   * column-qualifier, or -1 if
   * {@link RepositoryAdmin#discoverColumnMetadata(org.apache.hadoop.hbase.TableName, boolean, boolean)
   * column discovery} has not been run
   */
  public long getCellOccurrencesCount() {
    String value = getValue(CELL_COUNTER_KEY);
    return (value == null) ? -1 : Long.valueOf(value);
  }

  /**
   * Get timestamp of latest invocation of
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean, boolean) column metadata discovery}
   * which incremented cell occurrences count for this {@code ColumnAuditor}.
   *
   * @return timestamp of latest
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean, boolean) discovery invocation}
   * which incremented cell occurrences count for this {@code ColumnAuditor}
   * {@code ColumnAuditor}; returns 0 if
   * {@link RepositoryAdmin#discoverColumnMetadata(org.apache.hadoop.hbase.TableName, boolean, boolean)
   * column discovery} has not been run
   */
  public long getCellOccurrencesTimestamp() {
    String value = getValue(CELL_COUNTER_TIMESTAMP_KEY_STRING);
    return (value == null) ? 0 : Long.valueOf(value);
  }


}
