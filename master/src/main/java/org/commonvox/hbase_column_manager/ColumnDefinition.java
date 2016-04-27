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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * A <b>ColumnDefinition</b> (managed via a number of {@code RepositoryAdmin}  {@link RepositoryAdmin#addColumnDefinition(org.apache.hadoop.hbase.TableName, byte[],
 * org.commonvox.hbase_column_manager.ColumnDefinition)
 * add}, {@link RepositoryAdmin#getColumnDefinitions(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
 * get}, and {@link RepositoryAdmin#deleteColumnDefinition(org.apache.hadoop.hbase.TableName, byte[], byte[])
 * delete} methods) contains administrator-maintained settings pertaining to a specific
 * <i>Column Qualifier</i> within a <i>Column Family</i> of a
 * <a href="package-summary.html#config">ColumnManager-included</a> <i>Table</i>;<br>
 * when a <i>Column Family</i> has its  {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
 * ColumnDefinitionsEnforced} setting {@code enabled}, then (a) any <i>Column Qualifier</i>
 * submitted in a {@code put} (i.e., insert/update) to the <i>Table:Column-Family</i> must
 * correspond to an existing {@code ColumnDefinition}, and (b) the corresponding
 * <i>Column value</i> submitted must pass all validations (if any) stipulated by the
 * {@code ColumnDefinition}.
 */
public class ColumnDefinition extends Column {

  /**
   * Key for the COLUMN_LENGTH_KEY attribute.
   */
  static final String COLUMN_LENGTH_KEY = "COLUMN_LENGTH";
  /**
   * Default value for COLUMN_LENGTH_KEY attribute.
   */
  static final Long COLUMN_LENGTH_DEFAULT_VALUE = 0L;
  /**
   * Key for the COLUMN_VALIDATION_REGEX_KEY attribute.
   */
  static final String COLUMN_VALIDATION_REGEX_KEY = "COLUMN_VALIDATION_REGEX";
//    /** Key for the NAME_IS_REGEX_KEY attribute. */
//    static final String NAME_IS_REGEX_KEY = "NAME_IS_REGEX";

  /**
   * @param columnQualifier Column Qualifier
   */
  public ColumnDefinition(byte[] columnQualifier) {
    super(EntityType.COLUMN_DEFINITION.getRecordType(), columnQualifier);
  }

  /**
   * @param columnQualifier Column Qualifier
   */
  public ColumnDefinition(String columnQualifier) {
    super(EntityType.COLUMN_DEFINITION.getRecordType(), columnQualifier);
  }

  /**
   * This constructor accessed during deserialization process (i.e., building of objects by pulling
   * metadata components from Repository or from external archive).
   *
   * @param mEntity MetadataEntity to be deserialized into a ColumnDefinition
   */
  ColumnDefinition(MetadataEntity mEntity) {
    super(EntityType.COLUMN_DEFINITION.getRecordType(), mEntity.getName());
    this.setForeignKey(mEntity.getForeignKey());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry
            : mEntity.getValues().entrySet()) {
      this.setValue(valueEntry.getKey(), valueEntry.getValue());
    }
    for (Map.Entry<String, String> configEntry : mEntity.getConfiguration().entrySet()) {
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
  final ColumnDefinition setValue(String key, String value) {
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
  final ColumnDefinition setValue(byte[] key, byte[] value) {
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
  final ColumnDefinition setValue(final ImmutableBytesWritable key, final ImmutableBytesWritable value) {
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
  final ColumnDefinition setConfiguration(String key, String value) {
    super.setConfiguration(key, value);
    return this;
  }

  /**
   * Set the defined column length (i.e., the maximum length permitted for any submitted value for
   * this column when
   * {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
   * ColumnDefinitionsEnforced} is set to {@code true} for the column's <i>Column Family</i>);
   * setting the value to 0 (the default) will result in NO length validation of column values being
   * performed.
   *
   * @param columnLength length setting defined for column
   * @return this object, for method chaining
   */
  public ColumnDefinition setColumnLength(long columnLength) {
    return setValue(COLUMN_LENGTH_KEY, Long.toString(columnLength));
  }

  /**
   * Get the defined column length (i.e., the maximum length permitted for any submitted value for
   * this column when    {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
     * ColumnDefinitionsEnforced} is set to {@code true} for the column's <i>Column Family</i>); if
   * returned value is 0 (the default), NO length validation of column values will be performed.
   *
   * @return defined column length for this column
   */
  public long getColumnLength() {
    String value = getValue(COLUMN_LENGTH_KEY);
    return (value == null) ? COLUMN_LENGTH_DEFAULT_VALUE : Long.valueOf(value);
  }

  /**
   * Set the column's validation regex (i.e., the regular expression that any submitted value for
   * this column must match when    {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
     * ColumnDefinitionsEnforced} is set to {@code true} for the column's <i>Column Family</i>);
   * setting the value to blank (the default) will result in NO regular expression validation of
   * column values being performed.
   *
   * @param regexString regular expression validation-string defined for column
   * @return this object, for method chaining
   */
  public ColumnDefinition setColumnValidationRegex(String regexString) {
    return setValue(COLUMN_VALIDATION_REGEX_KEY, regexString);
  }

  /**
   * Get the defined column regex (i.e., the regular expression that any submitted value for this
   * column must match when
   * {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
   * ColumnDefinitionsEnforced} is set to {@code true} for the column's <i>Column Family</i>); if
   * returned value is blank (the default), NO regular expression validation of column values will
   * be performed.
   *
   * @return defined column length for this column
   */
  public String getColumnValidationRegex() {
    String value = getValue(COLUMN_VALIDATION_REGEX_KEY);
    return (value == null) ? "" : value;
  }
}
