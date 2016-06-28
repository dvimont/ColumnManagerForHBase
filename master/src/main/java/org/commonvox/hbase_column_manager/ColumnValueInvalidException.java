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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Thrown when a <i>Column Family</i> has its
 * {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
 * ColumnDefinitionsEnforced} setting enabled, and the <i>Column value</i> submitted in a
 * {@link org.apache.hadoop.hbase.client.Mutation Put} to the
 * <i>Column Family</i> does NOT pass a validation stipulated by the
 * <i>Column</i>'s corresponding {@link ColumnDefinition}.
 *
 * @author Daniel Vimont
 */
public class ColumnValueInvalidException extends ColumnManagerIOException {

  /**
   * Constructs an instance of <code>InvalidColumnValueException</code> with the optional message
   * extension appended to a standard "invalid column value submitted" message.
   *
   * @param tableName <i>Table</i> name
   * @param colFamily <i>Column Family</i> name
   * @param colQualifier <i>Column Qualifier</i>
   * @param value the invalid value that was submitted in attempted {@code Mutation}
   * @param msgExtension optional extension to standard message
   */
  ColumnValueInvalidException(byte[] tableName, byte[] colFamily,
          byte[] colQualifier, byte[] value, String msgExtension) {
    super("Invalid column value submitted in attempted Mutation of Table: <"
            + Bytes.toString(tableName)
            + "> Column Family: <" + Bytes.toString(colFamily)
            + "> Column Qualifier: <" + Bytes.toString(colQualifier) + ">"
            + ((value == null) ? ""
                    : ". Column Value submitted: " + ((value.length < 120)
                            ? Repository.getPrintableString(value)
                            : Repository.getPrintableString(Bytes.copy(value, 0, 120))))
            + ". " + ((msgExtension == null) ? "" : msgExtension));
  }
}
