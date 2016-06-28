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
 * ColumnDefinitionsEnforced} setting enabled, and a <i>Column Qualifier</i> submitted in a
 * {@code Put} to that <i>Column Family</i> does NOT correspond to an existing
 * {@link ColumnDefinition}.
 *
 * @author Daniel Vimont
 */
public class ColumnDefinitionNotFoundException extends ColumnManagerIOException {

  /**
   * Constructs an instance of <code>ColumnDefinitionNotFoundException</code> with the optional
   * message extension appended to a standard "column definition not found" message.
   *
   * @param tableName <i>Table</i> name
   * @param colFamily <i>Column Family</i> name
   * @param colQualifier <i>Column Qualifier</i>
   * @param msgExtension optional extension to standard message
   */
  ColumnDefinitionNotFoundException(byte[] tableName, byte[] colFamily,
          byte[] colQualifier, String msgExtension) {
    super("Invalid Column Qualifier submitted. Column Definition enforcement is active for"
            + " Table: <" + Bytes.toString(tableName)
            + "> Column Family: <" + Bytes.toString(colFamily)
            + ">. Column Definition NOT found that matches submitted Column Qualifier: <"
            + Bytes.toString(colQualifier)
            + ">. " + ((msgExtension == null) ? "" : msgExtension));
  }

}
