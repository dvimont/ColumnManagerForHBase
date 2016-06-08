/*
 * Copyright 2016 Daniel Vimont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonvox.hbase_column_manager;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Thrown when a submitted Table is not <a href="package-summary.html#config">included for
 * ColumnManager processing</a>, but the context permits only ColumnManager-included Tables.
 *
 * @author Daniel Vimont
 */
public class TableNotIncludedForProcessingException extends ColumnManagerIOException {

  TableNotIncludedForProcessingException(byte[] tableName,  String msgExtension) {
    super("Table not included for ColumnManagerAPI processing: <"
            + Bytes.toString(tableName) + ">. "
            + "Please see <" + Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES
            + "> or <" + Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES
            + "> configuration property for details on user Tables administratively included "
            + "for " + Repository.PRODUCT_NAME + " processing."
            + ((msgExtension == null) ? "" : " " + msgExtension));  }
}
