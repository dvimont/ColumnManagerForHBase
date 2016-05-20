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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 *
 * @author Daniel Vimont
 */
class InvalidColumnReport implements Closeable {

  private static final Logger staticLogger
          = Logger.getLogger(Repository.class.getPackage().getName());
  private static final String TEMP_REPORT_NAMESPACE = "column_manager_temp_reports";
  private static final String TEMP_REPORT_TABLENAME_PREFIX = "temp_report_table_";
  private static final byte[] TEMP_REPORT_CF = Bytes.toBytes("cr");
  private static final byte ROW_ID_DELIMITER = ':';

  private final Connection standardConnection;
  private final Table userTable; // table being analyzed
  private final Table tempReportTable; // table to which analysis metadata is written
  private final File targetFile;
  private final boolean mapReduceMode;

  InvalidColumnReport(Connection connection, TableName userTableName, File targetFile,
          boolean verbose) throws IOException {
    mapReduceMode = false;
    this.targetFile = targetFile;
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      this.standardConnection = ((MConnection)connection).getStandardConnection();
    } else {
      standardConnection = connection;
    }
    createTempReportNamespace(standardConnection.getAdmin());
    userTable = connection.getTable(userTableName);

    TableName tempReportTableName
            = TableName.valueOf(TEMP_REPORT_NAMESPACE, TEMP_REPORT_TABLENAME_PREFIX
                            + new Timestamp(System.currentTimeMillis()).toString().
                                    replaceAll("[\\.: ]", "_"));
    standardConnection.getAdmin().createTable(new HTableDescriptor(tempReportTableName).
            addFamily(new HColumnDescriptor(TEMP_REPORT_CF)));
    tempReportTable = standardConnection.getTable(tempReportTableName);
  }

  public InvalidColumnReport(Connection connection, TableName userTableName,
          TableName tempReportTableName) throws IOException {
    mapReduceMode = true;
    this.targetFile = null;
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      this.standardConnection = ((MConnection)connection).getStandardConnection();
    } else {
      standardConnection = connection;
    }
    userTable = connection.getTable(userTableName);

    tempReportTable = standardConnection.getTable(tempReportTableName);
  }

  public TableName getTempReportTableName() {
    return tempReportTable.getName();
  }

  private String getTempReportTableNameSuffix() {
    return new Timestamp(System.currentTimeMillis()).toString().replaceAll("[\\.: ]", "_");
  }

  public void addEntry(byte[] colFamily, byte[] colQualifier, int valueLength, byte[] value,
          byte[] userTableRowId) {
    byte[] rowId = buildRowId(colFamily, colQualifier);
    System.out.println(Bytes.toString(rowId));
  }

  private byte[] buildRowId(byte[] colFamily, byte[] colQualifier) {
    ByteBuffer rowId = ByteBuffer.allocate(3 + userTable.getName().getNamespace().length
            + userTable.getName().getQualifier().length + colFamily.length + colQualifier.length);
    rowId.put(userTable.getName().getNamespace()).put(ROW_ID_DELIMITER).
            put(userTable.getName().getQualifier()).put(ROW_ID_DELIMITER).
            put(colFamily).put(ROW_ID_DELIMITER).put(colQualifier);
    return rowId.array();
  }

  static void createTempReportNamespace(Admin standardAdmin) throws IOException {
    NamespaceDescriptor tempReportNamespaceDescriptor
            = NamespaceDescriptor.create(TEMP_REPORT_NAMESPACE).build();
    if (!Repository.namespaceExists(standardAdmin, tempReportNamespaceDescriptor)) {
      standardAdmin.createNamespace(tempReportNamespaceDescriptor);
      staticLogger.info(
              "ColumnManager TempReport Namespace has been created (did not already exist): "
              + tempReportNamespaceDescriptor.getName());
    }
  }

  static void dropTempReportNamespace(Admin standardAdmin) throws IOException {
    if (!Repository.namespaceExists(standardAdmin, Bytes.toBytes(TEMP_REPORT_NAMESPACE))) {
      return;
    }
    staticLogger.warn("DROP (disable/delete) of " + Repository.PRODUCT_NAME
            + " TempReport tables and namespace has been requested.");
    standardAdmin.disableTables(TEMP_REPORT_TABLENAME_PREFIX + ".*");
    standardAdmin.deleteTables(TEMP_REPORT_TABLENAME_PREFIX + ".*");
    standardAdmin.deleteNamespace(TEMP_REPORT_NAMESPACE);
    staticLogger.warn("DROP (delete) of " + Repository.PRODUCT_NAME
            + " TempReport tables and namespace has been completed: " + TEMP_REPORT_NAMESPACE);
  }

  @Override
  public void close() throws IOException {
    userTable.close();
    tempReportTable.close();
    if (!mapReduceMode) {
      Admin admin = standardConnection.getAdmin();
      admin.disableTable(tempReportTable.getName());
      admin.deleteTable(tempReportTable.getName());
    }
  }
}
