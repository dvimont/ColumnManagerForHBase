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
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 *
 * @author Daniel Vimont
 */
class InvalidColumnReport implements Closeable {

  public static final CSVFormat SUMMARY_CSV_FORMAT = CSVFormat.DEFAULT.withRecordSeparator("\n")
          .withCommentMarker('#').withHeader(SummaryReportHeader.class);
  public static final CSVFormat VERBOSE_CSV_FORMAT = CSVFormat.DEFAULT.withRecordSeparator("\n")
          .withCommentMarker('#').withHeader(VerboseReportHeader.class);
  private static final Logger staticLogger
          = Logger.getLogger(Repository.class.getPackage().getName());
  static final String TEMP_REPORT_NAMESPACE = "__column_manager_temp_reports";
  private static final String TEMP_REPORT_TABLENAME_PREFIX = "temp_report_table_";
  private static final byte[] TEMP_REPORT_CF = Bytes.toBytes("cr");
  private static final byte ROW_ID_DELIMITER = ':';
  private static final String ROW_ID_DELIMITER_STRING = String.valueOf((char)ROW_ID_DELIMITER);

  private final Connection standardConnection;
  private final MTableDescriptor userMtd;
  private final Table userTable; // table being analyzed
  private final byte[] userColFamily; // colFamily being analyzed (optional)
  private final Table tempReportTable; // table to which analysis metadata is written
  private final File targetFile;
  private final boolean verboseReport;
  private final boolean mapreduceSlave;
  private final ReportType reportType;
  enum ReportType {QUALIFIER, LENGTH, VALUE};

  InvalidColumnReport(ReportType reportType, Connection connection, MTableDescriptor userTableDescriptor,
          byte[] userColFamily, File targetFile, boolean verbose, boolean useMapreduce)
          throws IOException {
    this.reportType = reportType;
    this.targetFile = targetFile;
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      this.standardConnection = ((MConnection)connection).getStandardConnection();
    } else {
      standardConnection = connection;
    }
    createTempReportNamespace(standardConnection.getAdmin());
    userMtd = userTableDescriptor;
    userTable = connection.getTable(userTableDescriptor.getTableName());
    this.userColFamily = userColFamily;

    TableName tempReportTableName
            = TableName.valueOf(TEMP_REPORT_NAMESPACE, TEMP_REPORT_TABLENAME_PREFIX
                            + new Timestamp(System.currentTimeMillis()).toString().
                                    replaceAll("[\\.\\-: ]", ""));
    standardConnection.getAdmin().createTable(new HTableDescriptor(tempReportTableName).
            addFamily(new HColumnDescriptor(TEMP_REPORT_CF)));
    tempReportTable = standardConnection.getTable(tempReportTableName);
    verboseReport = verbose;
    mapreduceSlave = false;
    if (useMapreduce) {
      collectInvalidColumnMetadataViaMapreduce(); // invoke mapreduce method
    } else {
      collectInvalidColumnMetadataViaScan();
    }
  }

  public InvalidColumnReport(ReportType reportType, Connection connection,
          MTableDescriptor userTableDescriptor, byte[] userColFamily,
          TableName tempReportTableName, boolean verbose)
          throws IOException {
    this.reportType = reportType;
    this.targetFile = null;
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      this.standardConnection = ((MConnection)connection).getStandardConnection();
    } else {
      standardConnection = connection;
    }
    userMtd = userTableDescriptor;
    userTable = standardConnection.getTable(userTableDescriptor.getTableName());
    this.userColFamily = userColFamily;
    tempReportTable = standardConnection.getTable(tempReportTableName);
    verboseReport = verbose;
    mapreduceSlave = true;
    collectInvalidColumnMetadataViaScan();
  }

  /**
   * Note that collecting invalid column metadata in an HBase table is intended to make for
   * easy implementation in a distributed mapreduce version of this procedure.
   *
   * @throws IOException if a remote or network exception occurs
   */
  private void collectInvalidColumnMetadataViaScan() throws IOException {
    // perform full scan (w/ KeyOnlyFilter(true) if summary report)
    Scan columnScan = new Scan();
    if (!verboseReport && !reportType.equals(ReportType.VALUE)) {
      columnScan.setFilter(new KeyOnlyFilter(true));
    }
    if (userColFamily != null) {
      columnScan.addFamily(userColFamily);
    }
    try (ResultScanner rows = userTable.getScanner(columnScan)) {
      for (Result row : rows) {
        for (Entry<byte[], NavigableMap<byte[],byte[]>> familyToColumnsMapEntry :
                row.getNoVersionMap().entrySet()) {
          MColumnDescriptor mcd = userMtd.getMColumnDescriptor(familyToColumnsMapEntry.getKey());
          if (mcd == null || mcd.getColumnDefinitions().isEmpty()) { // no def? everything's valid!
            continue;
          }
          for (Entry<byte[],byte[]> colEntry : familyToColumnsMapEntry.getValue().entrySet()) {
            byte[] colQualifier = colEntry.getKey();
            byte[] colValue = colEntry.getValue();
            ColumnDefinition colDef = mcd.getColumnDefinition(colQualifier);
            boolean invalidRow = false;
            switch (reportType) {
              case QUALIFIER:
                if (colDef == null) {
                  invalidRow = true;
                }
                break;
              case LENGTH:
                if (colDef != null && colDef.getColumnLength() > 0) {
                  if (verboseReport) {
                    if (colValue.length > colDef.getColumnLength()) {
                      invalidRow = true;
                    }
                  } else {
                    if (Bytes.toInt(colValue) > colDef.getColumnLength()) {
                      invalidRow = true;
                    }
                  }
                }
                break;
              case VALUE:
                if (colDef != null && !colDef.getColumnValidationRegex().isEmpty()) {
                  if (!Bytes.toString(colValue).matches(colDef.getColumnValidationRegex())) {
                    invalidRow = true;
                  }
                }
                break;
            }
            if (invalidRow) {
              // upserts a user-column-specific report row with invalid user-column metadata
              tempReportTable.put(new Put(buildRowId(mcd.getName(), colQualifier))
                      .addColumn(TEMP_REPORT_CF, row.getRow(),
                              (colValue.length < 200 ? colValue :
                                      Bytes.add(Bytes.head(colValue, 200),
                                              Bytes.toBytes("[value-truncated]")))));
            }
          }
        }
      }
    }
  }

  /**
   * RowId layout: <namespace:table:colFamily:colQualifier>
   */
  private byte[] buildRowId(byte[] colFamily, byte[] colQualifier) {
    ByteBuffer rowId = ByteBuffer.allocate(3 + userTable.getName().getNamespace().length
            + userTable.getName().getQualifier().length + colFamily.length + colQualifier.length);
    rowId.put(userTable.getName().getNamespace()).put(ROW_ID_DELIMITER).
            put(userTable.getName().getQualifier()).put(ROW_ID_DELIMITER).
            put(colFamily).put(ROW_ID_DELIMITER).put(colQualifier);
    return rowId.array();
  }

  private String[] parseRowId(byte[] rowId) {
    return Bytes.toString(rowId).split(ROW_ID_DELIMITER_STRING, 4);
  }

  private void collectInvalidColumnMetadataViaMapreduce() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  boolean isEmpty() {
    boolean reportIsEmpty;
    try (ResultScanner pingScan = tempReportTable.getScanner(new Scan().setMaxResultSize(1))) {
      reportIsEmpty = (pingScan.next() == null);
    } catch (IOException e) {
      reportIsEmpty = true;
    }
    return reportIsEmpty;
  }

  enum SummaryReportHeader {
    NAMESPACE, TABLE, COLUMN_FAMILY, COLUMN_QUALIFIER, INVALID_OCCURRENCE_COUNT}

  enum VerboseReportHeader {
     NAMESPACE, TABLE, COLUMN_FAMILY, COLUMN_QUALIFIER, ROW_ID, COLUMN_VALUE}

  private void outputReport() throws IOException {
    CSVFormat csvFormat = (verboseReport ? VERBOSE_CSV_FORMAT : SUMMARY_CSV_FORMAT);
    try (ResultScanner rows = tempReportTable.getScanner(new Scan());
            CSVPrinter csvPrinter = csvFormat.withHeaderComments(
                    (verboseReport ? "VERBOSE" : "SUMMARY")
                            + " Report on Invalid Column " + this.reportType + "S in Table <"
                            + userTable.getName().getNameAsString()
                            + (userColFamily == null ? "" :
                                    ">, ColumnFamily <" + Bytes.toString (userColFamily))
                            + "> -- Generated by " + Repository.PRODUCT_NAME + ":"
                            + this.getClass().getSimpleName(),
                            new Date())
                            .print(new FileWriter(targetFile))) {
      for (Result row : rows) {
        NavigableMap<byte[],byte[]> tempReportColumnMap = row.getFamilyMap(TEMP_REPORT_CF);
        String[] reportLineComponents = parseRowId(row.getRow());
        if (verboseReport) { // print line for each invalid occurrence found
          for (Entry<byte[],byte[]> tempReportColumn : tempReportColumnMap.entrySet()) {
            for (String reportLineComponent : reportLineComponents) {
              csvPrinter.print(reportLineComponent);
            }
            csvPrinter.print(Repository.getPrintableString(tempReportColumn.getKey())); // userRowId
            csvPrinter.print(Repository.getPrintableString(tempReportColumn.getValue())); // colVal
            csvPrinter.println();
          }
        } else { // print summary line giving count of invalid occurrences
          for (String reportLineComponent : reportLineComponents) {
            csvPrinter.print(reportLineComponent);
          }
          csvPrinter.print(String.valueOf(tempReportColumnMap.size()));
          csvPrinter.println();
        }
      }
    }
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
    standardAdmin.disableTables(TEMP_REPORT_NAMESPACE + ":" + ".*");
    standardAdmin.deleteTables(TEMP_REPORT_NAMESPACE + ":" + ".*");
    standardAdmin.deleteNamespace(TEMP_REPORT_NAMESPACE);
    staticLogger.warn("DROP (delete) of " + Repository.PRODUCT_NAME
            + " TempReport tables and namespace has been completed: " + TEMP_REPORT_NAMESPACE);
  }

  @Override
  public void close() throws IOException {
    userTable.close();
    tempReportTable.close();
    if (!mapreduceSlave) {
      outputReport();
      Admin admin = standardConnection.getAdmin();
      admin.disableTable(tempReportTable.getName());
      admin.deleteTable(tempReportTable.getName());
    }
  }
}
