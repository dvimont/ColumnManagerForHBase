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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 *
 * @author Daniel Vimont
 */
class InvalidColumnReport implements Closeable, AutoCloseable {

  private static final Log LOG = LogFactory.getLog(InvalidColumnReport.class);
  static final CSVFormat SUMMARY_CSV_FORMAT = CSVFormat.DEFAULT.withRecordSeparator("\n")
          .withCommentMarker('#').withHeader(SummaryReportHeader.class);
  static final CSVFormat VERBOSE_CSV_FORMAT = CSVFormat.DEFAULT.withRecordSeparator("\n")
          .withCommentMarker('#').withHeader(VerboseReportHeader.class);
  private static final Logger staticLogger
          = Logger.getLogger(Repository.class.getName());
  static final String TEMP_REPORT_NAMESPACE = "__column_manager_temp_reports";
  private static final String TEMP_REPORT_TABLENAME_PREFIX = "temp_report_table_";
  private static final byte[] TEMP_REPORT_CF = Bytes.toBytes("cr");
  private static final byte ROW_ID_DELIMITER = ':';
  private static final String ROW_ID_DELIMITER_STRING = String.valueOf((char)ROW_ID_DELIMITER);

  private static final String TABLE_NAME_ARG_KEY = "--sourceTable=";

  private final Connection standardConnection;
  private final Admin standardAdmin;
  private final MTableDescriptor sourceMtd; // descriptor for table being analyzed
  private final Table sourceTable; // table being analyzed
  private final byte[] sourceColFamily; // colFamily being analyzed (optional)
  private final Table tempReportTable; // table to which analysis metadata is written
  private final File targetFile;
  private final boolean verboseReport;
  private final boolean invokedByMapper;
  private final ReportType reportType;
  enum ReportType {QUALIFIER, LENGTH, VALUE};

  InvalidColumnReport(ReportType reportType, Connection connection,
          MTableDescriptor sourceTableDescriptor,
          byte[] sourceColFamily, File targetFile, boolean verbose, boolean useMapreduce)
          throws Exception {
    this.reportType = reportType;
    this.targetFile = targetFile;
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      this.standardConnection = ((MConnection)connection).getStandardConnection();
    } else {
      standardConnection = connection;
    }
    standardAdmin = standardConnection.getAdmin();
    createTempReportNamespace(standardAdmin);
    sourceMtd = sourceTableDescriptor;
    sourceTable = connection.getTable(sourceTableDescriptor.getTableName());
    this.sourceColFamily = sourceColFamily;

    TableName tempReportTableName
            = TableName.valueOf(TEMP_REPORT_NAMESPACE, TEMP_REPORT_TABLENAME_PREFIX
                            + new Timestamp(System.currentTimeMillis()).toString().
                                    replaceAll("[\\.\\-: ]", ""));
    standardAdmin.createTable(new HTableDescriptor(tempReportTableName).
            addFamily(new HColumnDescriptor(TEMP_REPORT_CF)));
    tempReportTable = standardConnection.getTable(tempReportTableName);
    verboseReport = verbose;
    invokedByMapper = false;
    if (useMapreduce) {
      collectReportMetadataViaMapreduce();
    } else {
      collectReportMetadataViaDirectScan();
    }
  }

  /**
   * This constructor invoked in MapReduce context from InvalidColumnReportMapper#setup.
   */
  InvalidColumnReport(ReportType reportType, Connection connection,
          MTableDescriptor sourceTableDescriptor, TableName tempReportTableName, boolean verbose)
          throws IOException {
    this.reportType = reportType;
    this.targetFile = null;
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      this.standardConnection = ((MConnection)connection).getStandardConnection();
    } else {
      standardConnection = connection;
    }
    standardAdmin = connection.getAdmin();
    sourceMtd = sourceTableDescriptor;
    sourceTable = standardConnection.getTable(sourceTableDescriptor.getTableName());
    sourceColFamily = null; // not needed by Mapper; only referenced by Tool in Scan setup.
    tempReportTable = standardConnection.getTable(tempReportTableName);
    verboseReport = verbose;
    invokedByMapper = true;
  }

  /**
   * Note that outputting invalid column metadata to an HBase table is intended to make for
   * easy implementation in a distributed mapreduce version of this procedure.
   *
   * @throws IOException if a remote or network exception occurs
   */
  private void collectReportMetadataViaDirectScan() throws IOException {
    // perform full scan (w/ KeyOnlyFilter(true) if summary report)
    Scan scan = new Scan();
    if (!verboseReport && !reportType.equals(ReportType.VALUE)) {
      scan.setFilter(new KeyOnlyFilter(true));
    }
    if (sourceColFamily != null) {
      scan.addFamily(sourceColFamily);
    }
    try (ResultScanner rows = sourceTable.getScanner(scan)) {
      for (Result row : rows) {
        doSourceRowProcessing(row);
      }
    }
  }

  /**
   * This method directly invoked by InvalidColumnReportMapper during MapReduce processing
   */
  void doSourceRowProcessing (Result row) throws IOException {
    for (Entry<byte[], NavigableMap<byte[],byte[]>> familyToColumnsMapEntry :
            row.getNoVersionMap().entrySet()) {
      MColumnDescriptor mcd = sourceMtd.getMColumnDescriptor(familyToColumnsMapEntry.getKey());
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

  /**
   * RowId layout: <namespace:table:colFamily:colQualifier>
   */
  private byte[] buildRowId(byte[] colFamily, byte[] colQualifier) {
    ByteBuffer rowId = ByteBuffer.allocate(3 + sourceTable.getName().getNamespace().length
            + sourceTable.getName().getQualifier().length + colFamily.length + colQualifier.length);
    rowId.put(sourceTable.getName().getNamespace()).put(ROW_ID_DELIMITER).
            put(sourceTable.getName().getQualifier()).put(ROW_ID_DELIMITER).
            put(colFamily).put(ROW_ID_DELIMITER).put(colQualifier);
    return rowId.array();
  }

  private String[] parseRowId(byte[] rowId) {
    return Bytes.toString(rowId).split(ROW_ID_DELIMITER_STRING, 4);
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
            CSVPrinter csvPrinter = csvFormat.withHeaderComments((verboseReport ? "VERBOSE" : "SUMMARY")
                            + " Report on Invalid Column " + this.reportType + "S in Table <"
                            + sourceTable.getName().getNameAsString()
                            + (sourceColFamily == null ? "" :
                                    ">, ColumnFamily <" + Bytes.toString (sourceColFamily))
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
    dropTempReportTables(standardAdmin);
    standardAdmin.deleteNamespace(TEMP_REPORT_NAMESPACE);
    staticLogger.warn("DROP (disable/delete) of " + Repository.PRODUCT_NAME
            + " TempReport tables and namespace has been completed: " + TEMP_REPORT_NAMESPACE);
  }

  static void dropTempReportTables(Admin standardAdmin) throws IOException {
    standardAdmin.disableTables(TEMP_REPORT_NAMESPACE + ":" + ".*");
    standardAdmin.deleteTables(TEMP_REPORT_NAMESPACE + ":" + ".*");
  }

  @Override
  public void close() throws IOException {
    sourceTable.close();
    tempReportTable.close();
    if (!invokedByMapper) {
      outputReport();
      standardAdmin.disableTable(tempReportTable.getName());
      standardAdmin.deleteTable(tempReportTable.getName());
    }
  }

  private void collectReportMetadataViaMapreduce() throws Exception {
    List<String> argList = new ArrayList<>();
    argList.add(Repository.TABLE_NAME_ARG_KEY + sourceMtd.getTableName().getNameAsString());
    if (sourceColFamily != null) {
      argList.add(Repository.COLFAMILY_ARG_KEY + Bytes.toString(sourceColFamily));
    }
    argList.add(InvalidColumnReportTool.REPORT_TYPE_ARG_KEY + reportType.name());
    argList.add(InvalidColumnReportTool.REPORT_TEMP_TABLE_ARG_KEY + tempReportTable.getName());
    argList.add(InvalidColumnReportTool.REPORT_VERBOSE_ARG_KEY + verboseReport);

    int jobCompletionCode = ToolRunner.run(MConfiguration.create(), new InvalidColumnReportTool(),
            argList.toArray(new String[argList.size()]));
    if (jobCompletionCode != 0) {
      LOG.warn("Mapreduce process failure in " + this.getClass().getSimpleName());
    }
  }

  static class InvalidColumnReportTool extends Configured implements Tool  {

    private static final Log LOG = LogFactory.getLog(InvalidColumnReportTool.class);
    static final String REPORT_TYPE_CONF_KEY
            = Repository.COLMANAGER_MAP_CONF_KEY_PREFIX + "report.type";
    static final String REPORT_VERBOSE_CONF_KEY
            = Repository.COLMANAGER_MAP_CONF_KEY_PREFIX + "report.verbose";
    static final String REPORT_TEMP_TABLE_CONF_KEY
            = Repository.COLMANAGER_MAP_CONF_KEY_PREFIX + "report.target.temptable";
    static final String REPORT_TYPE_ARG_KEY
            = Repository.ARG_KEY_PREFIX + REPORT_TYPE_CONF_KEY + Repository.ARG_DELIMITER;
    static final String REPORT_TEMP_TABLE_ARG_KEY
            = Repository.ARG_KEY_PREFIX + REPORT_TEMP_TABLE_CONF_KEY + Repository.ARG_DELIMITER;
    static final String REPORT_VERBOSE_ARG_KEY
            = Repository.ARG_KEY_PREFIX + REPORT_VERBOSE_CONF_KEY + Repository.ARG_DELIMITER;

    private String sourceTableNameString = null;
    private byte[] sourceColFamily = null;
    private boolean verboseReport = false;
    private InvalidColumnReport.ReportType reportType;

    Job createSubmittableJob(final String[] args) throws IOException {
      Configuration configFromArgs = parseArguments(args);
      if (configFromArgs == null || sourceTableNameString == null) {
        return null;
      }
      getConf().addResource(configFromArgs);
      getConf().setBoolean(Repository.MAP_SPECULATIVE_CONF_KEY, true); // no redundant processing

      Job job = Job.getInstance(
              getConf(), getConf().get(Repository.JOB_NAME_CONF_KEY, sourceTableNameString));
      TableMapReduceUtil.addDependencyJars(job);
      Scan scan = new Scan();
      // note that user can override scan row-caching by setting TableInputFormat.SCAN_CACHEDROWS
      scan.setCaching(getConf().getInt(TableInputFormat.SCAN_CACHEDROWS, 500));
      scan.setCacheBlocks(false);  // should be false for MapReduce jobs

      if (!verboseReport && !reportType.equals(ReportType.VALUE)) {
        scan.setFilter(new KeyOnlyFilter(true));
      }
      if (sourceColFamily != null) {
        scan.addFamily(sourceColFamily);
      }
      TableMapReduceUtil.initTableMapperJob(
              sourceTableNameString,
              scan,
              InvalidColumnReportMapper.class,
              null,  // mapper output key is null
              null,  // mapper output value is null
              job);
      job.setOutputFormatClass(NullOutputFormat.class);   // no Mapper output, no Reducer

      return job;
    }

    private Configuration parseArguments (final String[] args) {
      if (args.length < 1) {
        return null;
      }
      Configuration configFromArgs = new Configuration();
      for (String arg : args) {
        String[] keyValuePair = arg.substring(
                Repository.ARG_KEY_PREFIX.length()).split(Repository.ARG_DELIMITER);
        if (keyValuePair == null || keyValuePair.length != 2) {
          LOG.warn("ERROR in MapReduce " + this.getClass().getSimpleName()
                  + " submission: Invalid argument '" + arg + "'");
          return null;
        }
        switch (keyValuePair[0]) {
          case Repository.TABLE_NAME_CONF_KEY:
            sourceTableNameString = keyValuePair[1];
            break;
          case Repository.COLFAMILY_CONF_KEY:
            sourceColFamily = Bytes.toBytes(keyValuePair[1]);
            break;
          case REPORT_VERBOSE_CONF_KEY:
            verboseReport = keyValuePair[1].equalsIgnoreCase(Boolean.TRUE.toString());
            break;
          case REPORT_TYPE_CONF_KEY:
            reportType = InvalidColumnReport.ReportType.valueOf(keyValuePair[1]);
            break;
          case REPORT_TEMP_TABLE_CONF_KEY:
            break;
          default:
            LOG.warn("ERROR in MapReduce " + this.getClass().getSimpleName()
                    + " submission: Invalid argument '" + arg + "'");
            return null;
        }
        configFromArgs.set(keyValuePair[0], keyValuePair[1]);
      }
      return configFromArgs;
    }

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception if mapreduce job fails
     */
    public static void main(final String[] args) throws Exception {
      int ret = ToolRunner.run(MConfiguration.create(), new InvalidColumnReportTool(), args);
      System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
      Job job = createSubmittableJob(args);
      if (job == null) {
        return 1;
      }
      if (!job.waitForCompletion(true)) {
        LOG.warn(InvalidColumnReportTool.class.getSimpleName() + " mapreduce job failed!");
        return 1;
      }
      return 0;
    }
  }

  static class InvalidColumnReportMapper extends TableMapper<Text, Text> {
    private static final Log LOG = LogFactory.getLog(InvalidColumnReportMapper.class);
    private MConnection columnManagerConnection = null;
    private Repository repository = null;
    private MTableDescriptor sourceMtd;
    private TableName tempReportTableName; // table to which analysis metadata is written
    private boolean verboseReport;
    private ReportType reportType;
    private InvalidColumnReport invalidColumnReport;

    @Override
    protected void setup(Context context) {
      try {
        columnManagerConnection = (MConnection)MConnectionFactory.createConnection();
        repository = columnManagerConnection.getRepository();
        Configuration jobConfig = context.getConfiguration();
        reportType = InvalidColumnReport.ReportType.valueOf(
                jobConfig.get(InvalidColumnReportTool.REPORT_TYPE_CONF_KEY));
        sourceMtd = repository.getMTableDescriptor(TableName.valueOf(
                jobConfig.get(Repository.TABLE_NAME_CONF_KEY)));
        tempReportTableName = TableName.valueOf(
                        jobConfig.get(InvalidColumnReportTool.REPORT_TEMP_TABLE_CONF_KEY));
        verboseReport = jobConfig.get(InvalidColumnReportTool.REPORT_VERBOSE_CONF_KEY)
                .equalsIgnoreCase(Boolean.TRUE.toString());
        invalidColumnReport = new InvalidColumnReport(
                reportType, columnManagerConnection.getStandardConnection(), sourceMtd,
                tempReportTableName, verboseReport);
      } catch (Exception e) {
        columnManagerConnection = null;
        repository = null;
        LOG.warn(this.getClass().getSimpleName() + "mapper failed to initialize due to: "
                + e.getMessage());
      }
    }
    @Override
    protected void cleanup(Context context) {
      if (invalidColumnReport != null) {
        try {
          invalidColumnReport.close();
        } catch (IOException e) { }
      }
      if (columnManagerConnection != null) {
        try {
          columnManagerConnection.close();
        } catch (IOException e) { }
      }
    }

    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context)
            throws InterruptedException, IOException {
      if (columnManagerConnection == null || columnManagerConnection.isClosed()
              || columnManagerConnection.isAborted() || repository == null || sourceMtd == null) {
        return;
      }
      invalidColumnReport.doSourceRowProcessing(value);
    }
  }
}
