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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableMultiplexer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Running of these methods requires that an up-and-running instance of HBase be accessible. (The
 * emulation environment provided by HBaseTestUtility is not appropriate for these tests.)
 *
 * @author Daniel Vimont
 */
public class TestColumnAliasing {
  private static final List<String> TEST_NAMESPACE_LIST
          = new ArrayList<>(Arrays.asList("testNamespace01", "testNamespace02", ""));
  private static final List<String> TEST_TABLE_NAME_LIST
          = new ArrayList<>(
                  Arrays.asList("testTable01", "testTable02", "testTable03"));
  private static final List<byte[]> TEST_COLUMN_FAMILY_LIST
          = new ArrayList<>(Arrays.asList(Bytes.toBytes("CF1"), Bytes.toBytes("CF2")));
  private static final List<byte[]> TEST_COLUMN_QUALIFIER_LIST
          = new ArrayList<>(Arrays.asList(Bytes.toBytes("column01"), Bytes.toBytes("column02"),
                          Bytes.toBytes("column03"), Bytes.toBytes("column04"),
                          Bytes.toBytes("column05"), Bytes.toBytes("column06"),
                          Bytes.toBytes("column07"), Bytes.toBytes("column08")));
//private static final byte[] QUALIFIER_IN_EXCLUDED_TABLE = Bytes.toBytes("qualifierOnExcludedTable");
  private static final byte[] ROW_ID_01 = Bytes.toBytes("rowId01");
  private static final byte[] ROW_ID_02 = Bytes.toBytes("rowId02");
  private static final byte[] ROW_ID_03 = Bytes.toBytes("rowId03");
  private static final byte[] ROW_ID_04 = Bytes.toBytes("rowId04");
  private static final byte[] ROW_ID_05 = Bytes.toBytes("rowId05");
  private static final List<byte[]> TEST_ROW_ID_LIST = new LinkedList<>();
  static {
    TEST_ROW_ID_LIST.add(ROW_ID_01);
    TEST_ROW_ID_LIST.add(ROW_ID_02);
    TEST_ROW_ID_LIST.add(ROW_ID_03);
    TEST_ROW_ID_LIST.add(ROW_ID_04);
    TEST_ROW_ID_LIST.add(ROW_ID_05);
  }
  private static final byte[] VALUE_2_BYTES_LONG = Bytes.toBytes("xy");
  private static final byte[] VALUE_5_BYTES_LONG = Bytes.toBytes("54321");
  private static final byte[] VALUE_9_BYTES_LONG = Bytes.toBytes("123456789");
  private static final byte[] VALUE_82_BYTES_LONG = new byte[82];
  static {
    Arrays.fill(VALUE_82_BYTES_LONG, (byte) 'A');
  }
  // namespace01 has ALL tables included in CM processing
  private static final int NAMESPACE01_INDEX = 0;
  // namespace02 is NOT included in CM processing
  private static final int NAMESPACE02_INDEX = 1;
  // namespace03's table02 NOT included in CM processing
  private static final int NAMESPACE03_INDEX = 2;
  private static final int TABLE01_INDEX = 0;
  private static final int TABLE02_INDEX = 1;
  private static final int TABLE03_INDEX = 2;

  private static final String NAMESPACE01 = TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX);
  private static final String NAMESPACE02 = TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX);
  private static final String NAMESPACE03 = TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX);
  private static final TableName NAMESPACE01_TABLE01
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE01_INDEX));
  private static final TableName NAMESPACE01_TABLE02
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE02_INDEX));
  private static final TableName NAMESPACE01_TABLE03
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE03_INDEX));
  private static final TableName NAMESPACE02_TABLE01
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE01_INDEX));
  private static final TableName NAMESPACE02_TABLE02
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE02_INDEX));
  private static final TableName NAMESPACE02_TABLE03
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE03_INDEX));
   private static final TableName NAMESPACE03_TABLE01
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE01_INDEX));
  private static final TableName NAMESPACE03_TABLE02
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE02_INDEX));
  private static final TableName NAMESPACE03_TABLE03
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE03_INDEX));
  private static final byte[] CF01 = TEST_COLUMN_FAMILY_LIST.get(0);
  private static final byte[] CF02 = TEST_COLUMN_FAMILY_LIST.get(1);
  private static final byte[] COLQUALIFIER01 = TEST_COLUMN_QUALIFIER_LIST.get(0);
  private static final byte[] COLQUALIFIER02 = TEST_COLUMN_QUALIFIER_LIST.get(1);
  private static final byte[] COLQUALIFIER03 = TEST_COLUMN_QUALIFIER_LIST.get(2);
  private static final byte[] COLQUALIFIER04 = TEST_COLUMN_QUALIFIER_LIST.get(3);
  private static final byte[] COLQUALIFIER05 = TEST_COLUMN_QUALIFIER_LIST.get(4);
  private static final byte[] COLQUALIFIER06 = TEST_COLUMN_QUALIFIER_LIST.get(5);
  private static final byte[] COLQUALIFIER07 = TEST_COLUMN_QUALIFIER_LIST.get(6);
  private static final byte[] COLQUALIFIER08 = TEST_COLUMN_QUALIFIER_LIST.get(7);

  // static column values for testing CRUD methods
  private static final byte[] TABLE_SIMPLE_PUT
          = Bytes.toBytes("persisted with table#put(Put)");
  private static final byte[] TABLE_PUT_WITH_LIST
          = Bytes.toBytes("persisted with table#put(List<Put>)");
  private static final byte[] TABLE_PUT_WITH_CHECKPUT
          = Bytes.toBytes("persisted with table#checkAndPut");
  private static final byte[] TABLE_PUT_WITH_BATCH
          = Bytes.toBytes("persisted with table#batch(List<Row>)");
  private static final byte[] TABLE_PUT_WITH_BUFFERED_MUTATOR
          = Bytes.toBytes("persisted with bufferedMutator#mutate");
  private static final byte[] TABLE_PUT_WITH_BUFFERED_MUTATOR_LIST
          = Bytes.toBytes("persisted with bufferedMutator#mutate(List<>)");
  private static final byte[] TABLE_PUT_WITH_TABLE_MULTIPLEXER
          = Bytes.toBytes("persisted with HTableMultiplexer#mutate(List<>)");


  // non-static fields
  private Map<String, NamespaceDescriptor> testNamespacesAndDescriptors;
  private Map<TableName, HTableDescriptor> testTableNamesAndDescriptors;
  private Map<String, HColumnDescriptor> testColumnFamilyNamesAndDescriptors;

  @Test
  public void testMTableReadWrite() throws IOException, InterruptedException {
    System.out.println("#testMTableReadWrite has been INVOKED.");
    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();
    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();

    // STEP 1: Read/Write processing WITHOUT aliasing
    createSchemaStructuresInHBase(configuration, false); // create Tables with no aliasing
    persistColumnData(configuration);
    List<Result> resultListForCompleteScansWithoutAliases
            = getResultListUsingGetScannerForFullTableScan(configuration);
    List<Result> resultListForSpecificScansWithoutAliases
            = getResultListUsingGetScannerForSpecificScan(configuration);
    List<Result> resultListForFamilyScansWithoutAliases
            = getResultListUsingGetScannerForFamilyScan(configuration);
    List<Result> resultListForColumnScansWithoutAliases
            = getResultListUsingGetScannerForColumnScan(configuration);
    List<Result> resultListForIndividualGetsWithoutAliases
            = getResultListUsingIndividualGets(configuration);
    List<Result> resultListForListOfGetsWithoutAliases
            = getResultListUsingGetList(configuration);
    testExistsMethods(configuration);

    clearTestingEnvironment();

    // STEP 2: Read/Write processing WITH aliasing
    createSchemaStructuresInHBase(configuration, true); // create Tables WITH aliasing
    persistColumnData(configuration);
    List<Result> resultListForCompleteScansWithAliases
            = getResultListUsingGetScannerForFullTableScan(configuration);
    List<Result> resultListForSpecificScansWithAliases
            = getResultListUsingGetScannerForSpecificScan(configuration);
    List<Result> resultListForFamilyScansWithAliases
            = getResultListUsingGetScannerForFamilyScan(configuration);
    List<Result> resultListForColumnScansWithAliases
            = getResultListUsingGetScannerForColumnScan(configuration);
    List<Result> resultListForIndividualGetsWithAliases
            = getResultListUsingIndividualGets(configuration);
    List<Result> resultListForListOfGetsWithAliases
            = getResultListUsingGetList(configuration);
    testExistsMethods(configuration);

    // STEP 3: Compare Step 1 results with Step 2 results
    compareResultLists(
            "#getScanner with Full-table-Scan result lists (WITHOUT aliasing and WITH aliasing): ",
            resultListForCompleteScansWithoutAliases,
            resultListForCompleteScansWithAliases);
    compareResultLists(
            "#getScanner with Specific-Scan result lists (WITHOUT aliasing and WITH aliasing): ",
            resultListForSpecificScansWithoutAliases,
            resultListForSpecificScansWithAliases);
    compareResultLists(
            "#getScanner with Family-Scan result lists (WITHOUT aliasing and WITH aliasing): ",
            resultListForFamilyScansWithoutAliases,
            resultListForFamilyScansWithAliases);
    compareResultLists(
            "#getScanner with Column-Scan result lists (WITHOUT aliasing and WITH aliasing): ",
            resultListForColumnScansWithoutAliases,
            resultListForColumnScansWithAliases);
    compareResultLists(
            "result lists from individual #get invocations (WITHOUT aliasing and WITH aliasing): ",
            resultListForIndividualGetsWithoutAliases,
            resultListForIndividualGetsWithAliases);
    compareResultLists(
            "result lists from #get(List<Get>) invocations (WITHOUT aliasing and WITH aliasing): ",
            resultListForListOfGetsWithoutAliases,
            resultListForListOfGetsWithAliases);

    clearTestingEnvironment();
    System.out.println("#testMTableReadWrite has been COMPLETED.");
  }

  private void initializeTestNamespaceAndTableObjects() {

    testNamespacesAndDescriptors = new TreeMap<>();
    testTableNamesAndDescriptors = new TreeMap<>();
    testColumnFamilyNamesAndDescriptors = new TreeMap<>();

    for (String namespace : TEST_NAMESPACE_LIST) {
      testNamespacesAndDescriptors.put(namespace, NamespaceDescriptor.create(namespace).build());
      for (String tableNameString : TEST_TABLE_NAME_LIST) {
        TableName tableName = TableName.valueOf(namespace, tableNameString);
        testTableNamesAndDescriptors.put(tableName, new HTableDescriptor(tableName));
      }
    }
    for (byte[] columnFamily : TEST_COLUMN_FAMILY_LIST) {
      testColumnFamilyNamesAndDescriptors.put(
              Bytes.toString(columnFamily), new HColumnDescriptor(columnFamily));
    }
    int maxVersions = 8;
    for (HTableDescriptor htd : testTableNamesAndDescriptors.values()) {
      for (HColumnDescriptor hcd : testColumnFamilyNamesAndDescriptors.values()) {
        hcd.setMaxVersions(maxVersions);
        htd.addFamily(hcd);
      }
    }
  }

  private void clearTestingEnvironment() throws IOException {
    try (Admin standardAdmin = ConnectionFactory.createConnection().getAdmin()) {
      RepositoryAdmin.uninstallRepositoryStructures(standardAdmin);
      dropTestTablesAndNamespaces(standardAdmin);
    }
  }

  private void dropTestTablesAndNamespaces(Admin standardAdmin) throws IOException {
    // loop to disable and drop test tables and namespaces
    for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
      if (!standardAdmin.tableExists(tableName)) {
        continue;
      }
      if (!standardAdmin.isTableDisabled(tableName)) {
        standardAdmin.disableTable(tableName);
      }
      standardAdmin.deleteTable(tableName);
    }
    for (String namespaceName : testNamespacesAndDescriptors.keySet()) {
      if (namespaceName.isEmpty() || namespaceName.equals("default")) {
        continue;
      }
      try { standardAdmin.deleteNamespace(namespaceName);
      } catch (NamespaceNotFoundException e) {}
    }
  }

  private void createSchemaStructuresInHBase(Configuration configuration,
          boolean enableColumnAliases) throws IOException {

    try (Admin mAdmin = MConnectionFactory.createConnection(configuration).getAdmin()) {
      for (NamespaceDescriptor nd : testNamespacesAndDescriptors.values()) {
        if (nd.getName().isEmpty() || nd.getName().equals("default")) {
          continue;
        }
        mAdmin.createNamespace(nd);
      }
      for (HTableDescriptor htd : testTableNamesAndDescriptors.values()) {
        mAdmin.createTable(htd);
      }
      if (enableColumnAliases) {
        RepositoryAdmin repositoryAdmin = new RepositoryAdmin(mAdmin.getConnection());
        enableColumnAliases(repositoryAdmin, true, NAMESPACE01_TABLE01, CF01);
        enableColumnAliases(repositoryAdmin, true, NAMESPACE01_TABLE01, CF02);
        enableColumnAliases(repositoryAdmin, true, NAMESPACE01_TABLE02, CF01);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE01_TABLE02, CF02);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE01_TABLE03, CF01);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE01_TABLE03, CF02);

        enableColumnAliases(repositoryAdmin, true, NAMESPACE02_TABLE01, CF01);
        enableColumnAliases(repositoryAdmin, true, NAMESPACE02_TABLE01, CF02);
        enableColumnAliases(repositoryAdmin, true, NAMESPACE02_TABLE02, CF01);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE02_TABLE02, CF02);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE02_TABLE03, CF01);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE02_TABLE03, CF02);

        enableColumnAliases(repositoryAdmin, true, NAMESPACE03_TABLE01, CF01);
        enableColumnAliases(repositoryAdmin, true, NAMESPACE03_TABLE01, CF02);
        enableColumnAliases(repositoryAdmin, true, NAMESPACE03_TABLE02, CF01);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE03_TABLE02, CF02);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE03_TABLE03, CF01);
        enableColumnAliases(repositoryAdmin, false, NAMESPACE03_TABLE03, CF02);
      }
    }
  }

  private void enableColumnAliases(RepositoryAdmin repositoryAdmin, boolean enabled,
          TableName tableName, byte[] colFamily) throws IOException {
    try {
      repositoryAdmin.enableColumnAliases(enabled, tableName, colFamily);
    } catch (TableNotIncludedForProcessingException e) {}
  }

  private void persistColumnData(Configuration configuration)
          throws IOException, InterruptedException {
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        persistDataUsingTableMethods(connection, tableName);
        persistDataUsingBufferedMutatorMethods(connection, tableName);
      }
//      persistDataUsingHTableMultiplexerMethods(connection);
    }
  }

  private void persistDataUsingTableMethods(Connection connection, TableName tableName)
          throws IOException, InterruptedException {

    try (Table table = connection.getTable(tableName)) {
      // test Table#put(List<Put>)
      List<Put> putList = new LinkedList<>();
      putList.add(new Put(ROW_ID_01).
              addColumn(CF01, COLQUALIFIER01, TABLE_PUT_WITH_LIST).
              addColumn(CF01, COLQUALIFIER02, TABLE_PUT_WITH_LIST));
      putList.add(new Put(ROW_ID_02).
              addColumn(CF01, COLQUALIFIER01, TABLE_PUT_WITH_LIST).
              addColumn(CF01, COLQUALIFIER03, TABLE_PUT_WITH_LIST).
              addColumn(CF02, COLQUALIFIER04, TABLE_PUT_WITH_LIST));
      table.put(putList);
      // test Table#put(Put)
      table.put(new Put(ROW_ID_03).
              addColumn(CF01, COLQUALIFIER04, TABLE_SIMPLE_PUT).
              addColumn(CF02, COLQUALIFIER02, TABLE_SIMPLE_PUT));
      // test Table#checkAndPut
      assertTrue("Table#checkAndPut unexpectedly failed",
        table.checkAndPut(ROW_ID_03, CF01, COLQUALIFIER04, TABLE_SIMPLE_PUT,
                new Put(ROW_ID_03).addColumn(CF01, COLQUALIFIER03, TABLE_PUT_WITH_CHECKPUT)));
      assertTrue("Table#checkAndPut unexpectedly failed",
        table.checkAndPut(ROW_ID_03, CF01, COLQUALIFIER03, CompareOp.NOT_EQUAL, TABLE_SIMPLE_PUT,
                new Put(ROW_ID_03).addColumn(CF01, COLQUALIFIER05, TABLE_PUT_WITH_CHECKPUT)));
      // test Table#append
      // append both to an existing column, AND create a new column
      table.append(new Append(ROW_ID_02)
              .add(CF01, COLQUALIFIER03, Bytes.toBytes("appendedString"))
              .add(CF01, COLQUALIFIER02, Bytes.toBytes("newColumnViaAppend")));
      // use append to create new row with newly-used colQualifier
      table.append(new Append(ROW_ID_04)
              .add(CF01, COLQUALIFIER06, Bytes.toBytes("newColumnViaAppend")));
      // test Table#increment
      Result incrementResult
              = table.increment(new Increment(ROW_ID_02).addColumn(CF01, COLQUALIFIER05, 3));
      assertEquals("Incremented value incorrect.",
              3, Bytes.toLong(incrementResult.getValue(CF01, COLQUALIFIER05)));
      pauseBetweenIncrements();
      incrementResult
              = table.increment(new Increment(ROW_ID_02).addColumn(CF01, COLQUALIFIER05, 8));
      assertEquals("Incremented value incorrect.",
              11, Bytes.toLong(incrementResult.getValue(CF01, COLQUALIFIER05)));
      pauseBetweenIncrements();
      // test Table#incrementColumnValue
      assertEquals("Incremented value incorrect.",
              14, table.incrementColumnValue(ROW_ID_02, CF01, COLQUALIFIER05, 3));
      pauseBetweenIncrements();
      // test Table#batch with Append, Delete, Increment, Put, and Get
      testBatchProcessing(table);
      // test Table#delete
      table.delete(new Delete(ROW_ID_01).addColumn(CF01, COLQUALIFIER02));
      // test Table#delete(List)
      List<Delete> deleteList = new LinkedList<>();
      deleteList.add(new Delete(ROW_ID_02).addColumn(CF01, COLQUALIFIER02));
      deleteList.add(new Delete(ROW_ID_03).addColumn(CF01, COLQUALIFIER05));
      table.delete(deleteList);
      // test Table#checkAndDelete
      table.put(new Put(ROW_ID_02).addColumn(CF01, COLQUALIFIER02, TABLE_SIMPLE_PUT));
      assertTrue("Table#checkAndDelete unexpectedly failed",
        table.checkAndDelete(ROW_ID_02, CF01, COLQUALIFIER01, TABLE_PUT_WITH_LIST,
                new Delete(ROW_ID_02).addColumn(CF01, COLQUALIFIER02)));
      table.put(new Put(ROW_ID_02).addColumn(CF01, COLQUALIFIER02, TABLE_SIMPLE_PUT));
      assertTrue("Table#checkAndDelete unexpectedly failed",
        table.checkAndDelete(ROW_ID_02, CF01, COLQUALIFIER01, CompareOp.NOT_EQUAL, TABLE_SIMPLE_PUT,
                new Delete(ROW_ID_02).addColumn(CF01, COLQUALIFIER02)));
      // test Table#mutateRow
      RowMutations rowMutations = new RowMutations(ROW_ID_02);
      rowMutations.add(new Put(ROW_ID_02).addColumn(CF01, COLQUALIFIER02, TABLE_SIMPLE_PUT));
      rowMutations.add(new Delete(ROW_ID_02).addColumn(CF01, COLQUALIFIER05));
      table.mutateRow(rowMutations);
      // test Table#checkAndMutate
      rowMutations = new RowMutations(ROW_ID_02);
      rowMutations.add(new Delete(ROW_ID_02).addColumn(CF01, COLQUALIFIER02));
      rowMutations.add(new Put(ROW_ID_02).addColumn(CF02, COLQUALIFIER06, TABLE_SIMPLE_PUT));
      assertTrue("Table#checkAndMutate unexpectedly failed",
        table.checkAndMutate(ROW_ID_02, CF01, COLQUALIFIER01, CompareOp.NOT_EQUAL, TABLE_SIMPLE_PUT,
                rowMutations));
    }
  }

  private void persistDataUsingBufferedMutatorMethods(Connection connection, TableName tableName)
          throws IOException {
    try (BufferedMutator bufferedMutator = connection.getBufferedMutator(tableName);
            Table table = connection.getTable(tableName)) {
      // do standard Puts for subsequent Delete in BufferedMutator
      List<Put> putList = new LinkedList<>();
      putList.add(new Put(ROW_ID_02).
              addColumn(CF01, COLQUALIFIER08, TABLE_PUT_WITH_LIST).
              addColumn(CF02, COLQUALIFIER07, TABLE_PUT_WITH_LIST));
      table.put(putList);

      // test BufferMutator individual mutations (put and delete)
      bufferedMutator.mutate(
              new Put(ROW_ID_03).addColumn(CF02, COLQUALIFIER07, TABLE_PUT_WITH_BUFFERED_MUTATOR));
      bufferedMutator.mutate(
              new Delete(ROW_ID_02).addColumn(CF01, COLQUALIFIER08));
      bufferedMutator.flush();

      // test BufferMutator with List of mutations (put and delete)
      List<Mutation> mutationList = new LinkedList<>();
      mutationList.add(new Put(ROW_ID_04).addColumn(
              CF01, COLQUALIFIER07, TABLE_PUT_WITH_BUFFERED_MUTATOR_LIST));
      mutationList.add(new Delete(ROW_ID_02).addColumn(CF02, COLQUALIFIER07));
      bufferedMutator.mutate(mutationList);
      bufferedMutator.flush();
    }
  }

//  private void persistDataUsingHTableMultiplexerMethods(Connection connection)
//          throws IOException {
//    HTableMultiplexer htm = new RepositoryAdmin(connection).createHTableMultiplexer(20);
//    htm.put(NAMESPACE01_TABLE01, new Put(ROW_ID_05).addColumn(
//              CF01, COLQUALIFIER08, TABLE_PUT_WITH_TABLE_MULTIPLEXER));
////    htm.put(NAMESPACE01_TABLE02, new Put(ROW_ID_02).addColumn(
////              CF02, COLQUALIFIER07, TABLE_PUT_WITH_TABLE_MULTIPLEXER));
//  }


  private void testExistsMethods(Configuration configuration) throws IOException {
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      // test Table#exists(Get)
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        try (Table table = connection.getTable(tableName)) {
          for (byte[] rowId : TEST_ROW_ID_LIST) {
            existsAssertion(table, new Get(rowId)
                    .setId("Get for table: " + table.getName().getNameAsString()
                            + " rowId: " + Bytes.toString(rowId)));
            for (byte[] colFamily : TEST_COLUMN_FAMILY_LIST) {
              existsAssertion(table, new Get(rowId).addFamily(colFamily)
                      .setId("Get for table: " + table.getName().getNameAsString()
                              + " rowId: " + Bytes.toString(rowId)
                              + " colFamily: " + Bytes.toString(colFamily)));
              for (byte[] colQualifier : TEST_COLUMN_QUALIFIER_LIST) {
                existsAssertion(table, new Get(rowId).addColumn(colFamily, colQualifier)
                        .setId("Get for table: " + table.getName().getNameAsString()
                                + " rowId: " + Bytes.toString(rowId)
                                + " colFamily: " + Bytes.toString(colFamily)
                                + " colQualifier: " + Bytes.toString(colQualifier)));
              }
            }
          }
        }
      }
      // test Table#existsAll(List<Get>)
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        try (Table table = connection.getTable(tableName)) {
          List<Get> listOfGets = new LinkedList<>();
          for (byte[] rowId : TEST_ROW_ID_LIST) {
            listOfGets.add(new Get(rowId)
                    .setId("Get for table: " + table.getName().getNameAsString()
                            + " rowId: " + Bytes.toString(rowId)));
            for (byte[] colFamily : TEST_COLUMN_FAMILY_LIST) {
              listOfGets.add(new Get(rowId).addFamily(colFamily)
                      .setId("Get for table: " + table.getName().getNameAsString()
                              + " rowId: " + Bytes.toString(rowId)
                              + " colFamily: " + Bytes.toString(colFamily)));
              for (byte[] colQualifier : TEST_COLUMN_QUALIFIER_LIST) {
                listOfGets.add(new Get(rowId).addColumn(colFamily, colQualifier)
                        .setId("Get for table: " + table.getName().getNameAsString()
                                + " rowId: " + Bytes.toString(rowId)
                                + " colFamily: " + Bytes.toString(colFamily)
                                + " colQualifier: " + Bytes.toString(colQualifier)));
              }
            }
          }
          boolean[] resultExists = table.existsAll(listOfGets);
          Iterator<Get> iteratorForGets = listOfGets.iterator();
          int index = 0;
          while (iteratorForGets.hasNext()) {
            Get get = iteratorForGets.next();
            assertEquals("Inconsistent results from Table#get and Table#existsAll "
                    + "when using Get with getId == <" + get.getId() + ">;",
                    !table.get(get).isEmpty(), resultExists[index++]);
          }
        }
      }
    }
  }

  private void existsAssertion(Table table, Get get) throws IOException {
    assertEquals("Inconsistent results from Table#get and Table#exists "
                    + "when using Get with getId == <" + get.getId() + ">;",
            !table.get(get).isEmpty(), table.exists(get));
   }

  private void testBatchProcessing(Table table) throws IOException, InterruptedException {
    List<Row> actions = new LinkedList<>();
    actions.add(new Append(ROW_ID_02)
            .add(CF01, COLQUALIFIER03, Bytes.toBytes("appendedStringViaBatch")));
    actions.add(new Delete(ROW_ID_03).addColumn(CF01, COLQUALIFIER04));
    actions.add(new Increment(ROW_ID_02).addColumn(CF01, COLQUALIFIER05, 14));
    actions.add(new Put(ROW_ID_05).
            addColumn(CF01, COLQUALIFIER04, TABLE_PUT_WITH_LIST).
            addColumn(CF02, COLQUALIFIER02, TABLE_PUT_WITH_LIST));
    actions.add(new Get(ROW_ID_01).addColumn(CF01, COLQUALIFIER02));
    Object[] returnedObjects = new Object[actions.size()];
    table.batch(actions, returnedObjects);
    int index = 0;
    for (Object returnedObject : returnedObjects) {
      assertTrue("Table#batch action failed for " + actions.get(index).getClass().getSimpleName(),
              returnedObject != null);
      if (Get.class.isAssignableFrom(actions.get(index).getClass())) {
        Result resultFromGet = (Result)returnedObject;
        assertTrue("Table#batch Get action returned unexpected Result: expected <"
                + Bytes.toString(TABLE_PUT_WITH_LIST) + ">, returned <"
                + Bytes.toString(resultFromGet.getValue(CF01, COLQUALIFIER02)) + ">",
                Bytes.equals(TABLE_PUT_WITH_LIST, resultFromGet.getValue(CF01, COLQUALIFIER02)));
      }
      index++;
    }
  }

  private List<Result> getResultListUsingGetScannerForFullTableScan(Configuration configuration)
          throws IOException {
    List<Result> resultList = new LinkedList<>();
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        Iterator<Result> resultIterator = getScanner(connection, tableName, new Scan()).iterator();
        while (resultIterator.hasNext()) {
          resultList.add(resultIterator.next());
        }
      }
    }
    return resultList;
  }

  private List<Result> getResultListUsingGetScannerForSpecificScan(Configuration configuration)
          throws IOException {
    List<Result> resultList = new LinkedList<>();
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        Scan specificScan = new Scan()
                .addColumn(CF01, COLQUALIFIER01).addColumn(CF01, COLQUALIFIER02)
                .addColumn(CF01, COLQUALIFIER03).addColumn(CF01, COLQUALIFIER04)
                .addColumn(CF01, COLQUALIFIER05).addColumn(CF01, COLQUALIFIER06)
                .addColumn(CF02, COLQUALIFIER01).addColumn(CF02, COLQUALIFIER02)
                .addColumn(CF02, COLQUALIFIER03).addColumn(CF02, COLQUALIFIER04)
                .addColumn(CF02, COLQUALIFIER05).addColumn(CF02, COLQUALIFIER06);
        try (ResultScanner resultScanner = getScanner(connection, tableName, specificScan)) {
          Iterator<Result> resultIterator = resultScanner.iterator();
          while (resultIterator.hasNext()) {
            resultList.add(resultIterator.next());
          }
        }
      }
    }
    return resultList;
  }

  private ResultScanner getScanner(Connection connection, TableName tableName, Scan scanObject)
          throws IOException {
    try (Table table = connection.getTable(tableName)) {
      return table.getScanner(scanObject);
    }
  }

  private List<Result> getResultListUsingGetScannerForFamilyScan(Configuration configuration)
          throws IOException {
    List<Result> resultList = new LinkedList<>();
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        try (ResultScanner resultScanner = getScannerForFamily(connection, tableName)) {
          Iterator<Result> resultIterator = resultScanner.iterator();
          while (resultIterator.hasNext()) {
            resultList.add(resultIterator.next());
          }
        }
      }
    }
    return resultList;
  }

  private ResultScanner getScannerForFamily(Connection connection, TableName tableName)
          throws IOException {
    try (Table table = connection.getTable(tableName)) {
      return table.getScanner(CF01);
    }
  }

  private List<Result> getResultListUsingGetScannerForColumnScan(Configuration configuration)
          throws IOException {
    List<Result> resultList = new LinkedList<>();
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        try (ResultScanner resultScanner = getScannerForColumn(connection, tableName)) {
          Iterator<Result> resultIterator = resultScanner.iterator();
          while (resultIterator.hasNext()) {
            resultList.add(resultIterator.next());
          }
        }
      }
    }
    return resultList;
  }

  private ResultScanner getScannerForColumn(Connection connection, TableName tableName)
          throws IOException {
    try (Table table = connection.getTable(tableName)) {
      return table.getScanner(CF01, COLQUALIFIER01);
    }
  }

  private List<Result> getResultListUsingIndividualGets(Configuration configuration)
          throws IOException {
    List<Result> resultList = new LinkedList<>();
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        try (Table table = connection.getTable(tableName)) {
          for (byte[] rowId : TEST_ROW_ID_LIST) {
            resultList.add(table.get(new Get(rowId)));
            for (byte[] colFamily : TEST_COLUMN_FAMILY_LIST) {
              resultList.add(table.get(new Get(rowId).addFamily(colFamily)));
              for (byte[] colQualifier : TEST_COLUMN_QUALIFIER_LIST) {
                resultList.add(table.get(new Get(rowId).addColumn(colFamily, colQualifier)));
              }
            }
          }
        }
      }
    }
    return resultList;
  }

  private List<Result> getResultListUsingGetList(Configuration configuration)
          throws IOException {
    List<Result> resultList = new LinkedList<>();
    try (Connection connection = MConnectionFactory.createConnection(configuration)) {
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        List<Get> listOfGets = new LinkedList<>();
        for (byte[] rowId : TEST_ROW_ID_LIST) {
          listOfGets.add(new Get(rowId));
          for (byte[] colFamily : TEST_COLUMN_FAMILY_LIST) {
            listOfGets.add(new Get(rowId).addFamily(colFamily));
            for (byte[] colQualifier : TEST_COLUMN_QUALIFIER_LIST) {
              listOfGets.add(new Get(rowId).addColumn(colFamily, colQualifier));
            }
          }
        }
        try (Table table = connection.getTable(tableName)) {
          for (Result result : table.get(listOfGets)) {
            resultList.add(result);
          }
          // resultList.addAll(Arrays.asList(table.get(listOfGets)));
        }
      }
    }
    return resultList;
  }

  private void compareResultLists(String assertFailureMsg,
          List<Result> resultListWithoutAliases, List<Result> resultListWithAliases) {
    assertEquals(assertFailureMsg + "List SIZES unequal.",
            resultListWithoutAliases.size(),
            resultListWithAliases.size());
    Iterator<Result> resultIteratorWithoutAliases = resultListWithoutAliases.iterator();
    Iterator<Result> resultIteratorWithAliases = resultListWithAliases.iterator();
    while (resultIteratorWithoutAliases.hasNext()) {
      Result resultWithoutAliases = resultIteratorWithoutAliases.next();
      Result resultWithAliases = resultIteratorWithAliases.next();
      assertEquals(assertFailureMsg + "Result #isEmpty values are unequal.",
              resultWithoutAliases.isEmpty(), resultWithAliases.isEmpty());
      if (resultWithoutAliases.isEmpty()) {
        continue;
      }
      Set<Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>>> familyEntrySetWithoutAliases
              = resultWithoutAliases.getMap().entrySet();
      Set<Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>>> familyEntrySetWithAliases
              = resultWithAliases.getMap().entrySet();
      assertEquals(assertFailureMsg + "Result Family-Entry Set SIZES unequal.",
              familyEntrySetWithoutAliases.size(), familyEntrySetWithAliases.size());
      Iterator<Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>>>
              familyEntryIteratorWithoutAliases = familyEntrySetWithoutAliases.iterator();
      Iterator<Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>>>
              familyEntryIteratorWithAliases = familyEntrySetWithAliases.iterator();
      while (familyEntryIteratorWithoutAliases.hasNext()) {
        Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> familyEntryWithoutAliases
                = familyEntryIteratorWithoutAliases.next();
        Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> familyEntryWithAliases
                = familyEntryIteratorWithAliases.next();
        assertTrue(assertFailureMsg + "Result Entry COLUMN FAMILIES unequal: <"
                + Bytes.toString(familyEntryWithoutAliases.getKey()) +  "> NOT EQUAL TO <"
                + Bytes.toString(familyEntryWithAliases.getKey()),
                Bytes.equals(familyEntryWithoutAliases.getKey(), familyEntryWithAliases.getKey()));
        Set<Entry<byte[],NavigableMap<Long,byte[]>>> columnEntrySetWithoutAliases
                = familyEntryWithoutAliases.getValue().entrySet();
        Set<Entry<byte[],NavigableMap<Long,byte[]>>> columnEntrySetWithAliases
                = familyEntryWithAliases.getValue().entrySet();
        assertEquals(assertFailureMsg + "Result Column-Entry Set SIZES unequal.",
                columnEntrySetWithoutAliases.size(), columnEntrySetWithAliases.size());
        Iterator<Entry<byte[],NavigableMap<Long,byte[]>>>
                columnEntryIteratorWithoutAliases = columnEntrySetWithoutAliases.iterator();
        Iterator<Entry<byte[],NavigableMap<Long,byte[]>>>
                columnEntryIteratorWithAliases = columnEntrySetWithAliases.iterator();
        while (columnEntryIteratorWithoutAliases.hasNext()) {
          Entry<byte[],NavigableMap<Long,byte[]>> columnEntryWithoutAliases
                  = columnEntryIteratorWithoutAliases.next();
          Entry<byte[],NavigableMap<Long,byte[]>> columnEntryWithAliases
                  = columnEntryIteratorWithAliases.next();
          assertTrue(assertFailureMsg + "Result Entry COLUMN QUALIFIERS unequal: <"
                  + Repository.getPrintableString(columnEntryWithoutAliases.getKey())
                  +  "> NOT EQUAL TO <"
                  + Repository.getPrintableString(columnEntryWithAliases.getKey()) + ">",
                  Bytes.equals(columnEntryWithoutAliases.getKey(),
                          columnEntryWithAliases.getKey()));
          Set<Entry<Long,byte[]>> cellEntrySetWithoutAliases
                  = columnEntryWithoutAliases.getValue().entrySet();
          Set<Entry<Long,byte[]>> cellEntrySetWithAliases
                  = columnEntryWithAliases.getValue().entrySet();
          assertEquals(assertFailureMsg + "Result Cell-Entry Set SIZES unequal.",
                  cellEntrySetWithoutAliases.size(), cellEntrySetWithAliases.size());
          Iterator<Entry<Long,byte[]>>
                  cellEntryIteratorWithoutAliases = cellEntrySetWithoutAliases.iterator();
          Iterator<Entry<Long,byte[]>>
                  cellEntryIteratorWithAliases = cellEntrySetWithAliases.iterator();
          while (cellEntryIteratorWithoutAliases.hasNext()) {
            byte[] cellValueWithoutAliases = cellEntryIteratorWithoutAliases.next().getValue();
            byte[] cellValueWithAliases = cellEntryIteratorWithAliases.next().getValue();
            assertTrue(assertFailureMsg + "ColumnFamily <"
                    + Bytes.toString(familyEntryWithoutAliases.getKey())
                    + "> Qualifier <" + Bytes.toString(columnEntryWithoutAliases.getKey())
                    + "> -- Result Cell-Entry VALUES unequal: <"
                    + Repository.getPrintableString(cellValueWithoutAliases)
                    +  "> NOT EQUAL TO <"
                    + Repository.getPrintableString(cellValueWithAliases)
                    + ">",
                    Bytes.equals(cellValueWithoutAliases, cellValueWithAliases));
            // NOTE: cell timestamps will ALWAYS be unequal -- do NOT compare them!
          }
        }
      }
    }
  }

  /**
   * Need to pause slightly between Increment submissions; otherwise, risk having increment-column
   * cells with IDENTICAL timestamps, which screws up the validation logic of these tests, since
   * identical cell-timestamps can inconsistently alter the order of cells in the familyMap of a
   * Result.
   */
  private void pauseBetweenIncrements() {
    try {
      Thread.sleep(5);  // 5 milliseconds
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    new TestColumnAliasing().testMTableReadWrite();
  }
}
