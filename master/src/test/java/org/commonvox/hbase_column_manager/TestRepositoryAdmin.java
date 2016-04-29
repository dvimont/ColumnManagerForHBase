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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
public class TestRepositoryAdmin {

  private static final List<String> TEST_NAMESPACE_LIST
          = new ArrayList<>(Arrays.asList("testNamespace01", "testNamespace02", "testNamespace03"));
  private static final List<String> TEST_TABLE_NAME_LIST
          = new ArrayList<>(
                  Arrays.asList("testTable01", "testTable02", "testTable03", "testTable04"));
  private static final List<byte[]> TEST_COLUMN_FAMILY_LIST
          = new ArrayList<>(Arrays.asList(Bytes.toBytes("CF1"), Bytes.toBytes("CF2")));
  private static final List<byte[]> TEST_COLUMN_QUALIFIER_LIST
          = new ArrayList<>(Arrays.asList(Bytes.toBytes("column01"), Bytes.toBytes("column02"),
                          Bytes.toBytes("column03"), Bytes.toBytes("column04")));
  private static final byte[] QUALIFIER_IN_EXCLUDED_TABLE = Bytes.toBytes("qualifierOnExcludedTable");
  private static final byte[] ROW_ID_01 = Bytes.toBytes("rowId01");
  private static final byte[] ROW_ID_02 = Bytes.toBytes("rowId02");
  private static final byte[] ROW_ID_03 = Bytes.toBytes("rowId03");
  private static final byte[] ROW_ID_04 = Bytes.toBytes("rowId04");
  private static final byte[] VALUE_2_BYTES_LONG = Bytes.toBytes("xy");
  private static final byte[] VALUE_5_BYTES_LONG = Bytes.toBytes("54321");
  private static final byte[] VALUE_9_BYTES_LONG = Bytes.toBytes("123456789");
  private static final byte[] VALUE_82_BYTES_LONG = new byte[82];
  static {
    Arrays.fill(VALUE_82_BYTES_LONG, (byte) 'A');
  }
  private static final int NAMESPACE01_INDEX = 0;
  // namespace02 is NOT included in audit processing
  private static final int NAMESPACE02_INDEX = 1;
  // namespace03's table02 & table04 NOT included in audit processing
  private static final int NAMESPACE03_INDEX = 2;
  private static final int TABLE01_INDEX = 0;
  private static final int TABLE02_INDEX = 1;
  private static final int TABLE03_INDEX = 2;
  private static final int TABLE04_INDEX = 3;
  private static final int CF01_INDEX = 0;
  private static final int CF02_INDEX = 1;

  private static Set<byte[]> expectedColQualifiersForNamespace1Table1Cf1
            = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
  static {
    expectedColQualifiersForNamespace1Table1Cf1.add(TEST_COLUMN_QUALIFIER_LIST.get(0));
    expectedColQualifiersForNamespace1Table1Cf1.add(TEST_COLUMN_QUALIFIER_LIST.get(1));
    expectedColQualifiersForNamespace1Table1Cf1.add(TEST_COLUMN_QUALIFIER_LIST.get(2));
  }
  private static Set<byte[]> expectedColQualifiersForNamespace1Table1Cf2
            = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
  static {
    expectedColQualifiersForNamespace1Table1Cf2.add(TEST_COLUMN_QUALIFIER_LIST.get(3));
  }
  private static Set<ColumnAuditor> expectedColAuditorsForNamespace1Table1Cf1 = new TreeSet<>();
  static {
    expectedColAuditorsForNamespace1Table1Cf1.add(
            new ColumnAuditor(TEST_COLUMN_QUALIFIER_LIST.get(0)).setMaxValueLengthFound(82));
    expectedColAuditorsForNamespace1Table1Cf1.add(
            new ColumnAuditor(TEST_COLUMN_QUALIFIER_LIST.get(1)).setMaxValueLengthFound(5));
    expectedColAuditorsForNamespace1Table1Cf1.add(
            new ColumnAuditor(TEST_COLUMN_QUALIFIER_LIST.get(2)).setMaxValueLengthFound(9));
  }
  private static Set<ColumnAuditor> expectedColAuditorsForNamespace1Table1Cf2 = new TreeSet<>();
  static {
    expectedColAuditorsForNamespace1Table1Cf2.add(
            new ColumnAuditor(TEST_COLUMN_QUALIFIER_LIST.get(3)).setMaxValueLengthFound(82));
  }
  private static final String ALTERNATE_USERNAME = "testAlternateUserName";

  private static final String REPOSITORY_ADMIN_FAILURE
          = "FAILURE IN " + RepositoryAdmin.class.getSimpleName() + " PROCESSING!! ==>> ";
  private static final String COLUMN_AUDIT_FAILURE = "FAILURE IN Column Audit PROCESSING!! ==>> ";
  private static final String GET_COL_QUALIFIERS_FAILURE
          = COLUMN_AUDIT_FAILURE + "#getColumnQualifiers method returned unexpected results";
  private static final String GET_COL_AUDITORS_FAILURE
          = COLUMN_AUDIT_FAILURE + "#getColumnAuditors method returned unexpected results";
  // non-static fields
  private Map<String, NamespaceDescriptor> testNamespacesAndDescriptors;
  private Map<TableName, HTableDescriptor> testTableNamesAndDescriptors;
  private Map<String, HColumnDescriptor> testColumnFamilyNamesAndDescriptors;

  @Test
  public void testStaticMethods() throws IOException {
    System.out.println("#testStaticMethods has been invoked.");
    try (Connection standardConnection = ConnectionFactory.createConnection();
            Admin standardAdmin = standardConnection.getAdmin()) {

      // do "manual" cleanup to prepare for unit test
      TestMConnectionFactory.manuallyDropRepositoryStructures(standardConnection, standardAdmin);

      RepositoryAdmin.installRepositoryStructures(standardAdmin);
      assertTrue(REPOSITORY_ADMIN_FAILURE
              + "The following Repository NAMESPACE failed to be created upon "
              + "invocation of #installRepositoryStructures method: "
              + Repository.REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
              TestMConnectionFactory.namespaceExists(
                      standardAdmin, Repository.REPOSITORY_NAMESPACE_DESCRIPTOR));
      assertTrue(REPOSITORY_ADMIN_FAILURE
              + "The following Repository TABLE failed to be created upon "
              + "invocation of #installRepositoryStructures method: "
              + Repository.REPOSITORY_TABLENAME.getNameAsString(),
              standardAdmin.tableExists(Repository.REPOSITORY_TABLENAME));

      assertEquals(REPOSITORY_ADMIN_FAILURE
              + "Incorrect default value for Repository maxVersions returned by "
              + "#getRepositoryMaxVersions method.",
              Repository.REPOSITORY_DEFAULT_MAX_VERSIONS,
              RepositoryAdmin.getRepositoryMaxVersions(standardAdmin));

      final int NEW_MAX_VERSIONS = 160;
      RepositoryAdmin.setRepositoryMaxVersions(standardAdmin, NEW_MAX_VERSIONS);
      assertEquals(REPOSITORY_ADMIN_FAILURE
              + "Incorrect value for Repository maxVersions returned by "
              + "#getRepositoryMaxVersions method following invocation of "
              + "#setRepositoryMaxVersions method.",
              NEW_MAX_VERSIONS,
              RepositoryAdmin.getRepositoryMaxVersions(standardAdmin));

      RepositoryAdmin.uninstallRepositoryStructures(standardAdmin);
      assertTrue(REPOSITORY_ADMIN_FAILURE
              + "The following Repository NAMESPACE failed to be dropped upon "
              + "invocation of #uninstallRepositoryStructures method: "
              + Repository.REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
              !TestMConnectionFactory.namespaceExists(
                      standardAdmin, Repository.REPOSITORY_NAMESPACE_DESCRIPTOR));
      assertTrue(REPOSITORY_ADMIN_FAILURE
              + "The following Repository TABLE failed to be dropped upon "
              + "invocation of #uninstallRepositoryStructures method: "
              + Repository.REPOSITORY_TABLENAME.getNameAsString(),
              !standardAdmin.tableExists(Repository.REPOSITORY_TABLENAME));
    }
    System.out.println("#testStaticMethods has run to completion.");
  }

  @Test
  public void testColumnAuditingWithExplicitExcludes() throws IOException {
    System.out.println("#testColumnAuditing has been invoked using EXCLUDE config properties.");

    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    Configuration configuration = MConfiguration.create();
    testColumnAuditing(configuration);
    System.out.println("#testColumnAuditing using EXCLUDE config properties has run to completion.");
  }

  @Test
  public void testColumnAuditingWithExplicitIncludes() throws IOException {
    System.out.println("#testColumnAuditing has been invoked using EXPLICIT "
            + "INCLUDE config properties.");

    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);

    // NOTE that the "include" settings added here are the inverse of the "exclude" settings
    //  in the hbase-column-manager.xml file in the test/resources directory. They should
    //  result in EXACTLY the same results in ColumnManager auditing.
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES,
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)).getNameAsString(),
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE03_INDEX)).getNameAsString(),
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)).getNameAsString(),
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE02_INDEX)).getNameAsString(),
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE03_INDEX)).getNameAsString(),
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE04_INDEX)).getNameAsString()
    );

    testColumnAuditing(configuration);
    System.out.println("#testColumnAuditing using EXPLICIT INCLUDE config properties has "
            + "run to completion.");
  }

  @Test
  public void testColumnAuditingWithWildcardedIncludes() throws IOException {
    System.out.println("#testColumnAuditing has been invoked using WILDCARDED "
            + "INCLUDE config properties.");

    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);

    // NOTE that the "include" settings added here are the inverse of the "exclude" settings
    //  in the hbase-column-manager.xml file in the test/resources directory. They should
    //  result in EXACTLY the same results in ColumnManager auditing.
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES,
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)).getNameAsString(),
            TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                    TEST_TABLE_NAME_LIST.get(TABLE03_INDEX)).getNameAsString(),
            TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX) + ":*"  // include all namespace01 tables!!
    );

    testColumnAuditing(configuration);
    System.out.println("#testColumnAuditing using WILDCARDED INCLUDE config properties has "
            + "run to completion.");
  }

  private void testColumnAuditing(Configuration configuration) throws IOException {
//    System.out.println("#testColumnAuditing has been invoked using "
//            + (useAlternateConfigProperties ? "INCLUDE" : "EXCLUDE") + " config properties.");
//
//    initializeTestNamespaceAndTableObjects();
//    clearTestingEnvironment();
//
//    Configuration configuration;
//    if (useAlternateConfigProperties) {
//      configuration = HBaseConfiguration.create();
//      configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
//      configuration.set(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_MODE,
//              Repository.HBASE_CONFIG_PARM_VALUE_COLMANAGER_MODE_ACTIVE);
//      // NOTE that the "include" settings added here are the inverse of the "exclude" settings
//      //  in the hbase-column-manager.xml file in the test/resources directory. They should
//      //  result in EXACTLY the same results in ColumnManager auditing.
////      configuration.set(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_NAMESPACES,
////              TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX));
//      configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES,
//              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
//                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)).getNameAsString(),
//              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
//                                      TEST_TABLE_NAME_LIST.get(TABLE03_INDEX)).getNameAsString(),
//              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
//                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)).getNameAsString(),
//              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
//                                      TEST_TABLE_NAME_LIST.get(TABLE02_INDEX)).getNameAsString(),
//              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
//                                      TEST_TABLE_NAME_LIST.get(TABLE03_INDEX)).getNameAsString(),
//              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
//                                      TEST_TABLE_NAME_LIST.get(TABLE04_INDEX)).getNameAsString()
//      );
//
//    } else {
//      configuration = MConfiguration.create();
//    }

    createSchemaStructuresInHBase(configuration);
    loadColumnData(configuration);

    try (Connection mConnection = MConnectionFactory.createConnection(configuration);
            RepositoryAdmin repositoryAdmin = new RepositoryAdmin(mConnection)) {

      // Test #getColumnQualifiers
      Set<byte[]> returnedColQualifiersForNamespace1Table1Cf1
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX))));
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf1.equals(
                      returnedColQualifiersForNamespace1Table1Cf1));

      Set<byte[]> returnedColQualifiersForNamespace1Table1Cf2
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF02_INDEX))));
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf2.equals(
                      returnedColQualifiersForNamespace1Table1Cf2));

      Set<byte[]> returnedColQualifiersForNamespace2Table1Cf1
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX))));
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace2Table1Cf1 == null);

      Set<byte[]> returnedColQualifiersForNamespace3Table2Cf1
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE02_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX))));
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace3Table2Cf1 == null);

      // Test #getColumnQualifiers with alternate signature
      returnedColQualifiersForNamespace1Table1Cf1
              = repositoryAdmin.getColumnQualifiers(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX));
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf1.equals(
                      returnedColQualifiersForNamespace1Table1Cf1));

      returnedColQualifiersForNamespace1Table1Cf2
              = repositoryAdmin.getColumnQualifiers(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF02_INDEX));
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf2.equals(
                      returnedColQualifiersForNamespace1Table1Cf2));

      returnedColQualifiersForNamespace2Table1Cf1
              = repositoryAdmin.getColumnQualifiers(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX));
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace2Table1Cf1 == null);

      returnedColQualifiersForNamespace3Table2Cf1
              = repositoryAdmin.getColumnQualifiers(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE02_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX));
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace3Table2Cf1 == null);

      // Test #getColumnAuditors
      Set<ColumnAuditor> returnedColAuditorsForNamespace1Table1Cf1
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX))));
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf1.equals(
                      returnedColAuditorsForNamespace1Table1Cf1));

      Set<ColumnAuditor> returnedColAuditorsForNamespace1Table1Cf2
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF02_INDEX))));
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf2.equals(
                      returnedColAuditorsForNamespace1Table1Cf2));

      Set<ColumnAuditor> returnedColAuditorsForNamespace2Table1Cf1
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX))));
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace2Table1Cf1 == null);

      Set<ColumnAuditor> returnedColAuditorsForNamespace3Table2Cf1
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(
                              TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE02_INDEX))),
                      testColumnFamilyNamesAndDescriptors.get(
                              Bytes.toString(TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX))));
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace3Table2Cf1 == null);

      // Test #getColumnAuditors with alternate signature
      returnedColAuditorsForNamespace1Table1Cf1
              = repositoryAdmin.getColumnAuditors(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX));
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf1.equals(
                      returnedColAuditorsForNamespace1Table1Cf1));

      returnedColAuditorsForNamespace1Table1Cf2
              = repositoryAdmin.getColumnAuditors(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF02_INDEX));
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf2.equals(
                      returnedColAuditorsForNamespace1Table1Cf2));

      returnedColAuditorsForNamespace2Table1Cf1
              = repositoryAdmin.getColumnAuditors(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX));
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace2Table1Cf1 == null);

      returnedColAuditorsForNamespace3Table2Cf1
              = repositoryAdmin.getColumnAuditors(
                      TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                                      TEST_TABLE_NAME_LIST.get(TABLE02_INDEX)),
                      TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX));
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace3Table2Cf1 == null);
    }

    clearTestingEnvironment();

  }

  private void clearTestingEnvironment() throws IOException {
    try (Connection standardConnection = ConnectionFactory.createConnection();
            Admin standardAdmin = standardConnection.getAdmin();
            RepositoryAdmin repositoryAdmin = new RepositoryAdmin(standardConnection)) {

      RepositoryAdmin.uninstallRepositoryStructures(standardAdmin);

      // loop to disable and drop test tables and namespaces
      for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
        if (!standardAdmin.tableExists(tableName)) {
          continue;
        }
        standardAdmin.disableTable(tableName);
        standardAdmin.deleteTable(tableName);
      }
      for (String namespaceName : testNamespacesAndDescriptors.keySet()) {
        if (!repositoryAdmin.namespaceExists(namespaceName)) {
          continue;
        }
        standardAdmin.deleteNamespace(namespaceName);
      }
    }
  }

  private void initializeTestNamespaceAndTableObjects() {

    testNamespacesAndDescriptors = new TreeMap<>();
    testTableNamesAndDescriptors = new TreeMap<>();
    testColumnFamilyNamesAndDescriptors = new TreeMap<>();

    for (String namespace : TEST_NAMESPACE_LIST) {
      testNamespacesAndDescriptors.put(namespace, NamespaceDescriptor.create(namespace).build());
      for (String tableNameString : TEST_TABLE_NAME_LIST) {
        TableName tableName = TableName.valueOf(namespace, tableNameString);
        testTableNamesAndDescriptors.
                put(tableName, new HTableDescriptor(tableName));
      }
    }
    for (byte[] columnFamily : TEST_COLUMN_FAMILY_LIST) {
      testColumnFamilyNamesAndDescriptors.put(
              Bytes.toString(columnFamily), new HColumnDescriptor(columnFamily));
    }
  }

  private void createSchemaStructuresInHBase(Configuration configuration) throws IOException {
    int memStoreFlushSize = 60000000;
    int maxVersions = 8;
    boolean alternateBooleanAttribute = false;

    try (Connection mConnection = MConnectionFactory.createConnection(configuration);
            Admin mAdmin = mConnection.getAdmin();
            RepositoryAdmin repositoryAdmin = new RepositoryAdmin(mConnection)) {
      for (NamespaceDescriptor nd : testNamespacesAndDescriptors.values()) {
        nd.setConfiguration("NamespaceConfigTest", "value=" + nd.getName());
        mAdmin.createNamespace(nd);
      }
      for (HTableDescriptor htd : testTableNamesAndDescriptors.values()) {
        htd.setMemStoreFlushSize(memStoreFlushSize++);
        htd.setDurability(Durability.SKIP_WAL);
        for (HColumnDescriptor hcd : testColumnFamilyNamesAndDescriptors.values()) {
          if (alternateBooleanAttribute) {
            alternateBooleanAttribute = false;
          } else {
            alternateBooleanAttribute = true;
          }
          hcd.setInMemory(alternateBooleanAttribute);
          hcd.setMaxVersions(maxVersions++);
          htd.addFamily(hcd);
        }
        mAdmin.createTable(htd);
      }
    }
  }

  private void loadColumnData(Configuration configuration) throws IOException {

    try (Connection mConnection = MConnectionFactory.createConnection(configuration)) {

      // put rows into Table which is INCLUDED for auditing
      try (Table table01InNamespace01 = mConnection.getTable(TableName.valueOf(
              TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
              TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)))) {
        List<Put> putList = new ArrayList<>();
        putList.add(new Put(ROW_ID_01).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(0), TEST_COLUMN_QUALIFIER_LIST.get(0),
                        VALUE_2_BYTES_LONG).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(0), TEST_COLUMN_QUALIFIER_LIST.get(1),
                        VALUE_5_BYTES_LONG));
        putList.add(new Put(ROW_ID_02).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(0), TEST_COLUMN_QUALIFIER_LIST.get(0),
                        VALUE_82_BYTES_LONG).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(0), TEST_COLUMN_QUALIFIER_LIST.get(2),
                        VALUE_9_BYTES_LONG).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(1), TEST_COLUMN_QUALIFIER_LIST.get(3),
                        VALUE_82_BYTES_LONG));
        table01InNamespace01.put(putList);
      }

      // put two rows into Table in Namespace which is NOT included for ColumnManager auditing
      try (Table table01InNamespace02 = mConnection.getTable(TableName.valueOf(
              TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
              TEST_TABLE_NAME_LIST.get(TABLE01_INDEX)))) {

        List<Put> putList = new ArrayList<>();
        putList.add(new Put(ROW_ID_01).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(0), QUALIFIER_IN_EXCLUDED_TABLE,
                        VALUE_2_BYTES_LONG).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(1), QUALIFIER_IN_EXCLUDED_TABLE,
                        VALUE_82_BYTES_LONG));
        putList.add(new Put(ROW_ID_02).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(0), QUALIFIER_IN_EXCLUDED_TABLE,
                        VALUE_9_BYTES_LONG).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(1), QUALIFIER_IN_EXCLUDED_TABLE,
                        VALUE_82_BYTES_LONG));
        table01InNamespace02.put(putList);
      }

      // put one row into Table which is explicitly NOT included for ColumnManager auditing
      try (Table table02InNamespace03 = mConnection.getTable(TableName.valueOf(
              TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
              TEST_TABLE_NAME_LIST.get(TABLE02_INDEX)))) {

        List<Put> putList = new ArrayList<>();
        putList.add(new Put(ROW_ID_03).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(0), QUALIFIER_IN_EXCLUDED_TABLE,
                        VALUE_9_BYTES_LONG).
                addColumn(TEST_COLUMN_FAMILY_LIST.get(1), QUALIFIER_IN_EXCLUDED_TABLE,
                        VALUE_5_BYTES_LONG));
        table02InNamespace03.put(putList);
      }
    }
  }

  public static void main(String[] args) throws Exception {
   // new TestRepositoryAdmin().testStaticMethods();
    new TestRepositoryAdmin().testColumnAuditingWithExplicitExcludes();
    // new TestRepositoryAdmin().testColumnAuditingWithExplicitIncludes();
  }
}
