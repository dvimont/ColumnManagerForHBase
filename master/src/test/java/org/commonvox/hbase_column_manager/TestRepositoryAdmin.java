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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

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

  private static final TableName NAMESPACE01_TABLE01
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE01_INDEX));
  private static final TableName NAMESPACE01_TABLE02
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE02_INDEX));
  private static final TableName NAMESPACE01_TABLE03
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE03_INDEX));
  private static final TableName NAMESPACE01_TABLE04
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE04_INDEX));
  private static final TableName NAMESPACE02_TABLE01
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE01_INDEX));
  private static final TableName NAMESPACE02_TABLE02
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE02_INDEX));
  private static final TableName NAMESPACE02_TABLE03
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE03_INDEX));
  private static final TableName NAMESPACE02_TABLE04
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE02_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE04_INDEX));
  private static final TableName NAMESPACE03_TABLE01
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE01_INDEX));
  private static final TableName NAMESPACE03_TABLE02
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE02_INDEX));
  private static final TableName NAMESPACE03_TABLE03
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE03_INDEX));
  private static final TableName NAMESPACE03_TABLE04
          = TableName.valueOf(TEST_NAMESPACE_LIST.get(NAMESPACE03_INDEX),
                  TEST_TABLE_NAME_LIST.get(TABLE04_INDEX));
  private static final byte[] CF01 = TEST_COLUMN_FAMILY_LIST.get(CF01_INDEX);
  private static final byte[] CF02 = TEST_COLUMN_FAMILY_LIST.get(CF02_INDEX);
  private static final byte[] COLQUALIFIER01 = TEST_COLUMN_QUALIFIER_LIST.get(0);
  private static final byte[] COLQUALIFIER02 = TEST_COLUMN_QUALIFIER_LIST.get(1);
  private static final byte[] COLQUALIFIER03 = TEST_COLUMN_QUALIFIER_LIST.get(2);
  private static final byte[] COLQUALIFIER04 = TEST_COLUMN_QUALIFIER_LIST.get(3);

  private static final Set<byte[]> expectedColQualifiersForNamespace1Table1Cf1
            = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
  static {
    expectedColQualifiersForNamespace1Table1Cf1.add(COLQUALIFIER01);
    expectedColQualifiersForNamespace1Table1Cf1.add(COLQUALIFIER02);
    expectedColQualifiersForNamespace1Table1Cf1.add(COLQUALIFIER03);
  }
  private static final Set<byte[]> expectedColQualifiersForNamespace1Table1Cf2
            = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
  static {
    expectedColQualifiersForNamespace1Table1Cf2.add(COLQUALIFIER04);
  }
  private static final Set<byte[]> expectedColQualifiersForNamespace3Table1Cf1
            = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
  static {
    expectedColQualifiersForNamespace3Table1Cf1.add(COLQUALIFIER01);
    expectedColQualifiersForNamespace3Table1Cf1.add(COLQUALIFIER03);
  }
  private static final Set<ColumnAuditor> expectedColAuditorsForNamespace1Table1Cf1 = new TreeSet<>();
  static {
    expectedColAuditorsForNamespace1Table1Cf1.add(
            new ColumnAuditor(COLQUALIFIER01).setMaxValueLengthFound(82));
    expectedColAuditorsForNamespace1Table1Cf1.add(
            new ColumnAuditor(COLQUALIFIER02).setMaxValueLengthFound(5));
    expectedColAuditorsForNamespace1Table1Cf1.add(
            new ColumnAuditor(COLQUALIFIER03).setMaxValueLengthFound(9));
  }
  private static final Set<ColumnAuditor> expectedColAuditorsForNamespace1Table1Cf2 = new TreeSet<>();
  static {
    expectedColAuditorsForNamespace1Table1Cf2.add(
            new ColumnAuditor(COLQUALIFIER04).setMaxValueLengthFound(82));
  }
  private static final Set<ColumnAuditor> expectedColAuditorsForNamespace3Table1Cf1 = new TreeSet<>();
  static {
    expectedColAuditorsForNamespace3Table1Cf1.add(
            new ColumnAuditor(COLQUALIFIER01).setMaxValueLengthFound(9));
    expectedColAuditorsForNamespace3Table1Cf1.add(
            new ColumnAuditor(COLQUALIFIER03).setMaxValueLengthFound(82));
  }
  private static final String ALTERNATE_USERNAME = "testAlternateUserName";

  private static final String TEST_ENVIRONMENT_SETUP_PROBLEM
          = "TEST ENVIRONMENT SETUP PROBLEM!! ==>> ";
  private static final String REPOSITORY_ADMIN_FAILURE
          = "FAILURE IN " + RepositoryAdmin.class.getSimpleName() + " PROCESSING!! ==>> ";
  private static final String COLUMN_AUDIT_FAILURE = "FAILURE IN Column Audit PROCESSING!! ==>> ";
  private static final String GET_COL_QUALIFIERS_FAILURE
          = COLUMN_AUDIT_FAILURE + "#getColumnQualifiers method returned unexpected results";
  private static final String GET_COL_AUDITORS_FAILURE
          = COLUMN_AUDIT_FAILURE + "#getColumnAuditors method returned unexpected results";
  private static final String COLUMN_ENFORCE_FAILURE
          = "FAILURE IN Column Enforce PROCESSING!! ==>> ";
  private static final String COL_QUALIFIER_ENFORCE_FAILURE
          = COLUMN_ENFORCE_FAILURE + "FAILURE IN enforcement of Column Qualifier";
  private static final String COL_LENGTH_ENFORCE_FAILURE
          = COLUMN_ENFORCE_FAILURE + "FAILURE IN enforcement of Column Length";
  private static final String COL_VALUE_ENFORCE_FAILURE
          = COLUMN_ENFORCE_FAILURE + "FAILURE IN enforcement of Column Value (regex)";
  private static final String HSA_FAILURE = "FAILURE IN HBase Schema Archive PROCESSING!! ==>> ";

  private static final String CHANGE_EVENT_FAILURE = "FAILURE IN ChangeEvent PROCESSING!! ==>> ";

  // non-static fields
  private Map<String, NamespaceDescriptor> testNamespacesAndDescriptors;
  private Map<TableName, HTableDescriptor> testTableNamesAndDescriptors;
  private Map<String, HColumnDescriptor> testColumnFamilyNamesAndDescriptors;
  private boolean usernameSuffix;

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
              Repository.DEFAULT_REPOSITORY_MAX_VERSIONS,
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
  public void testColumnAuditingWithWildcardedExcludes() throws IOException {
    System.out.println("#testColumnAuditing has been invoked using WILDCARDED "
            + "EXCLUDE config properties.");

    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, false);
    loadColumnData(configuration, false);
    verifyColumnAuditing(configuration);
    System.out.println("#testColumnAuditing using WILDCARDED EXCLUDE config properties has "
            + "run to completion.");
  }

  @Test
  public void testColumnAuditingWithExplicitExcludes() throws IOException {
    System.out.println("#testColumnAuditing has been invoked using EXPLICIT "
            + "EXCLUDE config properties.");

    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES,
            NAMESPACE03_TABLE02.getNameAsString(),
            NAMESPACE03_TABLE04.getNameAsString(),
            NAMESPACE02_TABLE01.getNameAsString(),
            NAMESPACE02_TABLE02.getNameAsString(),
            NAMESPACE02_TABLE03.getNameAsString(),
            NAMESPACE02_TABLE04.getNameAsString());
    createSchemaStructuresInHBase(configuration, false);
    loadColumnData(configuration, false);
    verifyColumnAuditing(configuration);
    System.out.println("#testColumnAuditing using EXPLICIT EXCLUDE config properties has "
            + "run to completion.");
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
            NAMESPACE03_TABLE01.getNameAsString(),
            NAMESPACE03_TABLE03.getNameAsString(),
            NAMESPACE01_TABLE01.getNameAsString(),
            NAMESPACE01_TABLE02.getNameAsString(),
            NAMESPACE01_TABLE03.getNameAsString(),
            NAMESPACE01_TABLE04.getNameAsString()
    );

    createSchemaStructuresInHBase(configuration, false);
    loadColumnData(configuration, false);
    verifyColumnAuditing(configuration);
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
            NAMESPACE03_TABLE01.getNameAsString(),
            NAMESPACE03_TABLE03.getNameAsString(),
            TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX) + ":*"  // include all namespace01 tables!!
    );

    createSchemaStructuresInHBase(configuration, false);
    loadColumnData(configuration, false);
    verifyColumnAuditing(configuration);
    System.out.println("#testColumnAuditing using WILDCARDED INCLUDE config properties has "
            + "run to completion.");
  }

  @Test
  public void testColumnDiscoveryWithWildcardedExcludes() throws IOException {
    System.out.println("#testColumnDiscovery has been invoked using WILDCARDED "
            + "EXCLUDE config properties.");

    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, true);
    loadColumnData(configuration, true);
    doColumnDiscovery(configuration);
    verifyColumnAuditing(configuration);
    System.out.println("#testColumnDiscovery using WILDCARDED EXCLUDE config properties has "
            + "run to completion.");
  }

  @Test
  public void testColumnDefinitionAndEnforcement() throws IOException {
    System.out.println("#testColumnDefinitionAndEnforcement has been invoked using WILDCARDED "
            + "EXCLUDE config properties.");

    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, false);
    createAndEnforceColumnDefinitions(configuration);
    clearTestingEnvironment();

    System.out.println("#testColumnDefinitionAndEnforcement using WILDCARDED EXCLUDE config "
            + "properties has run to completion.");
  }

  private void clearTestingEnvironment() throws IOException {
    try (Connection standardConnection = ConnectionFactory.createConnection();
            Admin standardAdmin = standardConnection.getAdmin();
            RepositoryAdmin repositoryAdmin = new RepositoryAdmin(standardConnection)) {

      RepositoryAdmin.uninstallRepositoryStructures(standardAdmin);

      dropTestTablesAndNamespaces(standardAdmin);
    }
  }

  private void dropTestTablesAndNamespaces(Admin standardAdmin)
          throws IOException {
    // loop to disable and drop test tables and namespaces
    for (TableName tableName : testTableNamesAndDescriptors.keySet()) {
      if (!standardAdmin.tableExists(tableName)) {
        continue;
      }
      standardAdmin.disableTable(tableName);
      standardAdmin.deleteTable(tableName);
    }
    for (String namespaceName : testNamespacesAndDescriptors.keySet()) {
      try { standardAdmin.deleteNamespace(namespaceName);
      } catch (NamespaceNotFoundException e) {}
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
        testTableNamesAndDescriptors.put(tableName, new HTableDescriptor(tableName));
      }
    }
    for (byte[] columnFamily : TEST_COLUMN_FAMILY_LIST) {
      testColumnFamilyNamesAndDescriptors.put(
              Bytes.toString(columnFamily), new HColumnDescriptor(columnFamily));
    }
  }

  private void createSchemaStructuresInHBase(
          Configuration configuration, boolean bypassColumnManager) throws IOException {
    int memStoreFlushSize = 60000000;
    int maxVersions = 8;
    boolean alternateBooleanAttribute = false;

    try (Admin mAdmin = MConnectionFactory.createConnection(configuration).getAdmin();
            Admin standardAdmin = ConnectionFactory.createConnection(configuration).getAdmin()) {
      for (NamespaceDescriptor nd : testNamespacesAndDescriptors.values()) {
        nd.setConfiguration("NamespaceConfigTest", "value=" + nd.getName());
        if (bypassColumnManager) {
          standardAdmin.createNamespace(nd);
        } else {
          mAdmin.createNamespace(nd);
        }
      }
      for (HTableDescriptor htd : testTableNamesAndDescriptors.values()) {
        htd.setMemStoreFlushSize(memStoreFlushSize++);
        htd.setDurability(Durability.SKIP_WAL);
        for (HColumnDescriptor hcd : testColumnFamilyNamesAndDescriptors.values()) {
          alternateBooleanAttribute = !alternateBooleanAttribute;
          hcd.setInMemory(alternateBooleanAttribute);
          hcd.setMaxVersions(maxVersions++);
          htd.addFamily(hcd);
        }
        if (bypassColumnManager) {
          standardAdmin.createTable(htd);
        } else {
          mAdmin.createTable(htd);
        }
      }
    }
  }

  private void loadColumnData(Configuration configuration, boolean bypassColumnManager)
          throws IOException {

    try (Connection mConnection = MConnectionFactory.createConnection(configuration);
            Connection standardConnection = ConnectionFactory.createConnection(configuration)) {

      Connection connection;
      if (bypassColumnManager) {
        connection = standardConnection;
      } else {
        connection = mConnection;
      }
      // put rows into Table which is INCLUDED for auditing
      try (Table table01InNamespace01 = connection.getTable(NAMESPACE01_TABLE01)) {
        List<Put> putList = new ArrayList<>();
        putList.add(new Put(ROW_ID_01).
                addColumn(CF01, COLQUALIFIER01, VALUE_2_BYTES_LONG).
                addColumn(CF01, COLQUALIFIER02, VALUE_5_BYTES_LONG));
        putList.add(new Put(ROW_ID_02).
                addColumn(CF01, COLQUALIFIER01, VALUE_82_BYTES_LONG).
                addColumn(CF01, COLQUALIFIER03, VALUE_9_BYTES_LONG).
                addColumn(CF02, COLQUALIFIER04, VALUE_82_BYTES_LONG));
        table01InNamespace01.put(putList);
      }

      try (Table table01InNamespace03 = connection.getTable(NAMESPACE03_TABLE01)) {

        List<Put> putList = new ArrayList<>();
        putList.add(new Put(ROW_ID_04).
                addColumn(CF01, COLQUALIFIER03, VALUE_82_BYTES_LONG).
                addColumn(CF01, COLQUALIFIER01, VALUE_9_BYTES_LONG));
        table01InNamespace03.put(putList);
      }

      // put two rows into Table in Namespace which is NOT included for ColumnManager auditing
      try (Table table01InNamespace02 = connection.getTable(NAMESPACE02_TABLE01)) {

        List<Put> putList = new ArrayList<>();
        putList.add(new Put(ROW_ID_01).
                addColumn(CF01, QUALIFIER_IN_EXCLUDED_TABLE, VALUE_2_BYTES_LONG).
                addColumn(CF02, QUALIFIER_IN_EXCLUDED_TABLE, VALUE_82_BYTES_LONG));
        putList.add(new Put(ROW_ID_02).
                addColumn(CF01, QUALIFIER_IN_EXCLUDED_TABLE, VALUE_9_BYTES_LONG).
                addColumn(CF02, QUALIFIER_IN_EXCLUDED_TABLE, VALUE_82_BYTES_LONG));
        table01InNamespace02.put(putList);
      }

      // put one row into Table which is explicitly NOT included for ColumnManager auditing
      try (Table table02InNamespace03 = connection.getTable(NAMESPACE03_TABLE02)) {

        List<Put> putList = new ArrayList<>();
        putList.add(new Put(ROW_ID_03).
                addColumn(CF01, QUALIFIER_IN_EXCLUDED_TABLE, VALUE_9_BYTES_LONG).
                addColumn(CF02, QUALIFIER_IN_EXCLUDED_TABLE, VALUE_5_BYTES_LONG));
        table02InNamespace03.put(putList);
      }
    }
  }

  private void doColumnDiscovery(Configuration configuration) throws IOException {
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      repositoryAdmin.discoverSchema();
    }
  }

  private void verifyColumnAuditing(Configuration configuration) throws IOException {

    try (Connection mConnection = MConnectionFactory.createConnection(configuration);
            RepositoryAdmin repositoryAdmin = new RepositoryAdmin(mConnection)) {

      // Test #getColumnQualifiers
      Set<byte[]> returnedColQualifiersForNamespace1Table1Cf1
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(NAMESPACE01_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf1.equals(
                      returnedColQualifiersForNamespace1Table1Cf1));

      Set<byte[]> returnedColQualifiersForNamespace1Table1Cf2
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(NAMESPACE01_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF02)));
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf2.equals(
                      returnedColQualifiersForNamespace1Table1Cf2));

      Set<byte[]> returnedColQualifiersForNamespace2Table1Cf1
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(NAMESPACE02_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace2Table1Cf1 == null);

      Set<byte[]> returnedColQualifiersForNamespace3Table1Cf1
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(NAMESPACE03_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace3Table1Cf1.equals(
                      returnedColQualifiersForNamespace3Table1Cf1));

      Set<byte[]> returnedColQualifiersForNamespace3Table2Cf1
              = repositoryAdmin.getColumnQualifiers(
                      testTableNamesAndDescriptors.get(NAMESPACE03_TABLE02),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace3Table2Cf1 == null);

      // Test #getColumnQualifiers with alternate signature
      returnedColQualifiersForNamespace1Table1Cf1
              = repositoryAdmin.getColumnQualifiers(NAMESPACE01_TABLE01, CF01);
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf1.equals(
                      returnedColQualifiersForNamespace1Table1Cf1));

      returnedColQualifiersForNamespace1Table1Cf2
              = repositoryAdmin.getColumnQualifiers(NAMESPACE01_TABLE01, CF02);
      assertTrue(GET_COL_QUALIFIERS_FAILURE,
              expectedColQualifiersForNamespace1Table1Cf2.equals(
                      returnedColQualifiersForNamespace1Table1Cf2));

      returnedColQualifiersForNamespace2Table1Cf1
              = repositoryAdmin.getColumnQualifiers(NAMESPACE02_TABLE01, CF01);
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace2Table1Cf1 == null);

      returnedColQualifiersForNamespace3Table2Cf1
              = repositoryAdmin.getColumnQualifiers(NAMESPACE03_TABLE02, CF01);
      assertTrue(GET_COL_QUALIFIERS_FAILURE, returnedColQualifiersForNamespace3Table2Cf1 == null);

      // Test #getColumnAuditors
      Set<ColumnAuditor> returnedColAuditorsForNamespace1Table1Cf1
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(NAMESPACE01_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf1.equals(
                      returnedColAuditorsForNamespace1Table1Cf1));

      Set<ColumnAuditor> returnedColAuditorsForNamespace1Table1Cf2
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(NAMESPACE01_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF02)));
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf2.equals(
                      returnedColAuditorsForNamespace1Table1Cf2));

      Set<ColumnAuditor> returnedColAuditorsForNamespace2Table1Cf1
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(NAMESPACE02_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace2Table1Cf1 == null);

      Set<ColumnAuditor> returnedColAuditorsForNamespace3Table1Cf1
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(NAMESPACE03_TABLE01),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace3Table1Cf1.equals(
                      returnedColAuditorsForNamespace3Table1Cf1));

      Set<ColumnAuditor> returnedColAuditorsForNamespace3Table2Cf1
              = repositoryAdmin.getColumnAuditors(
                      testTableNamesAndDescriptors.get(NAMESPACE03_TABLE02),
                      testColumnFamilyNamesAndDescriptors.get(Bytes.toString(CF01)));
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace3Table2Cf1 == null);

      // Test #getColumnAuditors with alternate signature
      returnedColAuditorsForNamespace1Table1Cf1
              = repositoryAdmin.getColumnAuditors(NAMESPACE01_TABLE01, CF01);
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf1.equals(
                      returnedColAuditorsForNamespace1Table1Cf1));

      returnedColAuditorsForNamespace1Table1Cf2
              = repositoryAdmin.getColumnAuditors(NAMESPACE01_TABLE01, CF02);
      assertTrue(GET_COL_AUDITORS_FAILURE,
              expectedColAuditorsForNamespace1Table1Cf2.equals(
                      returnedColAuditorsForNamespace1Table1Cf2));

      returnedColAuditorsForNamespace2Table1Cf1
              = repositoryAdmin.getColumnAuditors(NAMESPACE02_TABLE01, CF01);
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace2Table1Cf1 == null);

      returnedColAuditorsForNamespace3Table2Cf1
              = repositoryAdmin.getColumnAuditors(NAMESPACE03_TABLE02, CF01);
      assertTrue(GET_COL_AUDITORS_FAILURE, returnedColAuditorsForNamespace3Table2Cf1 == null);
    }
    clearTestingEnvironment();
  }

  private void createAndEnforceColumnDefinitions(Configuration configuration) throws IOException {
    ColumnDefinition col01Definition = new ColumnDefinition(COLQUALIFIER01);
    ColumnDefinition col02Definition = new ColumnDefinition(COLQUALIFIER02).setColumnLength(20L);
    ColumnDefinition col03Definition
            = new ColumnDefinition(COLQUALIFIER03).setColumnValidationRegex("https?://.*");
    ColumnDefinition col04Definition
            = new ColumnDefinition(COLQUALIFIER04).setColumnLength(8L);

    try (Connection connection = MConnectionFactory.createConnection(configuration);
            RepositoryAdmin repositoryAdmin = new RepositoryAdmin(connection)) {
      repositoryAdmin.addColumnDefinition(NAMESPACE01_TABLE01, CF01, col01Definition);
      repositoryAdmin.addColumnDefinition(NAMESPACE01_TABLE01, CF01, col02Definition);
      repositoryAdmin.addColumnDefinition(NAMESPACE01_TABLE01, CF02, col03Definition);
      // next def not enforced, since namespace02 tables not included for CM processing!
      repositoryAdmin.addColumnDefinition(NAMESPACE02_TABLE03, CF01, col04Definition);

      repositoryAdmin.setColumnDefinitionsEnforced(true, NAMESPACE01_TABLE01, CF01);
      repositoryAdmin.setColumnDefinitionsEnforced(true, NAMESPACE01_TABLE01, CF02);
       // next def not enforced, since namespace02 tables not included for CM processing!
      repositoryAdmin.setColumnDefinitionsEnforced(true, NAMESPACE02_TABLE03, CF01);

      try (Table table01InNamespace01 = connection.getTable(NAMESPACE01_TABLE01);
              Table table03InNamespace02 = connection.getTable(NAMESPACE02_TABLE03)) {
        // put a row with valid columns
        table01InNamespace01.put(new Put(ROW_ID_01).
                addColumn(CF01, COLQUALIFIER01, VALUE_2_BYTES_LONG).
                addColumn(CF01, COLQUALIFIER02, VALUE_5_BYTES_LONG));
        // put a row with invalid column qualifier
        try {
          table01InNamespace01.put(new Put(ROW_ID_01).
                  addColumn(CF01, COLQUALIFIER03, VALUE_2_BYTES_LONG));
          fail(COL_QUALIFIER_ENFORCE_FAILURE);
        } catch (ColumnDefinitionNotFoundException e) {
        }
        // put same row to unenforced namespace/table
        table03InNamespace02.put(new Put(ROW_ID_01).
                addColumn(CF01, COLQUALIFIER03, VALUE_2_BYTES_LONG));
        // put a row with invalid column length
        try {
          table01InNamespace01.put(new Put(ROW_ID_01).
                  addColumn(CF01, COLQUALIFIER02, VALUE_82_BYTES_LONG));
          fail(COL_LENGTH_ENFORCE_FAILURE);
        } catch (InvalidColumnValueException e) {
        }
        // put same row to unenforced namespace/table
        table03InNamespace02.put(new Put(ROW_ID_01).
                addColumn(CF01, COLQUALIFIER02, VALUE_82_BYTES_LONG));
        // put a row with valid column value to regex-restricted column
        table01InNamespace01.put(new Put(ROW_ID_01).
                addColumn(CF02, COLQUALIFIER03, Bytes.toBytes("http://google.com")));
        // put a row with invalid column value
        try {
          table01InNamespace01.put(new Put(ROW_ID_01).
                  addColumn(CF02, COLQUALIFIER03, Bytes.toBytes("ftp://google.com")));
          fail(COL_VALUE_ENFORCE_FAILURE);
        } catch (InvalidColumnValueException e) {
          // this Exception SHOULD be thrown!
        }
        // put same row to unenforced namespace/table
        table03InNamespace02.put(new Put(ROW_ID_01).
                addColumn(CF02, COLQUALIFIER03, Bytes.toBytes("ftp://google.com")));
      }
    }
  }

  @Rule
  public TemporaryFolder tempTestFolder = new TemporaryFolder();

  @Test
  public void testExportImport() throws IOException, JAXBException {
    System.out.println("#testExportImport has been invoked.");
    // file setup
    final String TARGET_DIRECTORY = "target/"; // for standalone (non-JUnit) execution
    final String TARGET_EXPORT_ALL_FILE = "temp.export.repository.hsa.xml";
    final String TARGET_EXPORT_NAMESPACE_FILE = "temp.export.namespace.hsa.xml";
    final String TARGET_EXPORT_TABLE_FILE = "temp.export.table.hsa.xml";
    File exportAllFile;
    File exportNamespaceFile;
    File exportTableFile;
    try {
      exportAllFile = tempTestFolder.newFile(TARGET_EXPORT_ALL_FILE);
      exportNamespaceFile = tempTestFolder.newFile(TARGET_EXPORT_NAMESPACE_FILE);
      exportTableFile = tempTestFolder.newFile(TARGET_EXPORT_TABLE_FILE);
    } catch (IllegalStateException e) { // standalone (non-JUnit) execution
      exportAllFile = new File(TARGET_DIRECTORY + TARGET_EXPORT_ALL_FILE);
      exportNamespaceFile = new File(TARGET_DIRECTORY + TARGET_EXPORT_NAMESPACE_FILE);
      exportTableFile = new File(TARGET_DIRECTORY + TARGET_EXPORT_TABLE_FILE);
    }
    // environment cleanup before testing
    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // add schema and data to HBase
    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, false);
    loadColumnData(configuration, false);

    // extract schema into external HBase Schema Archive files
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      repositoryAdmin.exportRepository(exportAllFile, true);
      repositoryAdmin.exportNamespaceSchema(
              TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX), exportNamespaceFile, true);
      repositoryAdmin.exportTableSchema(NAMESPACE01_TABLE01, exportTableFile, true);
    }
    clearTestingEnvironment();

    // NOW restore full schema from external HSA file and verify that all structures restored
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      repositoryAdmin.importSchema(true, exportAllFile);
    }
    verifyColumnAuditing(configuration);

    // validate all export files against the XML-schema
    validateXmlAgainstXsd(exportAllFile);
    validateXmlAgainstXsd(exportNamespaceFile);
    validateXmlAgainstXsd(exportTableFile);

    // assure appropriate content in Namespace and Table archive files!!
    HBaseSchemaArchive repositoryArchive = HBaseSchemaArchive.deserializeXmlFile(exportAllFile);
    HBaseSchemaArchive namespaceArchive
            = HBaseSchemaArchive.deserializeXmlFile(exportNamespaceFile);
    HBaseSchemaArchive tableArchive = HBaseSchemaArchive.deserializeXmlFile(exportTableFile);
    for (SchemaEntity entity : repositoryArchive.getSchemaEntities()) {
      if (entity.getEntityRecordType() == SchemaEntityType.NAMESPACE.getRecordType()
              && entity.getNameAsString().equals(TEST_NAMESPACE_LIST.get(NAMESPACE01_INDEX))) {
        assertEquals(HSA_FAILURE + "Namespace SchemaEntity inconsistencies between full archive "
                + "and namespace-only archive.",
                entity, namespaceArchive.getSchemaEntities().iterator().next());
        for (SchemaEntity childEntity : entity.getChildren()) {
          if (childEntity.getEntityRecordType() == SchemaEntityType.TABLE.getRecordType()
                  && childEntity.getNameAsString().equals(NAMESPACE01_TABLE01.getNameAsString())) {
            SchemaEntity namespaceEntityInTableArchive
                    = tableArchive.getSchemaEntities().iterator().next();
            assertEquals(HSA_FAILURE + "Namespace ShemaEntity in Table Archive has unexpected "
                    + "size of children Set.",
                    1, namespaceEntityInTableArchive.getChildren().size());
            assertEquals(HSA_FAILURE + "Table SchemaEntity inconsistencies between full archive "
                    + "and table-only archive.",
                    childEntity, namespaceEntityInTableArchive.getChildren().iterator().next());
          }
        }
      }
    }
    System.out.println("#testExportImport has run to completeion.");
  }

  private void validateXmlAgainstXsd(File xmlFile) throws IOException {
    Document hsaDocument = null;
    Schema hsaSchema = null;
    try {
      hsaDocument = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
    } catch (ParserConfigurationException pce) {
      fail(TEST_ENVIRONMENT_SETUP_PROBLEM + " parser config exception thrown: " + pce.getMessage());
    } catch (SAXException se) {
      fail(REPOSITORY_ADMIN_FAILURE + " SAX exception thrown while loading test document: "
              + se.getMessage());
    }
    try {
      hsaSchema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(
              Paths.get(ClassLoader.getSystemResource(
                      XmlSchemaGenerator.DEFAULT_OUTPUT_FILE_NAME).toURI()).toFile());
    } catch (URISyntaxException ue) {
      fail(TEST_ENVIRONMENT_SETUP_PROBLEM + " URI syntax exception thrown: " + ue.getMessage());
    } catch (SAXException se) {
      fail(REPOSITORY_ADMIN_FAILURE + " SAX exception thrown while loading XML-schema: "
              + se.getMessage());
    }
    // validate against XSD
    try {
      hsaSchema.newValidator().validate(new DOMSource(hsaDocument));
    } catch (SAXException se) {
      fail(REPOSITORY_ADMIN_FAILURE + " exported HSA file is invalid with respect to "
              + "XML schema: " + se.getMessage());
    }
  }

  @Test
  public void testChangeEventMonitor() throws IOException, URISyntaxException {
    System.out.println("#testChangeEventMonitor has been invoked.");

    // environment cleanup before testing
    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // add schema and data to HBase
    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    changeJavaUsername();
    createSchemaStructuresInHBase(configuration, false);
    changeJavaUsername();
    createAndEnforceColumnDefinitions(configuration);
    deleteTableInHBase(configuration);

    // file setup
    final String TARGET_DIRECTORY = "target/"; // for standalone (non-JUnit) execution
    final String TEMP_PREFIX = "temp.";
    final String RESOURCE_PREFIX = "test.";
    final String EVENTS_BY_TIMESTAMP = "changeEvents.timestampOrder.csv";
    final String EVENTS_BY_TIMESTAMP_FILE = TEMP_PREFIX + EVENTS_BY_TIMESTAMP;
    final String EVENTS_BY_TIMESTAMP_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_BY_TIMESTAMP;
    final String EVENTS_BY_USERNAME = "changeEvents.userNameOrder.csv";
    final String EVENTS_BY_USERNAME_FILE = TEMP_PREFIX + EVENTS_BY_USERNAME;
    final String EVENTS_BY_USERNAME_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_BY_USERNAME;
    final String EVENTS_FOR_USER = "changeEvents.forUser.csv";
    final String EVENTS_FOR_USER_FILE = TEMP_PREFIX + EVENTS_FOR_USER;
    final String EVENTS_FOR_USER_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_FOR_USER;
    final String EVENTS_FOR_NAMESPACE = "changeEvents.forNamespace.csv";
    final String EVENTS_FOR_NAMESPACE_FILE = TEMP_PREFIX + EVENTS_FOR_NAMESPACE;
    final String EVENTS_FOR_NAMESPACE_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_FOR_NAMESPACE;
    final String EVENTS_FOR_TABLE = "changeEvents.forTable.csv";
    final String EVENTS_FOR_TABLE_FILE = TEMP_PREFIX + EVENTS_FOR_TABLE;
    final String EVENTS_FOR_TABLE_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_FOR_TABLE;
    final String EVENTS_FOR_COL_FAMILY = "changeEvents.forColFamily.csv";
    final String EVENTS_FOR_COL_FAMILY_FILE = TEMP_PREFIX + EVENTS_FOR_COL_FAMILY;
    final String EVENTS_FOR_COL_FAMILY_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_FOR_COL_FAMILY;
    final String EVENTS_FOR_COL_DEF = "changeEvents.forColDefinition.csv";
    final String EVENTS_FOR_COL_DEF_FILE = TEMP_PREFIX + EVENTS_FOR_COL_DEF;
    final String EVENTS_FOR_COL_DEF_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_FOR_COL_DEF;
    final String EVENTS_FOR_COL_AUDITOR = "changeEvents.forColAuditor.csv";
    final String EVENTS_FOR_COL_AUDITOR_FILE = TEMP_PREFIX + EVENTS_FOR_COL_AUDITOR;
    final String EVENTS_FOR_COL_AUDITOR_RESOURCE_FILE = RESOURCE_PREFIX + EVENTS_FOR_COL_AUDITOR;
    File eventsByTimestampFile;
    File eventsByUsernameFile;
    File eventsForUserFile;
    File eventsForNamespaceFile;
    File eventsForTableFile;
    File eventsForColumnFamilyFile;
    File eventsForColumnDefinitionFile;
    File eventsForColumnAuditorFile;
    try {
      eventsByTimestampFile = tempTestFolder.newFile(EVENTS_BY_TIMESTAMP_FILE);
      eventsByUsernameFile = tempTestFolder.newFile(EVENTS_BY_USERNAME_FILE);
      eventsForUserFile = tempTestFolder.newFile(EVENTS_FOR_USER_FILE);
      eventsForNamespaceFile = tempTestFolder.newFile(EVENTS_FOR_NAMESPACE_FILE);
      eventsForTableFile = tempTestFolder.newFile(EVENTS_FOR_TABLE_FILE);
      eventsForColumnFamilyFile = tempTestFolder.newFile(EVENTS_FOR_COL_FAMILY_FILE);
      eventsForColumnDefinitionFile = tempTestFolder.newFile(EVENTS_FOR_COL_DEF_FILE);
      eventsForColumnAuditorFile = tempTestFolder.newFile(EVENTS_FOR_COL_AUDITOR_FILE);
    } catch (IllegalStateException e) { // standalone (non-JUnit) execution
      eventsByTimestampFile = new File(TARGET_DIRECTORY + EVENTS_BY_TIMESTAMP_FILE);
      eventsByUsernameFile = new File(TARGET_DIRECTORY + EVENTS_BY_USERNAME_FILE);
      eventsForUserFile = new File(TARGET_DIRECTORY + EVENTS_FOR_USER_FILE);
      eventsForNamespaceFile = new File(TARGET_DIRECTORY + EVENTS_FOR_NAMESPACE_FILE);
      eventsForTableFile = new File(TARGET_DIRECTORY + EVENTS_FOR_TABLE_FILE);
      eventsForColumnFamilyFile = new File(TARGET_DIRECTORY + EVENTS_FOR_COL_FAMILY_FILE);
      eventsForColumnDefinitionFile = new File(TARGET_DIRECTORY + EVENTS_FOR_COL_DEF_FILE);
      eventsForColumnAuditorFile = new File(TARGET_DIRECTORY + EVENTS_FOR_COL_AUDITOR_FILE);
    }
    // create and test ChangeEventMonitor
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      ChangeEventMonitor monitor = repositoryAdmin.getChangeEventMonitor();

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getAllChangeEvents(), eventsByTimestampFile);
      compareResourceFileToExportedFile(
              EVENTS_BY_TIMESTAMP_RESOURCE_FILE, eventsByTimestampFile, "#getAllChangeEvents");

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getAllChangeEventsByUserName(), eventsByUsernameFile);
      compareResourceFileToExportedFile(EVENTS_BY_USERNAME_RESOURCE_FILE, eventsByUsernameFile,
              "#getAllChangeEventsByUserName");

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getChangeEventsForUserName("userfalse"), eventsForUserFile);
      compareResourceFileToExportedFile(EVENTS_FOR_USER_RESOURCE_FILE, eventsForUserFile,
              "#getChangeEventsForUserName");

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getChangeEventsForNamespace(NAMESPACE01_TABLE01.getNamespace()),
              eventsForNamespaceFile);
      compareResourceFileToExportedFile(EVENTS_FOR_NAMESPACE_RESOURCE_FILE, eventsForNamespaceFile,
              "#getChangeEventsForNamespace");

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getChangeEventsForTable(NAMESPACE01_TABLE01), eventsForTableFile);
      compareResourceFileToExportedFile(EVENTS_FOR_TABLE_RESOURCE_FILE, eventsForTableFile,
              "#getChangeEventsForTable");

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getChangeEventsForColumnFamily(NAMESPACE01_TABLE02, CF02),
              eventsForColumnFamilyFile);
      compareResourceFileToExportedFile(EVENTS_FOR_COL_FAMILY_RESOURCE_FILE,
              eventsForColumnFamilyFile, "#getChangeEventsForColumnFamily");

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getChangeEventsForColumnDefinition(
                      NAMESPACE01_TABLE01, CF02, COLQUALIFIER03), eventsForColumnDefinitionFile);
      compareResourceFileToExportedFile(EVENTS_FOR_COL_DEF_RESOURCE_FILE,
              eventsForColumnDefinitionFile, "#getChangeEventsForColumnDefinition");

      ChangeEventMonitor.exportChangeEventListToCsvFile(
              monitor.getChangeEventsForColumnAuditor(NAMESPACE01_TABLE01, CF01, COLQUALIFIER01),
              eventsForColumnAuditorFile);
      compareResourceFileToExportedFile(EVENTS_FOR_COL_AUDITOR_RESOURCE_FILE,
              eventsForColumnAuditorFile, "#getChangeEventsForColumnAuditor");

      final String ATTRIBUTE_NAME = "Value__MEMSTORE_FLUSHSIZE";
      List<ChangeEvent> attributeChangeEvents
              = monitor.getChangeEventsForTableAttribute(NAMESPACE01_TABLE01, ATTRIBUTE_NAME);
      assertEquals(CHANGE_EVENT_FAILURE + "unexpected value count returned from "
              + "#getChangeEventsForTableAttribute method", 1, attributeChangeEvents.size());
      assertEquals(CHANGE_EVENT_FAILURE + "unexpected attribute_name returned from "
              + "#getChangeEventsForTableAttribute method",
              ATTRIBUTE_NAME, attributeChangeEvents.get(0).getAttributeNameAsString());

      final String STATUS_ATTRIBUTE_NAME = "_Status";
      attributeChangeEvents
              = monitor.getChangeEventsForColumnFamilyAttribute(
                      NAMESPACE01_TABLE01, CF02, STATUS_ATTRIBUTE_NAME);
      assertEquals(CHANGE_EVENT_FAILURE + "unexpected value count returned from "
              + "#getChangeEventsForColumnFamilyAttribute method", 2, attributeChangeEvents.size());
      for (ChangeEvent ce : attributeChangeEvents) {
        assertEquals(CHANGE_EVENT_FAILURE + "unexpected attribute_name returned from "
                + "#getChangeEventsForColumnFamilyAttribute method",
                STATUS_ATTRIBUTE_NAME, ce.getAttributeNameAsString());
      }
    }
    clearTestingEnvironment();
    System.out.println("#testChangeEventMonitor has run to completion.");
  }

  private void compareResourceFileToExportedFile(String resourceFileString, File exportedFile,
          String methodName) throws IOException, URISyntaxException {
    Path resourcePath = Paths.get(ClassLoader.getSystemResource(resourceFileString).toURI());
    assertEquals(CHANGE_EVENT_FAILURE + "unexpected item count from " + methodName + " method",
            Files.lines(resourcePath).count(), Files.lines(exportedFile.toPath()).count());
    Iterator<String> resourceLinesIterator = Files.lines(resourcePath).iterator();
    Iterator<String> exportedLinesIterator = Files.lines(exportedFile.toPath()).iterator();
    resourceLinesIterator.next(); // skip first header line in both files
    exportedLinesIterator.next(); // skip first header line in both files
    while (resourceLinesIterator.hasNext()) {
      assertEquals(CHANGE_EVENT_FAILURE + "unexpected content returned by " + methodName,
              resourceLinesIterator.next().substring(14), // bypass timestamp at start of line
              exportedLinesIterator.next().substring(14)); // bypass timestamp at start of line
    }
  }

  private void changeJavaUsername() {
    usernameSuffix = !usernameSuffix;
    System.setProperty("user.name", "user" + usernameSuffix);
  }

  private void deleteTableInHBase(Configuration configuration) throws IOException {
    try (Admin mAdmin = MConnectionFactory.createConnection(configuration).getAdmin()) {
      mAdmin.disableTable(NAMESPACE01_TABLE01);
      mAdmin.deleteTable(NAMESPACE01_TABLE01);
    }
  }

  @Test
  public void testRepositoryMaxVersions() throws IOException {
    System.out.println("#testMaxVersions has been invoked.");
    // environment cleanup before testing
    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // add schema and data to HBase
    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, false);

    testRepositoryMaxVersionsOperation(configuration, NAMESPACE01_TABLE01);

    // test get and set methods for RepositoryMaxVersions
    final int INCREASE_IN_MAX_VERSIONS = 21;
    try (Admin standardAdmin = ConnectionFactory.createConnection(configuration).getAdmin()) {
      assertEquals(CHANGE_EVENT_FAILURE
              + "unexpected default value returned by #getRepositoryMaxVersions",
              Repository.DEFAULT_REPOSITORY_MAX_VERSIONS,
              RepositoryAdmin.getRepositoryMaxVersions(standardAdmin));
      RepositoryAdmin.setRepositoryMaxVersions(
              standardAdmin, Repository.DEFAULT_REPOSITORY_MAX_VERSIONS + INCREASE_IN_MAX_VERSIONS);
      assertEquals(CHANGE_EVENT_FAILURE
              + "unexpected value returned by #getRepositoryMaxVersions after setting incremented "
              + "with #setRepositoryMaxVersions",
              Repository.DEFAULT_REPOSITORY_MAX_VERSIONS + INCREASE_IN_MAX_VERSIONS,
              RepositoryAdmin.getRepositoryMaxVersions(standardAdmin));
    }
    // Test with new MaxVersions setting
    testRepositoryMaxVersionsOperation(configuration, NAMESPACE01_TABLE02);

    clearTestingEnvironment();
    System.out.println("#testMaxVersions has run to completion.");
  }

  private void testRepositoryMaxVersionsOperation (Configuration configuration, TableName tableName)
          throws IOException {
    // Submit 5 more changes to an attribute than maxVersions allows for.
    final int BASE_MEMSTORE_FLUSHSIZE = 64000000;
    try (Admin mAdmin = MConnectionFactory.createConnection(configuration).getAdmin()) {
      HTableDescriptor table01 = mAdmin.getTableDescriptor(tableName);
      for (int i = 0; i < (RepositoryAdmin.getRepositoryMaxVersions(mAdmin) + 5); i++) {
        table01.setMemStoreFlushSize(BASE_MEMSTORE_FLUSHSIZE + i);
        mAdmin.modifyTable(table01.getTableName(), table01);
      }
    }
    // Confirm that the first 5 changes have NOT been retained due to maxVersions limitation.
    final String ATTRIBUTE_NAME = "Value__MEMSTORE_FLUSHSIZE";
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      ChangeEventMonitor monitor = repositoryAdmin.getChangeEventMonitor();
      assertEquals(CHANGE_EVENT_FAILURE
              + "unexpected attribute value found when testing maxVersions processing",
              Integer.toString(BASE_MEMSTORE_FLUSHSIZE + 5),
              monitor.getChangeEventsForTableAttribute(
                      tableName, ATTRIBUTE_NAME).get(0).getAttributeValueAsString());
    }
  }

  @Test
  public void testRepositorySyncCheckForMissingNamespaces() throws IOException {
    System.out.println("#testRepositorySyncCheckForMissingNamespaces has been invoked.");
    // environment cleanup before testing
    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // add schema and data to HBase
    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, false);

    try (Admin standardAdmin = ConnectionFactory.createConnection().getAdmin()) {
      dropTestTablesAndNamespaces(standardAdmin);
    }

    // out-of-sync logger message should be generated upon Repository startup
    TestAppender testAppender = new TestAppender();
    Logger.getRootLogger().addAppender(testAppender);
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      int outOfSyncWarningCount = 0;
      for (LoggingEvent loggingEvent : testAppender.events) {
        if (loggingEvent.getMessage().toString().startsWith(
                Repository.NAMESPACE_NOT_FOUND_SYNC_ERROR_MSG)) {
          assertEquals("Unexpected loggingEvent level for Namespace OUT OF SYNC condition",
                  Level.WARN, loggingEvent.getLevel());
          outOfSyncWarningCount++;
        }
      }
      assertEquals("Unexpected loggingEvent count for Namespace OUT OF SYNC conditions",
              2, outOfSyncWarningCount);
    }
    clearTestingEnvironment();
    System.out.println("#testRepositorySyncCheckForMissingNamespaces has run to completion.");
  }

  @Test
  public void testRepositorySyncCheckForMissingTables() throws IOException {
    System.out.println("#testRepositorySyncCheckForMissingTables has been invoked.");
    // environment cleanup before testing
    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // add schema and data to HBase
    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, false);

    // bypass ColumnManager while deleting Tables to cause out of sync condition w/ Repository
    try (Admin standardAdmin = ConnectionFactory.createConnection().getAdmin()) {
      standardAdmin.disableTable(NAMESPACE01_TABLE02);
      standardAdmin.deleteTable(NAMESPACE01_TABLE02);
      standardAdmin.disableTable(NAMESPACE02_TABLE01); // not tracked by ColumnManager!
      standardAdmin.deleteTable(NAMESPACE02_TABLE01);
      standardAdmin.disableTable(NAMESPACE03_TABLE03);
      standardAdmin.deleteTable(NAMESPACE03_TABLE03);
    }

    // out-of-sync logger message should be generated upon Repository startup
    TestAppender testAppender = new TestAppender();
    Logger.getRootLogger().addAppender(testAppender);
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      int outOfSyncWarningCount = 0;
      for (LoggingEvent loggingEvent : testAppender.events) {
        if (loggingEvent.getMessage().toString().startsWith(
                Repository.TABLE_NOT_FOUND_SYNC_ERROR_MSG)) {
          assertEquals("Unexpected loggingEvent level for Table OUT OF SYNC condition",
                  Level.WARN, loggingEvent.getLevel());
          outOfSyncWarningCount++;
        }
      }
      assertEquals("Unexpected loggingEvent count for Table OUT OF SYNC conditions",
              2, outOfSyncWarningCount);
    }
    clearTestingEnvironment();
    System.out.println("#testRepositorySyncCheckForMissingTables has run to completion.");
  }

  @Test
  public void testRepositorySyncCheckForAttributeDiscrepancies() throws IOException {
    System.out.println("#testRepositorySyncCheckForAttributeDiscrepancies has been invoked.");
    // environment cleanup before testing
    initializeTestNamespaceAndTableObjects();
    clearTestingEnvironment();

    // add schema and data to HBase
    // NOTE that test/resources/hbase-column-manager.xml contains wildcarded excludedTables entries
    Configuration configuration = MConfiguration.create();
    createSchemaStructuresInHBase(configuration, false);

    // bypass ColumnManager while making mods to cause out of sync condition w/ Repository
    try (Admin standardAdmin = ConnectionFactory.createConnection().getAdmin()) {
      HTableDescriptor namespace01Table01 = standardAdmin.getTableDescriptor(NAMESPACE01_TABLE01);
      namespace01Table01.setCompactionEnabled(!namespace01Table01.isCompactionEnabled());
      namespace01Table01.setReadOnly(!namespace01Table01.isReadOnly());
      Collection<HColumnDescriptor> hcdCollection = namespace01Table01.getFamilies();
      for (HColumnDescriptor hcd : hcdCollection) {
        hcd.setCompressTags(!hcd.isCompressTags());
        hcd.setCacheBloomsOnWrite(!hcd.isCacheBloomsOnWrite());
        namespace01Table01.modifyFamily(hcd);
      }
      standardAdmin.modifyTable(namespace01Table01.getTableName(), namespace01Table01);
      HTableDescriptor namespace02Table01 = standardAdmin.getTableDescriptor(NAMESPACE02_TABLE01);
      namespace02Table01.setDurability(Durability.FSYNC_WAL);
      hcdCollection = namespace02Table01.getFamilies();
      for (HColumnDescriptor hcd : hcdCollection) {
        hcd.setCacheDataOnWrite(!hcd.isCacheDataOnWrite());
        namespace02Table01.modifyFamily(hcd);
      }
      standardAdmin.modifyTable(namespace02Table01.getTableName(), namespace02Table01);
      HTableDescriptor namespace03Table03 = standardAdmin.getTableDescriptor(NAMESPACE03_TABLE03);
      namespace03Table03.setDurability(Durability.FSYNC_WAL);
      hcdCollection = namespace03Table03.getFamilies();
      for (HColumnDescriptor hcd : hcdCollection) {
        hcd.setCacheDataOnWrite(!hcd.isCacheDataOnWrite());
        namespace03Table03.modifyFamily(hcd);
        break;
      }
      standardAdmin.modifyTable(namespace03Table03.getTableName(), namespace03Table03);
    }

    // out-of-sync logger message should be generated upon Repository startup
    TestAppender testAppender = new TestAppender();
    Logger.getRootLogger().addAppender(testAppender);
    try (RepositoryAdmin repositoryAdmin
            = new RepositoryAdmin(MConnectionFactory.createConnection(configuration))) {
      int tableAttributesOutOfSyncWarningCount = 0;
      int colDescriptorAttributesOutOfSyncWarningCount = 0;
      for (LoggingEvent loggingEvent : testAppender.events) {
        if (loggingEvent.getMessage().toString().startsWith(
                Repository.TABLE_ATTRIBUTE_SYNC_ERROR_MSG)) {
          assertEquals("Unexpected loggingEvent level for Table OUT OF SYNC condition",
                  Level.WARN, loggingEvent.getLevel());
          tableAttributesOutOfSyncWarningCount++;
        }
        if (loggingEvent.getMessage().toString().startsWith(
                Repository.COLDESCRIPTOR_ATTRIBUTE_SYNC_ERROR_MSG)) {
          assertEquals("Unexpected loggingEvent level for ColumnDescriptor OUT OF SYNC condition",
                  Level.WARN, loggingEvent.getLevel());
          colDescriptorAttributesOutOfSyncWarningCount++;
        }
      }
      assertEquals("Unexpected loggingEvent count for Table OUT OF SYNC conditions",
              2, tableAttributesOutOfSyncWarningCount);
      assertEquals("Unexpected loggingEvent count for ColumnDescriptor OUT OF SYNC conditions",
              3, colDescriptorAttributesOutOfSyncWarningCount);
    }
    clearTestingEnvironment();
    System.out.println("#testRepositorySyncCheckForAttributeDiscrepancies has run to completion.");
  }

  private class TestAppender extends AppenderSkeleton{
    public List<LoggingEvent> events = new ArrayList<>();
    @Override
    public void close() {}
    @Override
    public boolean requiresLayout() {return false;}
    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }
  }


  public static void main(String[] args) throws Exception {
    // new TestRepositoryAdmin().testStaticMethods();
    // new TestRepositoryAdmin().testColumnDiscoveryWithWildcardedExcludes();
    // new TestRepositoryAdmin().testColumnAuditingWithWildcardedExcludes();
    // new TestRepositoryAdmin().testColumnAuditingWithExplicitIncludes();
    // new TestRepositoryAdmin().testColumnDefinitionAndEnforcement();
    // new TestRepositoryAdmin().testExportImport();
    // new TestRepositoryAdmin().testChangeEventMonitor();
    // new TestRepositoryAdmin().testRepositoryMaxVersions();
    // new TestRepositoryAdmin().testRepositorySyncCheckForMissingNamespaces();
    // new TestRepositoryAdmin().testRepositorySyncCheckForMissingTables();
    new TestRepositoryAdmin().testRepositorySyncCheckForAttributeDiscrepancies();
  }
}
