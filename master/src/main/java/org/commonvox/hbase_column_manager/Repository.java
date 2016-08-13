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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Center of all CRUD operations for maintenance and retrieval of HBase schema stored in the
 * ColumnManager Repository table, including metadata pertaining to all
 * <i>Columns</i> actively stored in each <i>Column Family</i> for each included <i>Table</i>.
 *
 * @author Daniel Vimont
 */
class Repository {

  private final Logger logger;
  private static final Logger staticLogger = Logger.getLogger(Repository.class.getName());
  private final byte[] javaUsername;
  private final boolean columnManagerIsActivated;
  private Set<String> includedNamespaces;
  private Set<String> includedEntireNamespaces;
  private Set<TableName> includedTables;
  private Set<String> excludedNamespaces;
  private Set<String> excludedEntireNamespaces;
  private Set<TableName> excludedTables;
  private final Connection hbaseConnection;
  private final Admin standardAdmin;
  private final Table repositoryTable;
  private final Table aliasTable;
  private static final byte[] ALIAS_INCREMENTOR_COLUMN = Bytes.toBytes("#$$#_aliasIncrementor");


  static final String PRODUCT_NAME = "ColumnManagerAPI";
  static final byte[] JAVA_USERNAME_PROPERTY_KEY = Bytes.toBytes("user.name");
  private static final String REPOSITORY_NOT_ACTIVATED_MSG
          = PRODUCT_NAME + " Repository is NOT ACTIVATED.";

  // The following HBASE_CONFIG_PARM* keys & values used in hbase-site.xml
  //   or hbase-column-manager.xml to activate ColumnManager, include Tables for processing, etc.
  static final String HBASE_CONFIG_PARM_KEY_PREFIX = "column_manager.";
  static final String HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED
          = HBASE_CONFIG_PARM_KEY_PREFIX + "activated";
  private static final String HBASE_CONFIG_PARM_VALUE_COLMANAGER_DEACTIVATED = "false"; // default
  private static final String HBASE_CONFIG_PARM_VALUE_COLMANAGER_ACTIVATED = "true";
  static final String HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES
          = HBASE_CONFIG_PARM_KEY_PREFIX + "includedTables";
  static final String HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES
          = HBASE_CONFIG_PARM_KEY_PREFIX + "excludedTables";
  static final String ALL_TABLES_WILDCARD_INDICATOR = ":*";

  private static final int UNIQUE_FOREIGN_KEY_LENGTH = 16;
  private static final NamespaceDescriptor HBASE_SYSTEM_NAMESPACE_DESCRIPTOR
          = NamespaceDescriptor.create("hbase").build();
  static final NamespaceDescriptor REPOSITORY_NAMESPACE_DESCRIPTOR
          = NamespaceDescriptor.create("__column_manager_repository_namespace").build();
  static final TableName REPOSITORY_TABLENAME
          = TableName.valueOf(REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
                  "column_manager_repository_table");
  static final byte[] REPOSITORY_CF = Bytes.toBytes("se"); // ("se"="SchemaEntities")
  static final int DEFAULT_REPOSITORY_MAX_VERSIONS = 50; // should this be set higher?

  static final TableName ALIAS_DIRECTORY_TABLENAME
          = TableName.valueOf(REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
                  "column_manager_alias_directory_table");
  static final byte[] ALIAS_CF = Bytes.toBytes("ca"); // ("ca"="ColumnAliases")

  static final byte[] NAMESPACE_PARENT_FOREIGN_KEY = {'-'};
  static final byte[] HBASE_DEFAULT_NAMESPACE = Bytes.toBytes("default");
  private static final Map<ImmutableBytesWritable, ImmutableBytesWritable> EMPTY_VALUES = new HashMap<>();
  private static final String CONFIG_COLUMN_PREFIX = "Configuration__";
  private static final byte[] CONFIG_COLUMN_PREFIX_BYTES = Bytes.toBytes(CONFIG_COLUMN_PREFIX);
  private static final String VALUE_COLUMN_PREFIX = "Value__";
  private static final byte[] VALUE_COLUMN_PREFIX_BYTES = Bytes.toBytes(VALUE_COLUMN_PREFIX);
  static final String COUNTER_COLUMN_PREFIX = "Counter__";
  static final byte[] COUNTER_COLUMN_PREFIX_BYTES = Bytes.toBytes(COUNTER_COLUMN_PREFIX);
  static final byte[] COL_COUNTER_QUALIFIER = Bytes.toBytes(COUNTER_COLUMN_PREFIX + "column");
  static final byte[] CELL_COUNTER_QUALIFIER = Bytes.toBytes(COUNTER_COLUMN_PREFIX + "cell");
  static final String TIMESTAMP_KEY_PREFIX = "Timestamp__";
  static final byte[] TIMESTAMP_KEY_PREFIX_BYTES = Bytes.toBytes(TIMESTAMP_KEY_PREFIX);
  static final byte[] COL_COUNTER_TIMESTAMP_KEY = Bytes.toBytes(TIMESTAMP_KEY_PREFIX + "column_counter");
  static final byte[] CELL_COUNTER_TIMESTAMP_KEY = Bytes.toBytes(TIMESTAMP_KEY_PREFIX + "cell_counter");
  static final byte[] MAX_VALUE_QUALIFIER
          = ByteBuffer.allocate(VALUE_COLUMN_PREFIX.length() + ColumnAuditor.MAX_VALUE_LENGTH_KEY.length())
          .put(VALUE_COLUMN_PREFIX_BYTES).put(Bytes.toBytes(ColumnAuditor.MAX_VALUE_LENGTH_KEY))
          .array();

  static final String JOB_NAME_CONF_KEY = "mapreduce.job.name";
  static final String MAP_SPECULATIVE_CONF_KEY = "mapreduce.map.speculative";
  static final String COLMANAGER_MAP_CONF_KEY_PREFIX = "colmanager.map.";
  static final String TABLE_NAME_CONF_KEY = COLMANAGER_MAP_CONF_KEY_PREFIX + "source.table";
  static final String COLFAMILY_CONF_KEY = COLMANAGER_MAP_CONF_KEY_PREFIX + "source.colfamily";
  static final String ARG_KEY_PREFIX = "--";
  static final String ARG_DELIMITER = "=";
  static final String TABLE_NAME_ARG_KEY = ARG_KEY_PREFIX + TABLE_NAME_CONF_KEY + ARG_DELIMITER;
  static final String COLFAMILY_ARG_KEY = ARG_KEY_PREFIX + COLFAMILY_CONF_KEY + ARG_DELIMITER;
  static final String INCLUDE_ALL_CELLS_CONF_KEY = "include_all_cells";
  static final String INCLUDE_ALL_CELLS_ARG_KEY
          = ARG_KEY_PREFIX + INCLUDE_ALL_CELLS_CONF_KEY + ARG_DELIMITER;

  private static final String SYNC_ERROR_MSG
          = "SYNCHRONIZATION ERROR FOUND IN " + PRODUCT_NAME + " REPOSITORY. ";
  private static final String SCHEMA_ENTITY_NOT_FOUND_SYNC_ERROR_MSG = SYNC_ERROR_MSG
          + "%s in Repository NOT FOUND in HBase: ";
  static final String NAMESPACE_NOT_FOUND_SYNC_ERROR_MSG = String.format(
          SCHEMA_ENTITY_NOT_FOUND_SYNC_ERROR_MSG, NamespaceDescriptor.class.getSimpleName());
  static final String TABLE_NOT_FOUND_SYNC_ERROR_MSG = String.format(
          SCHEMA_ENTITY_NOT_FOUND_SYNC_ERROR_MSG, HTableDescriptor.class.getSimpleName());
  static final String COLDESCRIPTOR_NOT_FOUND_SYNC_ERROR_MSG = String.format(
          SCHEMA_ENTITY_NOT_FOUND_SYNC_ERROR_MSG, HColumnDescriptor.class.getSimpleName());
  private static final String SCHEMA_ENTITY_ATTRIBUTE_SYNC_ERROR_MSG = SYNC_ERROR_MSG
          + "%s in Repository has attribute-value(s) differing from that in HBase: ";
  static final String NAMESPACE_ATTRIBUTE_SYNC_ERROR_MSG = String.format(
          SCHEMA_ENTITY_ATTRIBUTE_SYNC_ERROR_MSG, NamespaceDescriptor.class.getSimpleName());
  static final String TABLE_ATTRIBUTE_SYNC_ERROR_MSG = String.format(
          SCHEMA_ENTITY_ATTRIBUTE_SYNC_ERROR_MSG, HTableDescriptor.class.getSimpleName());
  static final String COLDESCRIPTOR_ATTRIBUTE_SYNC_ERROR_MSG = String.format(
          SCHEMA_ENTITY_ATTRIBUTE_SYNC_ERROR_MSG, HColumnDescriptor.class.getSimpleName());
  private static final byte[] ENTITY_STATUS_COLUMN = Bytes.toBytes("_Status");
  private static final byte[] ACTIVE_STATUS = Bytes.toBytes("A");
  private static final byte[] DELETED_STATUS = Bytes.toBytes("D");
  static final byte[] FOREIGN_KEY_COLUMN = Bytes.toBytes("_ForeignKey");
  private static final byte[] HEX_00_ARRAY = new byte[16];
  private static final byte[] HEX_FF_ARRAY = new byte[16];

  static {
    Arrays.fill(HEX_FF_ARRAY, (byte) 0xff);
  }

  Repository(Connection hBaseConnection, Object originatingObject) throws IOException {
    logger = Logger.getLogger(this.getClass().getName());
    javaUsername
            = Bytes.toBytes(System.getProperty(Bytes.toString(JAVA_USERNAME_PROPERTY_KEY)));
    this.hbaseConnection = hBaseConnection;
    this.standardAdmin = getNewAdmin(this.hbaseConnection);
    Configuration conf = hbaseConnection.getConfiguration();
    // Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
    String columnManagerActivatedStatus
            = conf.get(HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED,
                    HBASE_CONFIG_PARM_VALUE_COLMANAGER_DEACTIVATED);
    logger.info(PRODUCT_NAME + " Repository instance being instantiated by object of "
            + originatingObject.getClass().getSimpleName() + " class.");
    logger.info(PRODUCT_NAME + " config parameter: " + HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED
            + " = " + columnManagerActivatedStatus);
    if (columnManagerActivatedStatus.equalsIgnoreCase(HBASE_CONFIG_PARM_VALUE_COLMANAGER_ACTIVATED)) {
      columnManagerIsActivated = true;
      logger.info(PRODUCT_NAME + " Repository is ACTIVATED.");
      buildIncludedAndExcludedTablesSets(conf);
      boolean newInstallation = !standardAdmin.tableExists(REPOSITORY_TABLENAME);
      initializeRepositoryNamespace(standardAdmin);
      repositoryTable = initializeRepositoryTable(standardAdmin);
      aliasTable = initializeAliasTable(standardAdmin);
      doSyncCheck();
      if (newInstallation) {
        discoverSchema(false, false, false);
      }
    } else {
//      throw new ColumnManagerIOException(PRODUCT_NAME + " Repository is NOT ACTIVATED.") {};
      columnManagerIsActivated = false;
      repositoryTable = null;
      aliasTable = null;
      logger.info(PRODUCT_NAME + " Repository is NOT ACTIVATED.");
    }
  }

  private void buildIncludedAndExcludedTablesSets(Configuration conf) {
    String[] includedTablesArray
            = conf.getStrings(HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES);
    String[] excludedTablesArray
            = conf.getStrings(HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES);
    if (includedTablesArray != null && excludedTablesArray != null) {
      logger.warn(PRODUCT_NAME + " " + HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES
              + " parameter will be ignored; overridden by "
              + HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES + " parameter.");
    }
    if (includedTablesArray == null) {
      includedTables = null;
      includedNamespaces = null;
      includedEntireNamespaces = null;
      if (excludedTablesArray == null) {
        excludedTables = null;
        excludedNamespaces = null;
        excludedEntireNamespaces = null;
        logger.info(PRODUCT_NAME + " Repository activated for ALL user tables.");
      } else {
        excludedTables = new TreeSet<>();
        excludedNamespaces = new TreeSet<>();
        excludedEntireNamespaces = new TreeSet<>();
        for (String excludedTableString : new TreeSet<>(Arrays.asList(excludedTablesArray))) {
          try {
            TableName excludedTableName = TableName.valueOf(excludedTableString);
            excludedTables.add(excludedTableName);
          } catch (IllegalArgumentException e) {
            if (excludedTableString.endsWith(ALL_TABLES_WILDCARD_INDICATOR)) {
              String excludedNamespaceString
                      = excludedTableString.substring(0, excludedTableString.length() - 2);
              // #isLegalNamespaceName throws IllegalArgumentException if not legal Namespace
              TableName.isLegalNamespaceName(Bytes.toBytes(excludedNamespaceString));
              excludedNamespaces.add(excludedNamespaceString);
              excludedEntireNamespaces.add(excludedNamespaceString);
            } else {
              throw e;
            }
          }
        }
        logger.info(PRODUCT_NAME + " Repository activated for all EXCEPT the following user tables: "
                + conf.get(HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES));
      }
    } else {
      excludedTables = null;
      excludedNamespaces = null;
      excludedEntireNamespaces = null;
      includedTables = new TreeSet<>();
      includedNamespaces = new TreeSet<>();
      includedEntireNamespaces = new TreeSet<>();
      for (String includedTableString : new TreeSet<>(Arrays.asList(includedTablesArray))) {
        try {
          TableName includedTableName = TableName.valueOf(includedTableString);
          includedTables.add(includedTableName);
          includedNamespaces.add(includedTableName.getNamespaceAsString());
        } catch (IllegalArgumentException e) {
          if (includedTableString.endsWith(ALL_TABLES_WILDCARD_INDICATOR)) {
            String includedNamespaceString
                    = includedTableString.substring(0, includedTableString.length() - 2);
            // #isLegalNamespaceName throws IllegalArgumentException if not legal Namespace
            TableName.isLegalNamespaceName(Bytes.toBytes(includedNamespaceString));
            includedNamespaces.add(includedNamespaceString);
            includedEntireNamespaces.add(includedNamespaceString);
          } else {
            throw e;
          }
        }
      }
      logger.info(PRODUCT_NAME + " Repository activated for ONLY the following user tables: "
              + conf.get(HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES));
    }
  }

  Table getRepositoryTable() {
    return repositoryTable;
  }

  static boolean repositoryTableExists(Admin standardAdmin) throws IOException {
    return standardAdmin.tableExists(REPOSITORY_TABLENAME);
  }

  /**
   * Creates repository namespace if it does not already exist.
   *
   * @param hbaseAdmin an Admin object
   * @throws IOException if a remote or network exception occurs
   */
  static void initializeRepositoryNamespace(Admin hbaseAdmin) throws IOException {
    Admin standardAdmin = getStandardAdmin(hbaseAdmin);

    if (namespaceExists(standardAdmin, REPOSITORY_NAMESPACE_DESCRIPTOR)) {
      staticLogger.info("ColumnManager Repository Namespace found: "
              + REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
    } else {
      standardAdmin.createNamespace(REPOSITORY_NAMESPACE_DESCRIPTOR);
      staticLogger.info(
              "ColumnManager Repository Namespace has been created (did not already exist): "
              + REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
    }
  }

  /**
   * Creates repository table if it does not already exist; in any case, the repository table is
   * returned.
   *
   * @param hbaseAdmin an Admin object
   * @return repository table
   * @throws IOException if a remote or network exception occurs
   */
  static Table initializeRepositoryTable(Admin hbaseAdmin) throws IOException {
    Connection standardConnection = getStandardConnection(hbaseAdmin.getConnection());
    Admin standardAdmin = getStandardAdmin(hbaseAdmin);

    try (Table existingRepositoryTable = standardConnection.getTable(REPOSITORY_TABLENAME)) {
      if (standardAdmin.tableExists(existingRepositoryTable.getName())) {
        staticLogger.info("ColumnManager Repository Table found: "
                + REPOSITORY_TABLENAME.getNameAsString());
        return existingRepositoryTable;
      }
    }

    // Create new repositoryTable, since it doesn't already exist
    standardAdmin.createTable(new HTableDescriptor(REPOSITORY_TABLENAME).
            addFamily(new HColumnDescriptor(REPOSITORY_CF).
                    setMaxVersions(DEFAULT_REPOSITORY_MAX_VERSIONS).
                    setInMemory(true)));
    try (Table newRepositoryTable
            = standardConnection.getTable(REPOSITORY_TABLENAME)) {
      staticLogger.info("ColumnManager Repository Table has been created (did not already exist): "
              + REPOSITORY_TABLENAME.getNameAsString());
      return newRepositoryTable;
    }
  }

  /**
   * Creates aliasTable if it does not already exist; in any case, the aliasTable is returned.
   *
   * @param hbaseAdmin an Admin object
   * @return aliasDirectory table
   * @throws IOException if a remote or network exception occurs
   */
  static Table initializeAliasTable(Admin hbaseAdmin) throws IOException {
    Connection standardConnection = getStandardConnection(hbaseAdmin.getConnection());
    Admin standardAdmin = getStandardAdmin(hbaseAdmin);

    try (Table existingAliasDirectoryTable = standardConnection.getTable(ALIAS_DIRECTORY_TABLENAME)) {
      if (standardAdmin.tableExists(existingAliasDirectoryTable.getName())) {
        staticLogger.info("ColumnManager AliasDirectory Table found: "
                + ALIAS_DIRECTORY_TABLENAME.getNameAsString());
        return existingAliasDirectoryTable;
      }
    }

    // Create new AliasDirectory Table, since it doesn't already exist
    standardAdmin.createTable(new HTableDescriptor(ALIAS_DIRECTORY_TABLENAME).
            addFamily(new HColumnDescriptor(ALIAS_CF).setInMemory(true)));
    try (Table newAliasDirectoryTable
            = standardConnection.getTable(ALIAS_DIRECTORY_TABLENAME)) {
      staticLogger.info("ColumnManager AliasDirectory Table has been created (did not already exist): "
              + ALIAS_DIRECTORY_TABLENAME.getNameAsString());
      return newAliasDirectoryTable;
    }
  }

  static void setRepositoryMaxVersions(Admin hbaseAdmin, int maxVersions)
          throws IOException {
    HColumnDescriptor repositoryHcd
            = getStandardAdmin(hbaseAdmin).getTableDescriptor(REPOSITORY_TABLENAME)
            .getFamily(REPOSITORY_CF);
    int oldMaxVersions = repositoryHcd.getMaxVersions();
    if (oldMaxVersions != maxVersions) {
      repositoryHcd.setMaxVersions(maxVersions);
      hbaseAdmin.modifyColumn(REPOSITORY_TABLENAME, repositoryHcd);
      staticLogger.info("ColumnManager Repository Table column-family's <maxVersions> setting has "
              + "been changed from <" + oldMaxVersions + "> to <" + maxVersions + ">.");
    }
  }

  static int getRepositoryMaxVersions(Admin hbaseAdmin)
          throws IOException {
    HColumnDescriptor repositoryHcd
            = getStandardAdmin(hbaseAdmin).
                    getTableDescriptor(REPOSITORY_TABLENAME).getFamily(REPOSITORY_CF);
    return repositoryHcd.getMaxVersions();
  }

  private static Connection getStandardConnection(Connection connection) {
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      return ((MConnection) connection).getStandardConnection();
    } else {
      return connection;
    }
  }

  private static Admin getStandardAdmin(Admin admin) {
    if (MAdmin.class.isAssignableFrom(admin.getClass())) {
      return ((MAdmin) admin).getWrappedAdmin();
    } else {
      return admin;
    }
  }

  private static Admin getNewAdmin(Connection hbaseConnection) throws IOException {
    try (Admin admin = getStandardConnection(hbaseConnection).getAdmin()) {
      return admin;
    }
  }

  Admin getAdmin() {
    return this.standardAdmin;
  }

  boolean isActivated() {
    return columnManagerIsActivated;
  }

  private boolean doSyncCheck() throws IOException {
    boolean syncErrorFound = false;
    for (MNamespaceDescriptor mnd : getMNamespaceDescriptors()) {
      try {
        NamespaceDescriptor nd = standardAdmin.getNamespaceDescriptor(mnd.getNameAsString());
        if (!schemaEntityAttributesInSync(nd.getName(), NAMESPACE_ATTRIBUTE_SYNC_ERROR_MSG,
                mnd.getConfiguration(), nd.getConfiguration(), null, null)) {
          syncErrorFound = true;
        }
      } catch (NamespaceNotFoundException e) {
        logger.warn(NAMESPACE_NOT_FOUND_SYNC_ERROR_MSG + mnd.getNameAsString());
        syncErrorFound = true;
        continue;
      }
      for (MTableDescriptor mtd : getMTableDescriptors(mnd.getForeignKey())) {
        if (!standardAdmin.tableExists(mtd.getTableName())) {
          logger.warn(TABLE_NOT_FOUND_SYNC_ERROR_MSG + mtd.getNameAsString());
          syncErrorFound = true;
          continue;
        }
        HTableDescriptor htd = standardAdmin.getTableDescriptor(mtd.getTableName());
        if (!schemaEntityAttributesInSync(mtd.getTableName().getNameAsString(),
                TABLE_ATTRIBUTE_SYNC_ERROR_MSG,
                mtd.getConfiguration(), htd.getConfiguration(),
                mtd.getValues(), htd.getValues())) {
          syncErrorFound = true;
        }
        Collection<HColumnDescriptor> hcdCollection = htd.getFamilies();
        Set<String> hcdNames = new TreeSet<>();
        for (HColumnDescriptor hcd : hcdCollection) {
          hcdNames.add(hcd.getNameAsString());
        }
        for (MColumnDescriptor mcd : mtd.getMColumnDescriptors()) {
          if (!hcdNames.contains(mcd.getNameAsString())) {
            logger.warn(COLDESCRIPTOR_NOT_FOUND_SYNC_ERROR_MSG + mcd.getNameAsString());
            syncErrorFound = true;
            continue;
          }
          HColumnDescriptor hcd = htd.getFamily(mcd.getName());
          if (!schemaEntityAttributesInSync(mtd.getNameAsString() + ":" + mcd.getNameAsString(),
                  COLDESCRIPTOR_ATTRIBUTE_SYNC_ERROR_MSG,
                  mcd.getConfiguration(), hcd.getConfiguration(),
                  mcd.getValues(), hcd.getValues())) {
            syncErrorFound = true;
          }
        }
      }
    }
    if (syncErrorFound) {
      logger.warn("DISCREPANCIES found between " + PRODUCT_NAME + " repository and schema "
              + "structures in HBase; invocation of RepositoryAdmin#discoverSchema method "
              + "may be required for resynchronization.");
    }
    return syncErrorFound;
  }

  private boolean schemaEntityAttributesInSync(String entityName, String errorMsg,
                  Map<String,String> repositoryConfigurationMap,
                  Map<String,String> hbaseConfigurationMap,
                  Map<ImmutableBytesWritable,ImmutableBytesWritable> repositoryValuesMap,
                  Map<ImmutableBytesWritable,ImmutableBytesWritable> hbaseValuesMap) {

    for (Entry<String,String> configEntry : repositoryConfigurationMap.entrySet()) {
      if (configEntry.getKey().equals(MColumnDescriptor.COL_DEFINITIONS_ENFORCED_KEY)
              || configEntry.getKey().equals(MColumnDescriptor.COL_ALIASES_ENABLED_KEY)) {
        continue;
      }
      String configValue = hbaseConfigurationMap.get(configEntry.getKey());
      if (configValue == null || !configValue.equals(configEntry.getValue())) {
        logger.warn(errorMsg + entityName);
        return false;
      }
    }
    for (Entry<String,String> configEntry : hbaseConfigurationMap.entrySet()) {
      String configValue = repositoryConfigurationMap.get(configEntry.getKey());
      if (configValue == null || !configValue.equals(configEntry.getValue())) {
        logger.warn(errorMsg + entityName);
        return false;
      }
    }
    if (repositoryValuesMap == null || hbaseValuesMap == null) { // Namespace has no values Map!
     return true;
    }
    for (Entry<ImmutableBytesWritable,ImmutableBytesWritable> valueEntry
            : repositoryValuesMap.entrySet()) {
      ImmutableBytesWritable valueEntryValue = hbaseValuesMap.get(valueEntry.getKey());
      if (valueEntryValue == null || !valueEntryValue.equals(valueEntry.getValue())) {
        logger.warn(errorMsg + entityName);
        return false;
      }
    }
    for (Entry<ImmutableBytesWritable,ImmutableBytesWritable> valueEntry
            : hbaseValuesMap.entrySet()) {
      ImmutableBytesWritable valueEntryValue = repositoryValuesMap.get(valueEntry.getKey());
      if (valueEntryValue == null || !valueEntryValue.equals(valueEntry.getValue())) {
        logger.warn(errorMsg + entityName);
        return false;
      }
    }
    return true;
  }

  static boolean namespaceExists(Admin hbaseAdmin, NamespaceDescriptor nd)
          throws IOException {
    try {
      hbaseAdmin.getNamespaceDescriptor(nd.getName());
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }

  static boolean namespaceExists(Admin hbaseAdmin, byte[] namespace)
          throws IOException {
    return namespaceExists(hbaseAdmin,
            NamespaceDescriptor.create(Bytes.toString(namespace)).build());
  }

  private boolean isIncludedNamespace(String namespaceName) {
    if (namespaceName.equals(HBASE_SYSTEM_NAMESPACE_DESCRIPTOR.getName())
            || namespaceName.equals(REPOSITORY_NAMESPACE_DESCRIPTOR.getName())
            || namespaceName.equals(ColumnInvalidityReport.TEMP_REPORT_NAMESPACE)) {
      return false;
    }
    if (includedNamespaces == null) {
      if (excludedNamespaces == null) {
        return true; // if nothing stipulated by administrator, all user namespaces included
      } else {
        return !excludedNamespaces.contains(namespaceName);
      }
    } else {
      return includedNamespaces.contains(namespaceName);
    }
  }

  boolean isIncludedTable(TableName tableName) {
    if (!isIncludedNamespace(tableName.getNamespaceAsString())) {
      return false;
    }
    if (includedTables == null && includedEntireNamespaces == null) {
      if (excludedTables == null && excludedEntireNamespaces == null) {
        return true; // if nothing stipulated by administrator, all user tables included
      } else {
        return excludedTables.contains(tableName) ? false
                : !excludedEntireNamespaces.contains(tableName.getNamespaceAsString());
      }
    } else {
      return includedTables.contains(tableName) ? true
              : includedEntireNamespaces.contains(tableName.getNamespaceAsString());
    }
  }

  private static byte[] generateUniqueForeignKey() {
    UUID uuid = UUID.randomUUID();
    ByteBuffer uniqueID = ByteBuffer.wrap(new byte[UNIQUE_FOREIGN_KEY_LENGTH]);
    uniqueID.putLong(uuid.getMostSignificantBits());
    uniqueID.putLong(uuid.getLeastSignificantBits());
    return uniqueID.array();
  }

  /**
   * Persist Namespace SchemaEntity in Repository
   *
   * @param nd NamespaceDescriptor to persist in SchemaEntity format
   * @return foreign key value of repository row that holds namespace SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  byte[] putNamespaceSchemaEntity(NamespaceDescriptor nd)
          throws IOException {
    if (!isIncludedNamespace(nd.getName())) {
      return null;
    }
    RowId namespaceRowId
            = new RowId(SchemaEntityType.NAMESPACE.getRecordType(), NAMESPACE_PARENT_FOREIGN_KEY,
                    Bytes.toBytes(nd.getName()));
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(EMPTY_VALUES, nd.getConfiguration());
    return putSchemaEntity(namespaceRowId, entityAttributeMap, false);
  }

  /**
   * Persist Table SchemaEntity in Repository
   *
   * @param htd TableDescriptor to persist in SchemaEntity format
   * @return foreign key value of repository row that holds table SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  byte[] putTableSchemaEntity(HTableDescriptor htd) throws IOException {
    if (!isIncludedTable(htd.getTableName())) {
      return null;
    }
    byte[] namespaceForeignKey
            = getNamespaceForeignKey(htd.getTableName().getNamespace());
    RowId tableRowId
            = new RowId(SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey,
                    htd.getTableName().getName());
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(htd.getValues(), htd.getConfiguration());
    byte[] tableForeignKey
            = putSchemaEntity(tableRowId, entityAttributeMap, false);

    // Account for potentially deleted ColumnFamilies
    Set<byte[]> oldMcdNames = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
    for (MColumnDescriptor oldMcd : getMColumnDescriptors(tableForeignKey)) {
      oldMcdNames.add(oldMcd.getName());
    }
    for (HColumnDescriptor newHcd : htd.getColumnFamilies()) {
      oldMcdNames.remove(newHcd.getName());
    }
    for (byte[] deletedMcdName : oldMcdNames) {
      deleteColumnFamily(htd.getTableName(), deletedMcdName);
    }

    // Account for added/modified ColumnFamilies
    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      putColumnFamilySchemaEntity(tableForeignKey, hcd, htd.getTableName());
    }
    return tableForeignKey;
  }

  /**
   * Persist ColumnFamily SchemaEntity in Repository
   *
   * @param tn TableName
   * @param hcd HColumnDescriptor
   * @return foreign key value of repository row that holds Column Family SchemaEntity
   * @throws IOException
   */
  byte[] putColumnFamilySchemaEntity(TableName tn, HColumnDescriptor hcd)
          throws IOException {
    if (!isIncludedTable(tn)) {
      return null;
    }
    byte[] tableForeignKey = getTableForeignKey(tn);
    // Note that ColumnManager can be installed atop an already-existing HBase
    //  installation, so table metadata might not yet have been captured in repository.
    if (tableForeignKey == null) {
      tableForeignKey = putTableSchemaEntity(standardAdmin.getTableDescriptor(tn));
    }
    return putColumnFamilySchemaEntity(tableForeignKey, hcd, tn);
  }

  /**
   * Persist ColumnFamily SchemaEntity in Repository
   *
   * @param tableForeignKey
   * @param hcd
   * @return foreign key value of repository row that holds Column Family SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] putColumnFamilySchemaEntity(
          byte[] tableForeignKey, HColumnDescriptor hcd, TableName tableName)
          throws IOException {
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(hcd.getValues(), hcd.getConfiguration());

    return putSchemaEntity(new RowId(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
                    tableForeignKey, hcd.getName()), entityAttributeMap, false);
  }

  /**
   * Invoked during serialization process, when fully-formed MTableDescriptor is submitted for
   * persistence (e.g., during importation of schema from external source).
   *
   * @param mtd MTableDescriptor
   * @return true if all serializations complete successfully
   * @throws IOException if a remote or network exception occurs
   */
  boolean putColumnAuditorSchemaEntities(MTableDescriptor mtd) throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      return false;
    }
    byte[] tableForeignKey = getTableForeignKey(mtd);
    boolean serializationCompleted = true;
    for (MColumnDescriptor mcd : mtd.getMColumnDescriptorArray()) {
      byte[] colDescForeignKey = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
              tableForeignKey, mcd.getName());
      if (colDescForeignKey == null) {
        serializationCompleted = false;
        continue;
      }
      for (ColumnAuditor columnAuditor : mcd.getColumnAuditors()) {
        byte[] colForeignKey = putColumnAuditorSchemaEntity(colDescForeignKey, columnAuditor);
        if (colForeignKey == null) {
          serializationCompleted = false;
        }
      }
    }
    return serializationCompleted;
  }

  /**
   * Private invocation.
   *
   * @param colFamilyForeignKey
   * @param columnAuditor
   * @return foreign key value of repository row that holds {@link ColumnAuditor} SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] putColumnAuditorSchemaEntity(
          byte[] colFamilyForeignKey, ColumnAuditor columnAuditor)
          throws IOException {
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(columnAuditor.getValues(), columnAuditor.getConfiguration());

    return putSchemaEntity(new RowId(SchemaEntityType.COLUMN_AUDITOR.getRecordType(),
            colFamilyForeignKey, columnAuditor.getName()),
            entityAttributeMap, false);
  }

  /**
   * Invoked by MTableMultiplexer.
   *
   * @param tableName TableName
   * @param mutations List of Mutations
   * @throws IOException if a remote or network exception occurs
   */
  void putColumnAuditorSchemaEntities(TableName tableName, List<? extends Mutation> mutations)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return;
    }
    MTableDescriptor mtd = getMTableDescriptor(tableName);
    for (Mutation mutation : mutations) {
      putColumnAuditorSchemaEntities(mtd, mutation);
    }
  }

  /**
   * Invoked by MTableMultiplexer.
   *
   * @param tableName TableName
   * @param mutation Mutation
   * @throws IOException if a remote or network exception occurs
   */
  void putColumnAuditorSchemaEntities(TableName tableName, Mutation mutation)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return;
    }
    putColumnAuditorSchemaEntities(getMTableDescriptor(tableName), mutation);
  }

  /**
   * Invoked by MTable for real-time audit of mutations.
   *
   * @param mtd MTableDescriptor
   * @param mutations RowMutations
   * @throws IOException if a remote or network exception occurs
   */
  void putColumnAuditorSchemaEntities(MTableDescriptor mtd, RowMutations mutations)
          throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      return;
    }
    for (Mutation mutation : mutations.getMutations()) {
      putColumnAuditorSchemaEntities(mtd, mutation);
    }
  }

  /**
   * Invoked by MBufferedMutator
   *
   * @param mtd MTableDescriptor
   * @param mutations RowMutations
   * @throws IOException if a remote or network exception occurs
   */
  void putColumnAuditorSchemaEntities(MTableDescriptor mtd, List<? extends Mutation> mutations)
          throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      return;
    }
    for (Mutation mutation : mutations) {
      putColumnAuditorSchemaEntities(mtd, mutation);
    }
  }

  /**
   * Invoked at application runtime to persist {@link ColumnAuditor} SchemaEntity in the Repository
   * (invoked after user application successfully invokes a {@code Mutation} to a table).
   *
   * @param mtd ColumnManager TableDescriptor -- deserialized from Repository
   * @param mutation object from which column SchemaEntity is extracted
   * @throws IOException if a remote or network exception occurs
   */
  void putColumnAuditorSchemaEntities(MTableDescriptor mtd, Mutation mutation) throws IOException {
    if (!isIncludedTable(mtd.getTableName())
            // column-cell deletes do not affect Repository
            || Delete.class.isAssignableFrom(mutation.getClass())) {
      return;
    }
    for (Entry<byte[], List<Cell>> colFamilyCellList : mutation.getFamilyCellMap().entrySet()) {
      MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamilyCellList.getKey());
      for (Cell cell : colFamilyCellList.getValue()) {
        byte[] colQualifier = Bytes.copy(
                cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        ColumnAuditor oldColAuditor = getColumnAuditor(mcd.getForeignKey(), colQualifier);
        if (oldColAuditor != null && cell.getValueLength() <= oldColAuditor.getMaxValueLengthFound()) {
          continue;
        }
        ColumnAuditor newColAuditor = new ColumnAuditor(colQualifier);
        if (oldColAuditor == null ||
                cell.getValueLength() > oldColAuditor.getMaxValueLengthFound()) {
          newColAuditor.setMaxValueLengthFound(cell.getValueLength());
        } else {
          newColAuditor.setMaxValueLengthFound(oldColAuditor.getMaxValueLengthFound());
        }
        boolean suppressUserName = (oldColAuditor == null) ? false : true;
        Map<byte[], byte[]> entityAttributeMap
                = buildEntityAttributeMap(newColAuditor.getValues(),
                        newColAuditor.getConfiguration());
        putSchemaEntity(new RowId(SchemaEntityType.COLUMN_AUDITOR.getRecordType(),
                mcd.getForeignKey(), newColAuditor.getName()), entityAttributeMap,
                suppressUserName);
      }
    }
  }

  /**
   * Invoked as part of discovery process. This method also invoked directly from mapreduce
   * discovery.
   *
   * @param mtd table descriptor for parent table of columns found in row
   * @param row Result object from which {@link ColumnAuditor} SchemaEntity is extracted
   * @throws IOException if a remote or network exception occurs
   */
  void putDiscoveredColumnAuditors(
          MTableDescriptor mtd, Result row, boolean keyOnlyFilterUsed) throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      return;
    }
    for (Entry<byte[], NavigableMap<byte[],NavigableMap<Long,byte[]>>> familyToColumnsMapEntry
            : row.getMap().entrySet()) {
      MColumnDescriptor mcd = mtd.getMColumnDescriptor(familyToColumnsMapEntry.getKey());
      for (Entry<byte[],NavigableMap<Long,byte[]>> colEntry
              : familyToColumnsMapEntry.getValue().entrySet()) {
        byte[] colQualifier = colEntry.getKey();
        RowId rowId = new RowId(SchemaEntityType.COLUMN_AUDITOR.getRecordType(),
                mcd.getForeignKey(), colQualifier);
        Set<Entry<Long,byte[]>> cellEntries = colEntry.getValue().entrySet();
        for (Entry<Long,byte[]> cellEntry : cellEntries) {
          int colValueLength;
          if (keyOnlyFilterUsed) {
            colValueLength = Bytes.toInt(cellEntry.getValue()); // value *length* returned as value
          } else {
            colValueLength = cellEntry.getValue().length;
          }
          ColumnAuditor oldColAuditor = getColumnAuditor(mcd.getForeignKey(), colQualifier);
          if (oldColAuditor != null && colValueLength <= oldColAuditor.getMaxValueLengthFound()) {
            continue;
          }
          ColumnAuditor newColAuditor = new ColumnAuditor(colQualifier);
          if (oldColAuditor == null || colValueLength > oldColAuditor.getMaxValueLengthFound()) {
            newColAuditor.setMaxValueLengthFound(colValueLength);
          } else {
            newColAuditor.setMaxValueLengthFound(oldColAuditor.getMaxValueLengthFound());
          }
          boolean suppressUserName = false;
          if (oldColAuditor != null) {
            suppressUserName = true;
          }
          Map<byte[], byte[]> entityAttributeMap
                  = buildEntityAttributeMap(newColAuditor.getValues(),
                          newColAuditor.getConfiguration());
          putSchemaEntity(rowId, entityAttributeMap, suppressUserName);
        }
        repositoryTable.incrementColumnValue(rowId.getByteArray(), REPOSITORY_CF,
                COL_COUNTER_QUALIFIER, 1);
        repositoryTable.incrementColumnValue(rowId.getByteArray(), REPOSITORY_CF,
                CELL_COUNTER_QUALIFIER, cellEntries.size());
      }
    }
  }

  /**
   * Invoked administratively to persist administrator-managed {@link ColumnAuditor}s in
   * Repository.
   *
   * @param tableName name of <i>Table</i> to which {@link ColumnDefinition}s are to be added
   * @param colFamily <i>Column Family</i> to which {@link ColumnDefinition}>s are to be added
   * @param colAuditors List of {@link ColumnAuditor}s to be added or modified
   * @return true if all puts complete successfully
   * @throws IOException if a remote or network exception occurs
   */
  boolean putColumnAuditorSchemaEntities(
          TableName tableName, byte[] colFamily, List<ColumnAuditor> colAuditors)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    boolean allPutsCompleted = false;
    byte[] colFamilyForeignKey = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName),
            colFamily);
    if (colFamilyForeignKey != null) {
      allPutsCompleted = true;
      for (ColumnAuditor colDefinition : colAuditors) {
        byte[] columnForeignKey
                = putColumnAuditorSchemaEntity(colFamilyForeignKey, colDefinition);
        if (columnForeignKey == null) {
          allPutsCompleted = false;
        }
      }
    }
    return allPutsCompleted;
  }

  void validateColumns(MTableDescriptor mtd, Mutation mutation) throws IOException {
    if (!isIncludedTable(mtd.getTableName())
            || !mtd.hasColDescriptorWithColDefinitionsEnforced()
            || Delete.class.isAssignableFrom(mutation.getClass())) { // Deletes not validated
      return;
    }
    for (Entry<byte[], List<Cell>> colFamilyCellList : mutation.getFamilyCellMap().entrySet()) {
      MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamilyCellList.getKey());
      if (!mcd.columnDefinitionsEnforced()) {
        continue;
      }
      for (Cell cell : colFamilyCellList.getValue()) {
        byte[] colQualifier = Bytes.copy(
                cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        ColumnDefinition colDefinition = mcd.getColumnDefinition(colQualifier);
        if (colDefinition == null) {
          throw new ColumnDefinitionNotFoundException(mtd.getTableName().getName(),
                  mcd.getName(), colQualifier, null);
        }
        if (colDefinition.getColumnLength() > 0
                && cell.getValueLength() > colDefinition.getColumnLength()) {
          throw new ColumnValueInvalidException(
                  mtd.getTableName().getName(), mcd.getName(), colQualifier, null,
                  "Value length of <" + cell.getValueLength()
                          + "> is longer than maximum length of <"
                          + colDefinition.getColumnLength() + "> defined for the column in its "
                          + "corresponding ColumnDefinition.");
        }
        String colValidationRegex = colDefinition.getColumnValidationRegex();
        if (colValidationRegex != null && colValidationRegex.length() > 0) {
          byte[] colValue = Bytes.getBytes(CellUtil.getValueBufferShallowCopy(cell));
          if (!Bytes.toString(colValue).matches(colValidationRegex)) {
            throw new ColumnValueInvalidException(mtd.getTableName().getName(), mcd.getName(),
                    colQualifier, colValue,
                    "Value does not match the regular expression defined for the column in its "
                            + "corresponding ColumnDefinition: <" + colValidationRegex + ">");
          }
        }
      }
    }
  }

  void validateColumns(MTableDescriptor mtd, RowMutations mutations) throws IOException {
    if (isIncludedTable(mtd.getTableName())
            && mtd.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Mutation mutation : mutations.getMutations()) {
        validateColumns(mtd, mutation);
      }
    }
  }

  void validateColumns(MTableDescriptor mtd, List<? extends Mutation> mutations)
          throws IOException {
    if (isIncludedTable(mtd.getTableName())
            && mtd.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Mutation mutation : mutations) {
        validateColumns(mtd, mutation);
      }
    }
  }

  void validateColumns(TableName tableName, Mutation mutation)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return;
    }
    MTableDescriptor mtd = getMTableDescriptor(tableName);
    if (mtd != null && mtd.hasColDescriptorWithColDefinitionsEnforced()) {
      validateColumns(mtd, mutation);
    }
  }

  void validateColumns(TableName tableName, List<? extends Mutation> mutations)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return;
    }
    MTableDescriptor mtd = getMTableDescriptor(tableName);
    if (mtd != null && mtd.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Mutation mutation : mutations) {
        validateColumns(mtd, mutation);
      }
    }
  }

  /**
   * Invoked during importation (from external source) process, when fully-formed MTableDescriptor
   * is submitted for persistence.
   *
   * @param mtd MTableDescriptor
   * @return true if all serializations complete successfully
   * @throws IOException if a remote or network exception occurs
   */
  private boolean putColumnDefinitionSchemaEntities(MTableDescriptor mtd) throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      throw new TableNotIncludedForProcessingException(mtd.getTableName().getName(), null);
    }
    byte[] tableForeignKey = getTableForeignKey(mtd);
    boolean serializationCompleted = true;
    for (MColumnDescriptor mcd : mtd.getMColumnDescriptorArray()) {
      byte[] colDescForeignKey = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
              tableForeignKey, mcd.getName());
      if (colDescForeignKey == null) {
        serializationCompleted = false;
        continue;
      }
      for (ColumnDefinition colDef : mcd.getColumnDefinitions()) {
        byte[] colDefinitionForeignKey
                = putColumnDefinitionSchemaEntity(colDescForeignKey, colDef);
        if (colDefinitionForeignKey == null) {
          serializationCompleted = false;
        }
      }
    }
    return serializationCompleted;
  }

  /**
   * Invoked administratively to persist administrator-managed {@link ColumnDefinition}s in
   * Repository.
   *
   * @param tableName name of <i>Table</i> to which {@link ColumnDefinition}s are to be added
   * @param colFamily <i>Column Family</i> to which {@link ColumnDefinition}>s are to be added
   * @param colDefinitions List of {@link ColumnDefinition}s to be added or modified
   * @return true if all puts complete successfully
   * @throws IOException if a remote or network exception occurs
   */
  boolean putColumnDefinitionSchemaEntities(
          TableName tableName, byte[] colFamily, List<ColumnDefinition> colDefinitions)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    boolean allPutsCompleted = false;
    byte[] colFamilyForeignKey = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName),
            colFamily);
    if (colFamilyForeignKey != null) {
      allPutsCompleted = true;
      for (ColumnDefinition colDefinition : colDefinitions) {
        byte[] columnForeignKey
                = putColumnDefinitionSchemaEntity(colFamilyForeignKey, colDefinition);
        if (columnForeignKey == null) {
          allPutsCompleted = false;
        }
      }
    }
    return allPutsCompleted;
  }

  /**
   * Private invocation.
   *
   * @param colFamilyForeignKey
   * @param colDef
   * @return foreign key value of repository row that holds {@link ColumnDefinition} SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] putColumnDefinitionSchemaEntity(
          byte[] colFamilyForeignKey, ColumnDefinition colDef)
          throws IOException {
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(colDef.getValues(), colDef.getConfiguration());

    return putSchemaEntity(new RowId(SchemaEntityType.COLUMN_DEFINITION.getRecordType(),
            colFamilyForeignKey, colDef.getName()), entityAttributeMap, false);
  }

  private Map<byte[], byte[]> buildEntityAttributeMap(
          Map<ImmutableBytesWritable, ImmutableBytesWritable> values,
          Map<String, String> configuration) {
    Map<byte[], byte[]> entityAttributeMap = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    for (Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry : values.entrySet()) {
      byte[] attributeKeySuffix = valueEntry.getKey().get();
      byte[] attributeValue = valueEntry.getValue().get();
      if (attributeKeySuffix.length > COUNTER_COLUMN_PREFIX_BYTES.length
              && Bytes.startsWith(attributeKeySuffix, COUNTER_COLUMN_PREFIX_BYTES)) {
        continue; // bypass all counters
      }
      if (attributeKeySuffix.length > TIMESTAMP_KEY_PREFIX_BYTES.length
              && Bytes.startsWith(attributeKeySuffix, TIMESTAMP_KEY_PREFIX_BYTES)) {
        continue; // bypass all timestamps
      }
      ByteBuffer attributeKey
              = ByteBuffer.allocate(VALUE_COLUMN_PREFIX_BYTES.length + attributeKeySuffix.length);
      attributeKey.put(VALUE_COLUMN_PREFIX_BYTES).put(attributeKeySuffix);
      entityAttributeMap.put(Bytes.toBytes(attributeKey), attributeValue);
    }
    for (Entry<String, String> configEntry : configuration.entrySet()) {
      entityAttributeMap.put(Bytes.toBytes(CONFIG_COLUMN_PREFIX + configEntry.getKey()),
              Bytes.toBytes(configEntry.getValue()));
    }

    return entityAttributeMap;
  }

  private byte[] putSchemaEntity(
          RowId rowId, Map<byte[], byte[]> entityAttributeMap, boolean suppressUserName)
          throws IOException {
    Result oldRow = repositoryTable.get(new Get(rowId.getByteArray()));
    Put newRow = new Put(rowId.getByteArray());
    Map<byte[], byte[]> oldEntityAttributeMap;

    // ADD Columns to newRow to set foreignKey and entityStatus values appropriately
    byte[] foreignKey;
    if (oldRow.isEmpty()) {
      oldEntityAttributeMap = null;
      foreignKey = generateUniqueForeignKey(); // note that foreignKey ignored in column-entities
      newRow.addColumn(REPOSITORY_CF, FOREIGN_KEY_COLUMN, foreignKey);
      newRow.addColumn(REPOSITORY_CF, ENTITY_STATUS_COLUMN, ACTIVE_STATUS);
    } else {
      SchemaEntity entity = deserializeSchemaEntity(oldRow);
      oldEntityAttributeMap
              = buildEntityAttributeMap(entity.getValues(), entity.getConfiguration());
      foreignKey = oldRow.getValue(REPOSITORY_CF, FOREIGN_KEY_COLUMN);
      if (!Bytes.equals(oldRow.getValue(REPOSITORY_CF, ENTITY_STATUS_COLUMN), ACTIVE_STATUS)) {
        newRow.addColumn(REPOSITORY_CF, ENTITY_STATUS_COLUMN, ACTIVE_STATUS);
      }
    }

    // ADD Columns to newRow based on changes in attributeValues
    for (Entry<byte[], byte[]> newAttribute : entityAttributeMap.entrySet()) {
      byte[] newAttributeKey = newAttribute.getKey();
      byte[] newAttributeValue = newAttribute.getValue();
      byte[] oldAttributeValue;
      if (oldEntityAttributeMap == null) {
        oldAttributeValue = null;
      } else {
        oldAttributeValue = oldEntityAttributeMap.get(newAttributeKey);
      }
      boolean attributeValueChanged = false;
      if (oldAttributeValue == null) {
        if (newAttributeValue != null) {
          attributeValueChanged = true;
        }
      } else {
        if (!Bytes.equals(oldAttributeValue, newAttributeValue)) {
          attributeValueChanged = true;
        }
      }
      if (attributeValueChanged) {
        if (newAttributeValue == null) {
          newRow.addColumn(REPOSITORY_CF, newAttributeKey, null);
        } else {
          newRow.addColumn(REPOSITORY_CF, newAttributeKey, newAttributeValue);
        }
      }
    }

    // ADD Columns to newRow to nullify value if attribute is in old map but NOT in new map
    if (oldEntityAttributeMap != null) {
      for (byte[] oldAttributeKey : oldEntityAttributeMap.keySet()) {
        if (!entityAttributeMap.containsKey(oldAttributeKey)) {
          newRow.addColumn(REPOSITORY_CF, oldAttributeKey, null);
        }
      }
    }

    // PUT newRow to Repository
    if (!newRow.isEmpty()) {
      if (!suppressUserName) {
        newRow.addColumn(REPOSITORY_CF, JAVA_USERNAME_PROPERTY_KEY, javaUsername);
      }
      if (rowId.entityType == SchemaEntityType.COLUMN_AUDITOR.getRecordType()) {
        List<Cell> maxValueLengthCells
                = newRow.get(REPOSITORY_CF, ColumnAuditor.MAX_VALUE_LENGTH_KEY_BYTES);
        if (maxValueLengthCells == null || maxValueLengthCells.size() == 0) {
          repositoryTable.put(newRow);
        } else {
          // #checkAndPut to prevent bogus overlay of maxValueLength when submitted via mapReduce
          repositoryTable.checkAndPut(rowId.getByteArray(), REPOSITORY_CF,
                  ColumnAuditor.MAX_VALUE_LENGTH_KEY_BYTES,
                  CompareFilter.CompareOp.LESS,
                  maxValueLengthCells.get(0).getValueArray(),
                  newRow);
        }

      } else {
        repositoryTable.put(newRow);
      }
    }
    return foreignKey;
  }

  MNamespaceDescriptor getMNamespaceDescriptor(String namespaceName)
          throws IOException {
    Result row = getActiveRow(SchemaEntityType.NAMESPACE.getRecordType(),
            NAMESPACE_PARENT_FOREIGN_KEY, Bytes.toBytes(namespaceName), null);
    if (row == null || row.isEmpty()) {
      // Note that ColumnManager can be installed atop an already-existing HBase
      //  installation, so namespace SchemaEntity might not yet have been captured in repository,
      //  or namespaceName may not represent included namespace (and so not stored in repository).
      if (isIncludedNamespace(namespaceName)) {
        putNamespaceSchemaEntity(standardAdmin.getNamespaceDescriptor(namespaceName));
        row = getActiveRow(SchemaEntityType.NAMESPACE.getRecordType(),
                NAMESPACE_PARENT_FOREIGN_KEY, Bytes.toBytes(namespaceName), null);
      } else {
        return null;
      }
    }
    MNamespaceDescriptor nd = new MNamespaceDescriptor(deserializeSchemaEntity(row));
    return nd;
  }

  Set<MNamespaceDescriptor> getMNamespaceDescriptors() throws IOException {
    Set<MNamespaceDescriptor> mNamespaceDescriptors = new TreeSet<>();
    for (Result row : getActiveRows(
            SchemaEntityType.NAMESPACE.getRecordType(), NAMESPACE_PARENT_FOREIGN_KEY)) {
      mNamespaceDescriptors.add(new MNamespaceDescriptor(deserializeSchemaEntity(row)));
    }
    return mNamespaceDescriptors;
  }

  MTableDescriptor getMTableDescriptor(TableName tn) throws IOException {
    byte[] namespaceForeignKey = getNamespaceForeignKey(tn.getNamespace());
    Result row = getActiveRow(
            SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tn.getName(), null);
    if (row == null || row.isEmpty()) {
      // Note that ColumnManager can be installed atop an already-existing HBase
      //  installation, so table SchemaEntity might not yet have been captured in repository,
      //  or TableName may not represent included Table (and so not stored in repository).
      if (isIncludedTable(tn)) {
        putTableSchemaEntity(standardAdmin.getTableDescriptor(tn));
        row = getActiveRow(
                SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tn.getName(), null);
      } else {
        return null;
      }
    }
    MTableDescriptor mtd = new MTableDescriptor(deserializeSchemaEntity(row));
    for (MColumnDescriptor mcd : getMColumnDescriptors(mtd.getForeignKey())) {
      mtd.addFamily(mcd);
    }
    return mtd;
  }

  Set<MTableDescriptor> getMTableDescriptors(byte[] namespaceForeignKey)
          throws IOException {
    Set<MTableDescriptor> mTableDescriptors = new TreeSet<>();
    for (Result row : getActiveRows(SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey)) {
      MTableDescriptor mtd = new MTableDescriptor(deserializeSchemaEntity(row));
      for (MColumnDescriptor mcd : getMColumnDescriptors(mtd.getForeignKey())) {
        mtd.addFamily(mcd);
      }
      mTableDescriptors.add(mtd);
    }
    return mTableDescriptors;
  }

  private Set<MColumnDescriptor> getMColumnDescriptors(byte[] tableForeignKey)
          throws IOException {
    Set<MColumnDescriptor> mColumnDescriptors = new TreeSet<>();
    for (Result row : getActiveRows(
            SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey)) {
      MColumnDescriptor mcd = new MColumnDescriptor(deserializeSchemaEntity(row));
      mColumnDescriptors.add(mcd.addColumnAuditors(getColumnAuditors(mcd.getForeignKey()))
              .addColumnDefinitions(getColumnDefinitions(mcd.getForeignKey())));
    }
    return mColumnDescriptors;
  }

  private MColumnDescriptor getMColumnDescriptor(byte[] tableForeignKey, byte[] colFamily)
          throws IOException {
    Result row = getActiveRow(
            SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey, colFamily, null);
    return row == null ? null : new MColumnDescriptor(deserializeSchemaEntity(row));
  }

  Set<ColumnAuditor> getColumnAuditors(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
    if (!isIncludedTable(htd.getTableName())) {
      throw new TableNotIncludedForProcessingException(htd.getTableName().getName(), null);
    }
    byte[] colFamilyForeignKey
            = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
                    getTableForeignKey(htd), hcd.getName());
    return (colFamilyForeignKey == null) ? null : getColumnAuditors(colFamilyForeignKey);
  }

  private Set<ColumnAuditor> getColumnAuditors(byte[] colFamilyForeignKey)
          throws IOException {
    Set<ColumnAuditor> columnAuditors = new TreeSet<>();
    Result[] colAuditorRows = getActiveRows(
            SchemaEntityType.COLUMN_AUDITOR.getRecordType(), colFamilyForeignKey);
    if (colAuditorRows != null) {
      for (Result row : colAuditorRows) {
        columnAuditors.add(new ColumnAuditor(deserializeSchemaEntity(row)));
      }
    }
    return columnAuditors;
  }

  private ColumnAuditor getColumnAuditor(byte[] colFamilyForeignKey, byte[] colQualifier)
          throws IOException {
    Result row = getActiveRow(SchemaEntityType.COLUMN_AUDITOR.getRecordType(),
            colFamilyForeignKey, colQualifier, null);
    return (row == null) ? null : new ColumnAuditor(deserializeSchemaEntity(row));
  }

  Set<ColumnDefinition> getColumnDefinitions(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
    if (!isIncludedTable(htd.getTableName())) {
      throw new TableNotIncludedForProcessingException(htd.getTableName().getName(), null);
    }
    byte[] colFamilyForeignKey = getForeignKey(
            SchemaEntityType.COLUMN_FAMILY.getRecordType(), getTableForeignKey(htd), hcd.getName());
    return getColumnDefinitions(colFamilyForeignKey);
  }

  private Set<ColumnDefinition> getColumnDefinitions(byte[] colFamilyForeignKey)
          throws IOException {
    Set<ColumnDefinition> columnDefinitions = new TreeSet<>();
    for (Result row : getActiveRows(
            SchemaEntityType.COLUMN_DEFINITION.getRecordType(), colFamilyForeignKey)) {
      columnDefinitions.add(new ColumnDefinition(deserializeSchemaEntity(row)));
    }
    return columnDefinitions;
  }

  private ColumnDefinition getColumnDefinition(byte[] colFamilyForeignKey, byte[] colQualifier)
          throws IOException {
    Result row = getActiveRow(SchemaEntityType.COLUMN_DEFINITION.getRecordType(),
            colFamilyForeignKey, colQualifier, null);
    return (row == null) ? null : new ColumnDefinition(deserializeSchemaEntity(row));
  }

  private SchemaEntity deserializeSchemaEntity(Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    }
    RowId rowId = new RowId(row.getRow());
    SchemaEntity entity = new SchemaEntity(rowId.getEntityType(), rowId.getEntityName());
    // full #getMap required to extract timestamps of counter columns
    for (Entry<byte[], NavigableMap<byte[],NavigableMap<Long,byte[]>>> familyToCellsMapEntry
            : row.getMap().entrySet()) {
      for (Entry<byte[],NavigableMap<Long,byte[]>> colEntry
              : familyToCellsMapEntry.getValue().entrySet()) {
        byte[] key = colEntry.getKey();
        for (Entry<Long,byte[]> cellEntry : colEntry.getValue().entrySet()) {
          byte[] value = cellEntry.getValue();
          if (Bytes.equals(key, FOREIGN_KEY_COLUMN)) {
            entity.setForeignKey(cellEntry.getValue());
          } else if (key.length > VALUE_COLUMN_PREFIX_BYTES.length
                  && Bytes.startsWith(key, VALUE_COLUMN_PREFIX_BYTES)) {
            entity.setValue(Bytes.tail(key, key.length - VALUE_COLUMN_PREFIX_BYTES.length),
                    value);
          } else if (key.length > COUNTER_COLUMN_PREFIX_BYTES.length
                  && Bytes.startsWith(key, COUNTER_COLUMN_PREFIX_BYTES)) {
            entity.setValue(key, value);
            if (Bytes.equals(key, COL_COUNTER_QUALIFIER)) {
              entity.setValue(COL_COUNTER_TIMESTAMP_KEY, Bytes.toBytes(cellEntry.getKey()));
            } else if (Bytes.equals(key, CELL_COUNTER_QUALIFIER)) {
              entity.setValue(CELL_COUNTER_TIMESTAMP_KEY, Bytes.toBytes(cellEntry.getKey()));
            }
          } else if (key.length > CONFIG_COLUMN_PREFIX_BYTES.length
                  && Bytes.startsWith(key, CONFIG_COLUMN_PREFIX_BYTES)) {
            entity.setConfiguration(
                    Bytes.toString(Bytes.tail(key, key.length - CONFIG_COLUMN_PREFIX_BYTES.length)),
                    Bytes.toString(cellEntry.getValue()));
          }
          break;
        }
      }
    }
    return entity;
  }

  private Result getActiveRow(byte recordType, byte[] parentForeignKey, byte[] entityName,
          byte[] columnToGet)
          throws IOException {
    Result[] rows = getActiveRows(false, recordType, parentForeignKey, entityName, columnToGet);
    return (rows == null || rows.length == 0) ? null : rows[0];
  }

  private Result[] getActiveRows(byte recordType, byte[] parentForeignKey)
          throws IOException {
    return getActiveRows(false, recordType, parentForeignKey, null, null);
  }

  private Result[] getActiveRows(boolean getRowIdAndStatusOnly, byte recordType,
          byte[] parentForeignKey, byte[] entityName, byte[] columnToGet)
          throws IOException {
    SingleColumnValueFilter activeRowsOnlyFilter = new SingleColumnValueFilter(
            REPOSITORY_CF, ENTITY_STATUS_COLUMN, CompareFilter.CompareOp.EQUAL, ACTIVE_STATUS);
    activeRowsOnlyFilter.setFilterIfMissing(true);
    return getRepositoryRows(getRowIdAndStatusOnly, recordType, parentForeignKey, entityName,
            columnToGet, activeRowsOnlyFilter);
  }

  private Result[] getRepositoryRows(byte recordType, byte[] parentForeignKey, byte[] columnToGet)
          throws IOException {
    return getRepositoryRows(false, recordType, parentForeignKey, null, columnToGet, null);
  }

  private Result[] getRepositoryRows(boolean getRowIdAndStatusOnly, byte recordType,
          byte[] parentForeignKey, byte[] entityName, byte[] columnToGet, Filter filter)
          throws IOException {
    if (parentForeignKey == null) {
      return null;
    }
    RowId startRowId = new RowId(recordType, parentForeignKey, entityName);
    byte[] stopRowId = startRowId.getStopRowIdByteArray();
    Scan scanParms = new Scan(startRowId.getByteArray(), stopRowId);
    if (getRowIdAndStatusOnly || columnToGet != null) {
      scanParms.addColumn(REPOSITORY_CF, FOREIGN_KEY_COLUMN);
      scanParms.addColumn(REPOSITORY_CF, ENTITY_STATUS_COLUMN);
      if (columnToGet != null) {
        scanParms.addColumn(REPOSITORY_CF, columnToGet);
      }
    }
    if (filter != null) {
      scanParms.setFilter(filter);
    }
    List<Result> rows = new ArrayList<>();
    try (ResultScanner results = repositoryTable.getScanner(scanParms)) {
      for (Result row : results) {
        rows.add(row);
      }
    }
    return rows.toArray(new Result[rows.size()]);
  }

  /**
   * Returns foreign key via lookup on Repository Table. The three parameters concatenated together
   * comprise the row's unique RowId.
   *
   * @param recordType
   * @param parentForeignKey
   * @param entityName
   * @return Entity's foreign key
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] getForeignKey(byte recordType, byte[] parentForeignKey, byte[] entityName)
          throws IOException {
    if (parentForeignKey == null || entityName == null) {
      return null;
    }
    Result row = repositoryTable.get(
            new Get(new RowId(recordType, parentForeignKey, entityName).getByteArray()));
    return row.isEmpty() ? null : row.getValue(REPOSITORY_CF, FOREIGN_KEY_COLUMN);
  }

  private byte[] getNamespaceForeignKey(byte[] namespace) throws IOException {
    byte[] namespaceForeignKey
            = getForeignKey(SchemaEntityType.NAMESPACE.getRecordType(),
                    NAMESPACE_PARENT_FOREIGN_KEY, namespace);
    // Note that ColumnManager could be installed atop an already-existing HBase
    //  installation, so namespace SchemaEntity might not be in repository the first
    //  time its foreign key is accessed or one of its descendents is modified.
    if (namespaceForeignKey == null) {
      if (namespaceExists(standardAdmin, namespace)) {
        namespaceForeignKey = putNamespaceSchemaEntity(
                standardAdmin.getNamespaceDescriptor(Bytes.toString(namespace)));
      }
    }
    return namespaceForeignKey;
  }

  /**
   * Returns Table's foreign key via lookup on Repository Table.
   *
   * @param tableName
   * @return Table's foreign key value
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] getTableForeignKey(TableName tableName) throws IOException {
    byte[] namespaceForeignKey = getNamespaceForeignKey(tableName.getNamespace());
    byte[] tableForeignKey
            = getForeignKey(SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tableName.getName());
    // Note that ColumnManager could be installed atop an already-existing HBase
    //  installation, so table SchemaEntity might not be in repository the first
    //  time its foreign key is accessed or one of its descendents is modified.
    if (tableForeignKey == null) {
      if (standardAdmin.tableExists(tableName)) {
        tableForeignKey = putTableSchemaEntity(standardAdmin.getTableDescriptor(tableName));
      }
    }
    return tableForeignKey;
  }

  /**
   * Returns Table's foreign key via lookup on Repository Table.
   *
   * @param table
   * @return Table's foreign key value
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] getTableForeignKey(Table table) throws IOException {
    return getTableForeignKey(table.getName());
  }

  /**
   * Returns Table's foreign key via lookup on Repository Table.
   *
   * @param htd HTableDescriptor of table
   * @return Table's foreign key value
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] getTableForeignKey(HTableDescriptor htd) throws IOException {
    return getTableForeignKey(htd.getTableName());
  }

  boolean columnDefinitionsEnforced(TableName tableName, byte[] colFamily)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    MColumnDescriptor mcd = getMColumnDescriptor(getTableForeignKey(tableName), colFamily);
    return (mcd == null) ? false : mcd.columnDefinitionsEnforced();
  }

  void enableColumnDefinitionEnforcement(boolean enabled, TableName tableName, byte[] colFamily)
          throws IOException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    byte[] tableForeignKey = getTableForeignKey(tableName);
    MColumnDescriptor mcd = getMColumnDescriptor(tableForeignKey, colFamily);
    if (mcd == null) {
      return;
    }
    if (mcd.columnDefinitionsEnforced() != enabled) {
      mcd.enableColumnDefinitionEnforcement(enabled);
      putColumnFamilySchemaEntity(tableForeignKey, mcd, tableName);
    }
  }

  void enableColumnAliases(boolean enabled, TableName tableName, byte[] colFamily)
          throws IOException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    byte[] tableForeignKey = getTableForeignKey(tableName);
    MColumnDescriptor mcd = getMColumnDescriptor(tableForeignKey, colFamily);
    if (mcd == null) {
      return;
    }
    if (mcd.columnAliasesEnabled() != enabled) {
      mcd.enableColumnAliases(enabled);
      putColumnFamilySchemaEntity(tableForeignKey, mcd, tableName);
    }
  }


  private String buildOrderedCommaDelimitedString(List<String> list) {
    Set<String> set = new TreeSet<>(list);
    StringBuilder stringBuilder = new StringBuilder();
    int itemCount = 0;
    for (String item : set) {
      stringBuilder.append(item);
      if (++itemCount < set.size()) {
        stringBuilder.append(',');
      }
    }
    return stringBuilder.toString();
  }

  void purgeNamespaceShemaEntity(String name) throws IOException {
    deleteNamespaceSchemaEntity(true, name);
  }

  void deleteNamespaceSchemaEntity(String name) throws IOException {
    deleteNamespaceSchemaEntity(false, name);
  }

  private void deleteNamespaceSchemaEntity(boolean purge, String name) throws IOException {
    deleteSchemaEntity(purge, false, SchemaEntityType.NAMESPACE.getRecordType(),
            NAMESPACE_PARENT_FOREIGN_KEY, Bytes.toBytes(name));
  }

  void purgeTableSchemaEntity(TableName tableName) throws IOException {
    deleteTableSchemaEntity(true, false, tableName);
  }

  void truncateTableColumns(TableName tableName) throws IOException {
    deleteTableSchemaEntity(false, true, tableName);
  }

  void deleteTableSchemaEntity(TableName tableName) throws IOException {
    deleteTableSchemaEntity(false, false, tableName);
  }

  private void deleteTableSchemaEntity(boolean purge, boolean truncateColumns,
          TableName tableName) throws IOException {
    byte[] namespaceForeignKey = getNamespaceForeignKey(tableName.getNamespace());
    deleteSchemaEntity(purge, truncateColumns, SchemaEntityType.TABLE.getRecordType(),
            namespaceForeignKey, tableName.getName());
  }

  void deleteColumnFamily(TableName tableName, byte[] name) throws IOException {
    deleteSchemaEntity(false, false, SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName), name);
  }

  /**
   * Used only in administrative deletion of {@link ColumnDefinition}
   *
   * @param tableName name of <i>Table</i> from which {@link ColumnDefinition} is to be deleted
   * @param colFamily <i>Column Family</i> from which {@link ColumnDefinition} is to be deleted
   * @param colQualifier colQualifier that identifies the {@link ColumnDefinition} to be deleted
   * @throws IOException
   */
  void deleteColumnDefinition(TableName tableName, byte[] colFamily, byte[] colQualifier)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    byte[] colFamilyForeignKey = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName),
            colFamily);
    if (colFamilyForeignKey == null) {
      return;
    }
    // TO DO?: before (or as part of) conceptual deletion, reset
    //   ColumnDefinition's validation-related attributes
    deleteSchemaEntity(false, false, SchemaEntityType.COLUMN_DEFINITION.getRecordType(),
            colFamilyForeignKey, colQualifier);
  }

  private void deleteSchemaEntity(boolean purge, boolean truncateColumns, byte recordType,
          byte[] parentForeignKey, byte[] entityName)
          throws IOException {
    if (parentForeignKey == null) {
      return;
    }
    for (Result row :
            getRepositoryRows(true, recordType, parentForeignKey, entityName, null, null)) {
      if (!truncateColumns || (truncateColumns &&
              recordType == SchemaEntityType.COLUMN_AUDITOR.getRecordType())) {
        if (purge) {
          repositoryTable.delete(new Delete(row.getRow()));
        } else {
          if (!Bytes.equals(row.getValue(REPOSITORY_CF, ENTITY_STATUS_COLUMN), DELETED_STATUS)) {
            repositoryTable.put(new Put(row.getRow())
                    .addColumn(REPOSITORY_CF, ENTITY_STATUS_COLUMN, DELETED_STATUS)
                    .addColumn(REPOSITORY_CF, JAVA_USERNAME_PROPERTY_KEY, javaUsername));
          }
        }
      }

      // cascade to child entities
      byte childRecordType;
      switch (SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP.get(recordType)) {
        case NAMESPACE:
          childRecordType = SchemaEntityType.TABLE.getRecordType();
          break;
        case TABLE:
          childRecordType = SchemaEntityType.COLUMN_FAMILY.getRecordType();
          break;
        case COLUMN_FAMILY:
          childRecordType = SchemaEntityType.COLUMN_AUDITOR.getRecordType();
          break;
        case COLUMN_AUDITOR: // ColumnAuditors and ColumnDefinitions have no children!!
        case COLUMN_DEFINITION:
        default:
          continue;
      }
      deleteSchemaEntity(purge, truncateColumns, childRecordType,
              row.getValue(REPOSITORY_CF, FOREIGN_KEY_COLUMN), null);
      if (childRecordType == SchemaEntityType.COLUMN_AUDITOR.getRecordType()) {
        deleteSchemaEntity(purge, truncateColumns,
                SchemaEntityType.COLUMN_DEFINITION.getRecordType(),
                row.getValue(REPOSITORY_CF, FOREIGN_KEY_COLUMN), null);
      }
    }
  }

  final void discoverSchema(
          boolean includeColumnQualifiers, boolean includeAllCells, boolean useMapReduce)
          throws IOException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    for (NamespaceDescriptor nd : standardAdmin.listNamespaceDescriptors()) {
      if (!isIncludedNamespace(nd.getName())) {
        continue;
      }
      putNamespaceSchemaEntity(nd);
      for (HTableDescriptor htd : standardAdmin.listTableDescriptorsByNamespace(nd.getName())) {
        if (!isIncludedTable(htd.getTableName())
                || standardAdmin.isTableDisabled(htd.getTableName())) {
          continue;
        }
        discoverSchema(htd.getTableName(), includeColumnQualifiers, includeAllCells, useMapReduce);
      }
    }
  }

  final void discoverSchema(String namespace,
          boolean includeColumnQualifiers, boolean includeAllCells, boolean useMapReduce)
          throws IOException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    NamespaceDescriptor nd = getAdmin().getNamespaceDescriptor(namespace); // Exception if not found
    if (!isIncludedNamespace(namespace)) {
      throw new TableNotIncludedForProcessingException(
              Bytes.toBytes(namespace + ALL_TABLES_WILDCARD_INDICATOR),
              "NO table from namespace <" + namespace + "> is included for "
                      + PRODUCT_NAME + " processing.");
    }
    putNamespaceSchemaEntity(nd);
    for (HTableDescriptor htd : standardAdmin.listTableDescriptorsByNamespace(nd.getName())) {
      if (!isIncludedTable(htd.getTableName())
              || standardAdmin.isTableDisabled(htd.getTableName())) {
        continue;
      }
      discoverSchema(htd.getTableName(), includeColumnQualifiers, includeAllCells, useMapReduce);
    }
  }


  final void discoverSchema(TableName tableName,
          boolean includeColumnQualifiers, boolean includeAllCells, boolean useMapReduce)
          throws IOException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    putTableSchemaEntity(standardAdmin.getTableDescriptor(tableName));
    if (includeColumnQualifiers) {
      discoverColumnMetadata(tableName, includeAllCells, useMapReduce);
    }
  }

  private void discoverColumnMetadata(
          TableName tableName, boolean includeAllCells, boolean useMapReduce)
          throws IOException {
    MTableDescriptor mtd = getMTableDescriptor(tableName);
    if (mtd == null) {
      return;
    }
    // for any previously-discovered ColumnAuditors, reset counters
    for (MColumnDescriptor mcd : mtd.getMColumnDescriptors()) {
      for (ColumnAuditor colAuditor : mcd.getColumnAuditors()) {
        byte[] rowId = new RowId(SchemaEntityType.COLUMN_AUDITOR.getRecordType(),
                mcd.getForeignKey(), colAuditor.getColumnQualifier()).getByteArray();
        long resetValue = repositoryTable.incrementColumnValue(
                rowId, REPOSITORY_CF, COL_COUNTER_QUALIFIER, 0) * -1;
        repositoryTable.incrementColumnValue(
                rowId, REPOSITORY_CF, COL_COUNTER_QUALIFIER, resetValue);
        resetValue = repositoryTable.incrementColumnValue(
                rowId, REPOSITORY_CF, CELL_COUNTER_QUALIFIER, 0) * -1;
        repositoryTable.incrementColumnValue(
                rowId, REPOSITORY_CF, CELL_COUNTER_QUALIFIER, resetValue);
      }
    }
    // perform full scan w/ KeyOnlyFilter(true), so only col name & length returned
    if (useMapReduce) {
      try {
        int jobCompletionCode = ToolRunner.run(MConfiguration.create(), new ColumnDiscoveryTool(),
                  new String[]{TABLE_NAME_ARG_KEY + tableName.getNameAsString(),
                    INCLUDE_ALL_CELLS_ARG_KEY + includeAllCells});
        if (jobCompletionCode != 0) {
          logger.warn("Mapreduce process failure in " + ColumnDiscoveryTool.class.getSimpleName());
        }
      } catch (Exception e) {
        if (IOException.class.isAssignableFrom(e.getClass())) {
          throw (IOException)e;
        } else {
          throw new IOException("Mapreduce process failure", e);
        }
      }
    } else {
      Table table = hbaseConnection.getTable(tableName);
      Scan colScan = new Scan().setFilter(new KeyOnlyFilter(true));
      if (includeAllCells) {
        colScan.setMaxVersions();
      }
      try (ResultScanner rows = table.getScanner(colScan)) {
        for (Result row : rows) {
          putDiscoveredColumnAuditors(mtd, row, true);
        }
      }
    }
  }

  Map<byte[], byte[]> getAliasMap(TableName tableName, byte[] colFamily, List<Cell> cellList)
          throws IOException {
    Map<byte[], byte[]> aliasMap = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    Set<byte[]> colQualifierSet = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
    for (Cell cell : cellList) {
      colQualifierSet.add(Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(),
              cell.getQualifierLength()));
    }
    // get existing aliases from aliasTable
    RowId rowId = new RowId(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName), colFamily);
    Get getAliasRow = new Get(rowId.getByteArray());
    for (byte[] colQualifier : colQualifierSet) {
      getAliasRow.addColumn(ALIAS_CF, colQualifier);
    }
    Result aliasRow = aliasTable.get(getAliasRow);
    if (!aliasRow.isEmpty()) {
      aliasMap.putAll(aliasRow.getFamilyMap(ALIAS_CF));
    }
    for (byte[] colQualifier : colQualifierSet) {
      if (aliasMap.get(colQualifier) == null) {
        aliasMap.put(colQualifier, getNewAlias(rowId.getByteArray(), colQualifier));
      }
    }
    return aliasMap;
  }


  private byte[] getNewAlias(byte[] aliasTableRowId, byte[] colQualifier) throws IOException {
    byte[] newAlias = Bytes.toBytes(new Long(aliasTable.incrementColumnValue(
            aliasTableRowId, ALIAS_CF, ALIAS_INCREMENTOR_COLUMN, 1)).intValue());
    Put putNewAlias = new Put(aliasTableRowId).addColumn(ALIAS_CF, colQualifier, newAlias);
    boolean putSucceeded = aliasTable.checkAndPut(
            aliasTableRowId, ALIAS_CF, colQualifier, null, putNewAlias);
    // put may NOT have succeeded if concurrent thread already stored an alias for the qualifier
    if (!putSucceeded) {
      Get getAlias = new Get(aliasTableRowId).addColumn(ALIAS_CF, colQualifier);
      newAlias = aliasTable.get(getAlias).getValue(ALIAS_CF, colQualifier);
    }
    return newAlias;
  }

  private void validateNamespaceTableNameIncludedForProcessing(
          String namespace, TableName tableName)
          throws TableNotIncludedForProcessingException {
    if (tableName == null || tableName.getNameAsString().isEmpty()) {
      if (namespace != null && !namespace.isEmpty()
              && !isIncludedNamespace(namespace)) {
        throw new TableNotIncludedForProcessingException(
                Bytes.toBytes(namespace + ALL_TABLES_WILDCARD_INDICATOR),
                "NO table from namespace <" + namespace + "> is included for "
                        + PRODUCT_NAME + " processing.");
      }
    } else {
      if (!isIncludedTable(tableName)) {
        throw new TableNotIncludedForProcessingException(tableName.getName(), null);
      }
    }
  }

  void exportSchema(String sourceNamespace, TableName sourceTableName, File targetFile)
          throws IOException, JAXBException, XMLStreamException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    validateNamespaceTableNameIncludedForProcessing(sourceNamespace, sourceTableName);
    String allLiteral = "";
    if ((sourceNamespace == null || sourceNamespace.isEmpty())
            && (sourceTableName == null || sourceTableName.getNameAsString().isEmpty())) {
      allLiteral = "ALL ";
    }
    logger.info("EXPORT of " + allLiteral
            + "ColumnManager repository schema to external XML file has been invoked.");
    if (sourceNamespace != null && !sourceNamespace.isEmpty()) {
      logger.info("EXPORT source NAMESPACE: " + sourceNamespace);
    }
    if (sourceTableName != null && !sourceTableName.getNameAsString().isEmpty()) {
      logger.info("EXPORT source TABLE: " + sourceTableName.getNameAsString());
    }
    logger.info("EXPORT target FILE NAME: " + targetFile.getAbsolutePath());

    HBaseSchemaArchive.exportToXmlFile(
            new HBaseSchemaArchive(sourceNamespace, sourceTableName, this), targetFile);
    logger.info("EXPORT of ColumnManager repository schema has been completed.");
  }

  void importSchema(File sourceHsaFile, String namespaceFilter, TableName tableNameFilter,
          byte[] colFamilyFilter, boolean includeColumnAuditors,
          boolean bypassNamespacesTablesAndCFs)
          throws IOException, JAXBException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    validateNamespaceTableNameIncludedForProcessing(namespaceFilter, tableNameFilter);
    submitImportMessagesToLogger(sourceHsaFile, namespaceFilter, tableNameFilter,
            colFamilyFilter, includeColumnAuditors, bypassNamespacesTablesAndCFs);

    Set<Object> importedDescriptors =  new LinkedHashSet<>();
    for (SchemaEntity entity :
            HBaseSchemaArchive.deserializeXmlFile(sourceHsaFile).getSchemaEntities()) {
      importedDescriptors.addAll(SchemaEntity.convertToNamespaceAndTableDescriptorSet(
              entity, namespaceFilter, tableNameFilter, colFamilyFilter));
    }
    createImportedStructures(importedDescriptors,
            includeColumnAuditors, bypassNamespacesTablesAndCFs);
  }

  private void submitImportMessagesToLogger(File sourceHsaFile, String namespaceFilter,
          TableName tableNameFilter, byte[] colFamilyFilter, boolean includeColumnAuditors,
          boolean bypassNamespacesTablesAndCFs) {
    logger.info("IMPORT of "
            + ((bypassNamespacesTablesAndCFs) ? "<COLUMN DEFINITION> " : "")
            + "schema "
            + ((includeColumnAuditors) ? "<INCLUDING COLUMN AUDITOR METADATA> " : "")
            + "from external HBaseSchemaArchive (XML) file has been requested.");
    if (namespaceFilter != null && !namespaceFilter.isEmpty()
            && (tableNameFilter == null || tableNameFilter.getNameAsString().isEmpty())) {
      logger.info("IMPORT NAMESPACE: " + namespaceFilter);
    }
    if (tableNameFilter != null && !tableNameFilter.getNameAsString().isEmpty()) {
      logger.info("IMPORT TABLE: " + tableNameFilter.getNameAsString());
    }
    if (colFamilyFilter != null && colFamilyFilter.length > 0) {
      logger.info("IMPORT COLUMN FAMILY: " + Bytes.toString(colFamilyFilter));
    }
    logger.info("IMPORT source PATH/FILE-NAME: " + sourceHsaFile.getAbsolutePath());
  }

  private void createImportedStructures(Set<Object> importedDescriptors,
          boolean includeColumnAuditors, boolean bypassNamespacesTablesAndCFs)
          throws IOException {
    for (Object descriptor : importedDescriptors) {
      if (MNamespaceDescriptor.class.isAssignableFrom(descriptor.getClass())) {
        NamespaceDescriptor nd
                = ((MNamespaceDescriptor) descriptor).getNamespaceDescriptor();
        if (!isIncludedNamespace(nd.getName()) || namespaceExists(nd.getName())) {
          continue;
        }
        getAdmin().createNamespace(nd);
        putNamespaceSchemaEntity(nd);
        logger.info("IMPORT COMPLETED FOR NAMESPACE: " + nd.getName());
      } else if (MTableDescriptor.class.isAssignableFrom(descriptor.getClass())) {
        MTableDescriptor mtd = (MTableDescriptor) descriptor;
        if (!isIncludedTable(mtd.getTableName())) {
          continue;
        }
        if (getAdmin().tableExists(mtd.getTableName())) {
          if (bypassNamespacesTablesAndCFs) {
            putColumnDefinitionSchemaEntities(mtd);
          }
        } else {
          getAdmin().createTable(mtd); // includes creation of Column Families
          putTableSchemaEntity(mtd);
          putColumnDefinitionSchemaEntities(mtd);
          if (includeColumnAuditors) {
            putColumnAuditorSchemaEntities(mtd);
          }
          logger.info("IMPORT COMPLETED FOR TABLE: " + mtd.getNameAsString()
                  + (includeColumnAuditors ? " <INCLUDING COLUMN AUDITOR METADATA>" : ""));
        }
      }
    }
  }

  void dumpRepositoryTable() throws IOException {
    logger.info("DUMP of ColumnManager repository table has been requested.");
    try (ResultScanner results
            = repositoryTable.getScanner(new Scan().setMaxVersions())) {
      logger.info("** START OF COMPLETE SCAN OF " + PRODUCT_NAME + " REPOSITORY TABLE **");
      for (Result result : results) {
        byte[] rowId = result.getRow();
        logger.info("Row type: "
                + Bytes.toString(rowId).substring(0, 1));
        logger.info("  Row ID: " + getPrintableString(rowId));
        logger.info("  Element name: " + Bytes.toString(new RowId(rowId).getEntityName()));
        for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cfEntry : result.getMap().entrySet()) {
          logger.info("  Column Family: "
                  + Bytes.toString(cfEntry.getKey()));
          for (Entry<byte[], NavigableMap<Long, byte[]>> colEntry
                  : cfEntry.getValue().entrySet()) {
            logger.info("    Column: "
                    + getPrintableString(colEntry.getKey()));
            for (Entry<Long, byte[]> cellEntry : colEntry.getValue().entrySet()) {
              logger.info("      Cell timestamp: "
                      + cellEntry.getKey());
              logger.info("      Cell value: "
                      + getPrintableString(cellEntry.getValue()));
            }
          }
        }
      }
      logger.info("** END OF COMPLETE SCAN OF " + PRODUCT_NAME + " REPOSITORY TABLE **");
    }
    logger.info("DUMP of ColumnManager repository table is complete.");
  }

  boolean outputReportOnColumnQualifiers (
          String namespace, TableName tableName, byte[] colFamily, File targetFile)
          throws IOException {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    if (tableName == null && !isIncludedNamespace(namespace)) {
      throw new TableNotIncludedForProcessingException(
              Bytes.toBytes(namespace + ALL_TABLES_WILDCARD_INDICATOR),
              "NO table from namespace <" + namespace + "> is included for "
                      + PRODUCT_NAME + " processing.");
    }
    if (tableName != null && !isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    ColumnQualifierReport columnQualifierReport
            = new ColumnQualifierReport(namespace, tableName, colFamily, this, targetFile);
    return columnQualifierReport.isEmpty();
  }

  boolean outputReportOnInvalidColumns (ColumnInvalidityReport.ReportType reportType,
          TableName tableName, byte[] colFamily, File targetFile, boolean verbose,
          boolean includeAllCells, boolean useMapreduce) throws Exception {
    if (!this.isActivated()) {
      throw new ColumnManagerIOException(REPOSITORY_NOT_ACTIVATED_MSG) {};
    }
    if (!isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    MTableDescriptor mtd = getMTableDescriptor(tableName);
    if (mtd == null || !mtd.hasColumnDefinitions()) {
      throw new ColumnDefinitionNotFoundException(tableName.getName(), colFamily, null,
              "No ColumnDefinitions found for table/columnFamily");
    }
    if (colFamily != null
            && (mtd.getMColumnDescriptor(colFamily) == null
            || mtd.getMColumnDescriptor(colFamily).getColumnDefinitions().isEmpty())) {
      throw new ColumnDefinitionNotFoundException(tableName.getName(), colFamily, null,
              "No ColumnDefinitions found for columnFamily");
    }
    try (ColumnInvalidityReport columnInvalidityReport = new ColumnInvalidityReport(
            reportType, hbaseConnection, mtd, colFamily, targetFile,
            verbose, includeAllCells, useMapreduce)) {
      return !columnInvalidityReport.isEmpty();
    }
  }

  static void dropRepository(Admin hbaseAdmin, Logger logger) throws IOException {
    Admin standardAdmin = getStandardAdmin(hbaseAdmin);
    if (!standardAdmin.tableExists(REPOSITORY_TABLENAME)) {
      return;
    }
    logger.warn("DROP (disable/delete) of " + PRODUCT_NAME
            + " Repository tables and namespace has been requested.");
    standardAdmin.disableTable(REPOSITORY_TABLENAME);
    standardAdmin.deleteTable(REPOSITORY_TABLENAME);
    logger.warn("DROP (disable/delete) of " + PRODUCT_NAME
            + " Repository table has been completed: "
            + REPOSITORY_TABLENAME.getNameAsString());
    standardAdmin.disableTable(ALIAS_DIRECTORY_TABLENAME);
    standardAdmin.deleteTable(ALIAS_DIRECTORY_TABLENAME);
    logger.warn("DROP (disable/delete) of " + PRODUCT_NAME
            + " AliasDirectory table has been completed: "
            + REPOSITORY_TABLENAME.getNameAsString());
    standardAdmin.deleteNamespace(REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
    logger.warn("DROP (delete) of " + PRODUCT_NAME + " Repository namespace has been completed: "
            + REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
  }

  boolean namespaceExists(String namespaceName) throws IOException {
    try {
      getAdmin().getNamespaceDescriptor(namespaceName);
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }

  void logIOExceptionAsError(IOException e, String originatingClassName) {
    logger.error(new StringBuilder(PRODUCT_NAME).append(" ")
            .append(e.getClass().getSimpleName())
            .append(" encountered in multithreaded ")
            .append(originatingClassName)
            .append(" processing.").toString(), e);
  }

  static String getPrintableString(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return "";
    }
    if (isPrintable(bytes)) {
      return Bytes.toString(bytes);
    } else {
      StringBuilder sb = new StringBuilder("H\'");
      for (byte b : bytes) {
        sb.append(String.format("%02x", b));
      }
      sb.append("\'");
      return sb.toString();
    }
  }

  static boolean isPrintable(byte[] bytes) {
    if (bytes == null) {
      return false;
    }
    for (byte nextByte : bytes) {
      if (!(Character.isDefined(nextByte) && nextByte > 31)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isInteger(String input) {
    try {
      Integer.parseInt(input);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  static class RowId {
    private final ByteBuffer rowIdByteBuffer;
    private final byte entityType;
    private final byte[] parentForeignKey;
    private final byte[] entityName;

    RowId (byte recordType, byte[] parentForeignKey, byte[] entityName) {
      this.entityType = recordType;
      this.parentForeignKey = parentForeignKey;
      this.entityName = entityName;
      if (this.entityName == null) {
        rowIdByteBuffer = ByteBuffer.allocate(1 + this.parentForeignKey.length);
        rowIdByteBuffer.put(recordType).put(this.parentForeignKey);
      } else {
        rowIdByteBuffer
                = ByteBuffer.allocate(1 + this.parentForeignKey.length + this.entityName.length);
        rowIdByteBuffer.put(this.entityType).put(this.parentForeignKey).put(this.entityName);
      }
    }

    RowId (byte[] rowIdByteArray) {
      if (rowIdByteArray.length < 2) {
        throw new RuntimeException("Invalid RowId length (less than 2) encountered in "
                + PRODUCT_NAME + " Repository Table processing.");
      }
      entityType = rowIdByteArray[0];
      int entityNamePosition;
      switch (SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP.get(entityType)) {
        case NAMESPACE:
          entityNamePosition = 2;
          parentForeignKey = NAMESPACE_PARENT_FOREIGN_KEY;
          break;
        case TABLE:
        case COLUMN_FAMILY:
        case COLUMN_AUDITOR:
        case COLUMN_DEFINITION:
          entityNamePosition = 1 + UNIQUE_FOREIGN_KEY_LENGTH;
          parentForeignKey = Bytes.copy(rowIdByteArray, 1, UNIQUE_FOREIGN_KEY_LENGTH);
          break;
        default:
          throw new RuntimeException("Invalid record type encountered in " + PRODUCT_NAME
                  + " Repository Table processing: <" + String.valueOf(entityType) + ">");
      }
      entityName = new byte[rowIdByteArray.length - entityNamePosition];
      rowIdByteBuffer = ByteBuffer.wrap(rowIdByteArray);
      rowIdByteBuffer.position(entityNamePosition);
      rowIdByteBuffer.get(entityName, 0, entityName.length);
    }

    byte[] getByteArray() {
      return rowIdByteBuffer.array();
    }

    byte getEntityType() {
      return entityType;
    }

    byte[] getParentForeignKey() {
      return parentForeignKey;
    }

    byte[] getEntityName() {
      return entityName;
    }

    /**
     * If submitted entityName is null, stopRowId will be concatenation of startRowId and a
     * byte-array of 0xff value bytes (making for a Scan intended to return one-to-many Rows,
     * all with RowIds prefixed with the startRowId value); if entityName is NOT null,
     * stopRowId will be concatenation of startRowId and a byte-array of 0x00 value bytes
     * (making for a Scan intended to return a single Row with RowId precisely equal to
     * startRowId value).
     *
     * @return stopRowId value
     */
    byte[] getStopRowIdByteArray() {
      final byte[] fillerArray;
      if (entityName == null) {
        fillerArray = HEX_FF_ARRAY;
      } else {
        fillerArray = HEX_00_ARRAY;
      }
      return ByteBuffer.allocate(rowIdByteBuffer.array().length + fillerArray.length)
              .put(rowIdByteBuffer.array()).put(fillerArray).array();
    }
  }
}