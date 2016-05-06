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
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
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
  private static final Logger staticLogger
          = Logger.getLogger(Repository.class.getPackage().getName());
  private final byte[] javaUsername;
  private final boolean columnManagerIsActivated;
  private final Set<String> includedNamespaces;
  private final Set<String> includedEntireNamespaces;
  private final Set<TableName> includedTables;
  private final Set<String> excludedNamespaces;
  private final Set<String> excludedEntireNamespaces;
  private final Set<TableName> excludedTables;
  private final Connection hbaseConnection;
  private final Admin standardAdmin;
  private final Table repositoryTable;

  static final String PRODUCT_NAME = "ColumnManagerAPI";
  private static final byte[] JAVA_USERNAME_PROPERTY_KEY = Bytes.toBytes("user.name");

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

  private static final int UNIQUE_FOREIGN_KEY_LENGTH = 16;
  private static final NamespaceDescriptor HBASE_SYSTEM_NAMESPACE_DESCRIPTOR
          = NamespaceDescriptor.create("hbase").build();
  static final NamespaceDescriptor REPOSITORY_NAMESPACE_DESCRIPTOR
          = NamespaceDescriptor.create("column_manager_repository_namespace").build();
  static final TableName REPOSITORY_TABLENAME
          = TableName.valueOf(REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
                  "column_manager_repository_table");
  private static final byte[] REPOSITORY_COLFAMILY = Bytes.toBytes("se"); // ("se"="SchemaEntities")
  static final int REPOSITORY_DEFAULT_MAX_VERSIONS = 50; // should this be set higher?

  static final byte[] NAMESPACE_PARENT_FOREIGN_KEY = {'-'};
  private static final byte[] DEFAULT_NAMESPACE = Bytes.toBytes("default");
  private static final Map<ImmutableBytesWritable, ImmutableBytesWritable> EMPTY_VALUES = new HashMap<>();
  private static final String CONFIG_COLUMN_PREFIX = "Configuration__";
  private static final byte[] CONFIG_COLUMN_PREFIX_BYTES = Bytes.toBytes(CONFIG_COLUMN_PREFIX);
  private static final String VALUE_COLUMN_PREFIX = "Value__";
  private static final byte[] VALUE_COLUMN_PREFIX_BYTES = Bytes.toBytes(VALUE_COLUMN_PREFIX);
  private static final byte[] MAX_VALUE_COLUMN_NAME
          = ByteBuffer.allocate(VALUE_COLUMN_PREFIX.length() + ColumnAuditor.MAX_VALUE_LENGTH_KEY.length())
          .put(VALUE_COLUMN_PREFIX_BYTES).put(Bytes.toBytes(ColumnAuditor.MAX_VALUE_LENGTH_KEY))
          .array();

  static final String OUT_OF_SYNC_ERROR_MSG
          = PRODUCT_NAME + " repository may be out of sync with HBase.";
  private static final String BLANKS = "                    ";
  private static final int TAB = 3;
  private static final byte[] ENTITY_STATUS_COLUMN = Bytes.toBytes("_Status");
  private static final byte[] ACTIVE_STATUS = Bytes.toBytes("A");
  private static final byte[] DELETED_STATUS = Bytes.toBytes("D");
  private static final byte[] FOREIGN_KEY_COLUMN = Bytes.toBytes("_ForeignKey");
  private static final byte[] COL_DEFINITIONS_ENFORCED_COLUMN
          = Bytes.toBytes("_ColDefinitionsEnforced");
  private static final byte[] HEX_00_ARRAY = new byte[16];
  private static final byte[] HEX_FF_ARRAY = new byte[16];

  static {
    Arrays.fill(HEX_FF_ARRAY, (byte) 0xff);
  }

  Repository(Connection hBaseConnection, Object originatingObject) throws IOException {
    logger = Logger.getLogger(this.getClass().getPackage().getName());
    javaUsername
            = Bytes.toBytes(System.getProperty(Bytes.toString(JAVA_USERNAME_PROPERTY_KEY)));
    this.hbaseConnection = hBaseConnection;
    this.standardAdmin = getNewAdmin(this.hbaseConnection);
    Configuration conf = hbaseConnection.getConfiguration();
    // Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
    String columnManagerActivatedStatus
            = conf.get(HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED,
                    HBASE_CONFIG_PARM_VALUE_COLMANAGER_DEACTIVATED);
    String[] includedTablesArray
            = conf.getStrings(HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES);
    String[] excludedTablesArray
            = conf.getStrings(HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES);
    logger.info(PRODUCT_NAME + " Repository instance being instantiated by object of "
            + originatingObject.getClass().getSimpleName() + " class.");
    logger.info(PRODUCT_NAME + " config parameter: " + HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED
            + " = " + columnManagerActivatedStatus);
    if (columnManagerActivatedStatus.equalsIgnoreCase(HBASE_CONFIG_PARM_VALUE_COLMANAGER_ACTIVATED)) {
      columnManagerIsActivated = true;
      repositoryTable = getRepositoryTable();
      logger.info(PRODUCT_NAME + " Repository is ACTIVATED.");
    } else {
      columnManagerIsActivated = false;
      repositoryTable = null;
      logger.info(PRODUCT_NAME + " Repository is NOT ACTIVATED.");
      includedNamespaces = null;
      includedEntireNamespaces = null;
      includedTables = null;
      excludedNamespaces = null;
      excludedEntireNamespaces = null;
      excludedTables = null;
      return;
    }
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
            if (excludedTableString.endsWith(":*")) { // exclude entire namespace
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
          if (includedTableString.endsWith(":*")) { // include entire namespace
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

  private Table getRepositoryTable() throws IOException {
    createRepositoryNamespace(standardAdmin);
    return createRepositoryTable(standardAdmin);
  }

  /**
   * Creates repository namespace if it does not already exist.
   *
   * @param hbaseAdmin an Admin object
   * @throws IOException if a remote or network exception occurs
   */
  static void createRepositoryNamespace(Admin hbaseAdmin) throws IOException {
    Admin standardAdmin = getStandardAdmin(hbaseAdmin);

    if (namespaceExists(standardAdmin, REPOSITORY_NAMESPACE_DESCRIPTOR)) {
      staticLogger.info("ColumnManager Repository Namespace found: "
              + REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
    } else {
      standardAdmin.createNamespace(REPOSITORY_NAMESPACE_DESCRIPTOR);
      staticLogger.info("ColumnManager Repository Namespace has been created (did not already exist): "
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
  static Table createRepositoryTable(Admin hbaseAdmin) throws IOException {
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
            addFamily(new HColumnDescriptor(REPOSITORY_COLFAMILY).
                    setMaxVersions(REPOSITORY_DEFAULT_MAX_VERSIONS).
                    setInMemory(true)));
    try (Table newRepositoryTable
            = standardConnection.getTable(REPOSITORY_TABLENAME)) {
      staticLogger.info("ColumnManager Repository Table has been created (did not already exist): "
              + REPOSITORY_TABLENAME.getNameAsString());
      return newRepositoryTable;
    }
  }

  static void setRepositoryMaxVersions(Admin hbaseAdmin, int maxVersions)
          throws IOException {
    HColumnDescriptor repositoryHcd
            = getStandardAdmin(hbaseAdmin).getTableDescriptor(REPOSITORY_TABLENAME)
            .getFamily(REPOSITORY_COLFAMILY);
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
            = getStandardAdmin(hbaseAdmin).getTableDescriptor(REPOSITORY_TABLENAME)
            .getFamily(REPOSITORY_COLFAMILY);
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

  boolean isActivated() {
    return columnManagerIsActivated;
  }

  Admin getAdmin() {
    return this.standardAdmin;
  }

  private static Admin getNewAdmin(Connection hbaseConnection) throws IOException {
    try (Admin admin = getStandardConnection(hbaseConnection).getAdmin()) {
      return admin;
    }
  }

  private static boolean namespaceExists(Admin hbaseAdmin, NamespaceDescriptor nd)
          throws IOException {
    try {
      hbaseAdmin.getNamespaceDescriptor(nd.getName());
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }

  private static boolean namespaceExists(Admin hbaseAdmin, byte[] namespace)
          throws IOException {
    return namespaceExists(hbaseAdmin,
            NamespaceDescriptor.create(Bytes.toString(namespace)).build());
  }

  private boolean isIncludedNamespace(String namespaceName) {
    if (namespaceName.equals(HBASE_SYSTEM_NAMESPACE_DESCRIPTOR.getName())
            || namespaceName.equals(REPOSITORY_NAMESPACE_DESCRIPTOR.getName())) {
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

  private boolean isIncludedTable(TableName tableName) {
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
   *
   * @param nd
   * @return foreign key value of repository row that holds namespace SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  byte[] putNamespace(NamespaceDescriptor nd)
          throws IOException {
    if (!isIncludedNamespace(nd.getName())) {
      return null;
    }
    byte[] namespaceRowId
            = buildRowId(SchemaEntityType.NAMESPACE.getRecordType(), NAMESPACE_PARENT_FOREIGN_KEY,
                    Bytes.toBytes(nd.getName()));
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(EMPTY_VALUES, nd.getConfiguration());
    return putSchemaEntityChanges(namespaceRowId, entityAttributeMap, false);
  }

  /**
   *
   * @param htd
   * @return foreign key value of repository row that holds table SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  byte[] putTable(HTableDescriptor htd) throws IOException {
    if (!isIncludedTable(htd.getTableName())) {
      return null;
    }
    byte[] namespaceForeignKey
            = getNamespaceForeignKey(htd.getTableName().getNamespace());
    byte[] tableRowId
            = buildRowId(SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey,
                    htd.getTableName().getName());
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(htd.getValues(), htd.getConfiguration());
    byte[] tableForeignKey
            = putSchemaEntityChanges(tableRowId, entityAttributeMap, false);

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
      putColumnFamily(tableForeignKey, hcd, htd.getTableName());
    }
    return tableForeignKey;
  }

  byte[] putColumnFamily(TableName tn, HColumnDescriptor hcd)
          throws IOException {
    if (!isIncludedTable(tn)) {
      return null;
    }
    byte[] tableForeignKey = getTableForeignKey(tn);
    // Note that ColumnManager can be installed atop an already-existing HBase
    //  installation, so table metadata might not yet have been captured in repository.
    if (tableForeignKey == null) {
      tableForeignKey = putTable(standardAdmin.getTableDescriptor(tn));
    }
    return putColumnFamily(tableForeignKey, hcd, tn);
  }

  /**
   *
   * @param tableForeignKey
   * @param hcd
   * @return foreign key value of repository row that holds Column Family SchemaEntity
   * @throws IOException if a remote or network exception occurs
   */
  private byte[] putColumnFamily(byte[] tableForeignKey, HColumnDescriptor hcd, TableName tableName)
          throws IOException {
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(hcd.getValues(), hcd.getConfiguration());

    byte[] colFamilyForeignKey
            = putSchemaEntityChanges(buildRowId(SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey, hcd.getName()),
                    entityAttributeMap, false);

    if (MColumnDescriptor.class.isAssignableFrom(hcd.getClass())) {
      setColumnDefinitionsEnforced(((MColumnDescriptor) hcd).columnDefinitionsEnforced(),
              SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey, hcd.getName());
    }

    return colFamilyForeignKey;
  }

  /**
   * Invoked during serialization process, when fully-formed MTableDescriptor is submitted for
   * persistence (e.g., during importation of schema from external source).
   *
   * @param mtd MTableDescriptor
   * @return true if all serializations complete successfully
   * @throws IOException if a remote or network exception occurs
   */
  boolean putColumnAuditors(MTableDescriptor mtd) throws IOException {
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
        byte[] colForeignKey = putColumnAuditor(colDescForeignKey, columnAuditor);
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
  private byte[] putColumnAuditor(byte[] colFamilyForeignKey, ColumnAuditor columnAuditor)
          throws IOException {
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(columnAuditor.getValues(), columnAuditor.getConfiguration());

    return putSchemaEntityChanges(buildRowId(SchemaEntityType.COLUMN_AUDITOR.getRecordType(), colFamilyForeignKey, columnAuditor.getName()),
            entityAttributeMap, false);
  }

  void putColumnAuditors(TableName tableName, List<? extends Mutation> mutations)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return;
    }
    MTableDescriptor mtd = getMTableDescriptor(tableName);
    for (Mutation mutation : mutations) {
      putColumnAuditors(mtd, mutation);
    }
  }

  void putColumnAuditors(TableName tableName, Mutation mutation)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return;
    }
    putColumnAuditors(getMTableDescriptor(tableName), mutation);
  }

  void putColumnAuditors(MTableDescriptor mtd, RowMutations mutations) throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      return;
    }
    for (Mutation mutation : mutations.getMutations()) {
      putColumnAuditors(mtd, mutation);
    }
  }

  void putColumnAuditors(MTableDescriptor mtd, List<? extends Mutation> mutations) throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      return;
    }
    for (Mutation mutation : mutations) {
      putColumnAuditors(mtd, mutation);
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
  void putColumnAuditors(MTableDescriptor mtd, Mutation mutation) throws IOException {
    if (!isIncludedTable(mtd.getTableName())
            // column-cell deletes do not affect Repository
            || Delete.class.isAssignableFrom(mutation.getClass())) {
      return;
    }
    for (Entry<byte[], List<Cell>> colFamilyCellList : mutation.getFamilyCellMap().entrySet()) {
      MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamilyCellList.getKey());
      for (Cell cell : colFamilyCellList.getValue()) {
        byte[] colQualifier
                = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        ColumnAuditor oldColAuditor = getColumnAuditor(mcd.getForeignKey(), colQualifier);
        if (oldColAuditor != null && cell.getValueLength() <= oldColAuditor.getMaxValueLengthFound()) {
          continue;
        }
        ColumnAuditor newColAuditor = new ColumnAuditor(colQualifier);
        if (oldColAuditor == null || cell.getValueLength() > oldColAuditor.getMaxValueLengthFound()) {
          newColAuditor.setMaxValueLengthFound(cell.getValueLength());
        } else {
          newColAuditor.setMaxValueLengthFound(oldColAuditor.getMaxValueLengthFound());
        }
        boolean suppressUserName = (oldColAuditor == null) ? false : true;
        Map<byte[], byte[]> entityAttributeMap
                = buildEntityAttributeMap(newColAuditor.getValues(),
                        newColAuditor.getConfiguration());
        putSchemaEntityChanges(buildRowId(SchemaEntityType.COLUMN_AUDITOR.getRecordType(),
                mcd.getForeignKey(), newColAuditor.getName()), entityAttributeMap,
                suppressUserName);
      }
    }
  }

  /**
   * Invoked as part of discovery process.
   *
   * @param mtd table descriptor for parent table of columns found in row
   * @param row Result object from which {@link ColumnAuditor} SchemaEntity is extracted
   * @throws IOException if a remote or network exception occurs
   */
  private void putColumnAuditors(MTableDescriptor mtd, Result row, boolean keyOnlyFilterUsed) throws IOException {
    if (!isIncludedTable(mtd.getTableName())) {
      return;
    }
    for (MColumnDescriptor mcd : mtd.getMColumnDescriptors()) {
      NavigableMap<byte[], byte[]> columnMap = row.getFamilyMap(mcd.getName());
      for (Entry<byte[], byte[]> colEntry : columnMap.entrySet()) {
        byte[] colQualifier = colEntry.getKey();
        int colValueLength;
        if (keyOnlyFilterUsed) {
          colValueLength = Bytes.toInt(colEntry.getValue()); // colValue *length* returned as value
        } else {
          colValueLength = colEntry.getValue().length;
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
                = buildEntityAttributeMap(newColAuditor.getValues(), newColAuditor.getConfiguration());
        putSchemaEntityChanges(buildRowId(SchemaEntityType.COLUMN_AUDITOR.getRecordType(),
                mcd.getForeignKey(), colQualifier),
                entityAttributeMap, suppressUserName);
      }
    }
  }

  void validateColumns(MTableDescriptor mtd, Mutation mutation) throws IOException {
    if (!isIncludedTable(mtd.getTableName())
            || !mtd.hasColDescriptorWithColDefinitionsEnforced()
            || Delete.class.isAssignableFrom(mutation.getClass())) { // column-cell Deletes not validated
      return;
    }
    for (Entry<byte[], List<Cell>> colFamilyCellList : mutation.getFamilyCellMap().entrySet()) {
      MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamilyCellList.getKey());
      if (!mcd.columnDefinitionsEnforced()) {
        continue;
      }
      for (Cell cell : colFamilyCellList.getValue()) {
        byte[] colQualifier
                = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        ColumnDefinition colDefinition = mcd.getColumnDefinition(colQualifier);
        if (colDefinition == null) {
          throw new ColumnDefinitionNotFoundException(mtd.getTableName().getName(),
                  mcd.getName(), colQualifier, null);
        }
        if (colDefinition.getColumnLength() > 0
                && cell.getValueLength() > colDefinition.getColumnLength()) {
          throw new InvalidColumnValueException(mtd.getTableName().getName(), mcd.getName(), colQualifier, null,
                  "Value length of <" + cell.getValueLength()
                  + "> is longer than defined maximum length of <"
                  + colDefinition.getColumnLength() + ">.");
        }
        String colValidationRegex = colDefinition.getColumnValidationRegex();
        if (colValidationRegex != null && colValidationRegex.length() > 0) {
          byte[] colValue = Bytes.getBytes(CellUtil.getValueBufferShallowCopy(cell));
          if (!Bytes.toString(colValue).matches(colValidationRegex)) {
            throw new InvalidColumnValueException(mtd.getTableName().getName(), mcd.getName(),
                    colQualifier, colValue,
                    "Value does not match the regular expression defined for column: <"
                    + colValidationRegex + ">");
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
   * Invoked during serialization process, when fully-formed MTableDescriptor is submitted for
   * persistence (e.g., during importation of schema from external source).
   *
   * @param mtd MTableDescriptor
   * @return true if all serializations complete successfully
   * @throws IOException if a remote or network exception occurs
   */
  boolean putColumnDefinitions(MTableDescriptor mtd) throws IOException {
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
      for (ColumnDefinition colDef : mcd.getColumnDefinitions()) {
        byte[] colDefinitionForeignKey
                = putColumnDefinition(colDescForeignKey, colDef);
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
  boolean putColumnDefinitions(TableName tableName, byte[] colFamily, List<ColumnDefinition> colDefinitions)
          throws IOException {
    boolean allPutsCompleted = false;
    byte[] colFamilyForeignKey = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName),
            colFamily);
    if (colFamilyForeignKey != null) {
      allPutsCompleted = true;
      for (ColumnDefinition colDefinition : colDefinitions) {
        byte[] columnForeignKey
                = putColumnDefinition(colFamilyForeignKey, colDefinition);
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
  private byte[] putColumnDefinition(byte[] colFamilyForeignKey, ColumnDefinition colDef)
          throws IOException {
    Map<byte[], byte[]> entityAttributeMap
            = buildEntityAttributeMap(colDef.getValues(), colDef.getConfiguration());

    return putSchemaEntityChanges(buildRowId(SchemaEntityType.COLUMN_DEFINITION.getRecordType(),
            colFamilyForeignKey, colDef.getName()), entityAttributeMap, false);
  }

  private Map<byte[], byte[]> buildEntityAttributeMap(Map<ImmutableBytesWritable, ImmutableBytesWritable> values,
          Map<String, String> configuration) {
    Map<byte[], byte[]> entityAttributeMap = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    for (Entry<ImmutableBytesWritable, ImmutableBytesWritable> tableValueEntry
            : values.entrySet()) {
      byte[] attributeKeySuffix = tableValueEntry.getKey().copyBytes();
      byte[] attributeValue = tableValueEntry.getValue().copyBytes();
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

  private byte[] putSchemaEntityChanges(byte[] rowId,
          Map<byte[], byte[]> entityAttributeMap, boolean suppressUserName)
          throws IOException {
    Result oldRow = repositoryTable.get(new Get(rowId));
    Put newRow = new Put(rowId);
    Map<byte[], byte[]> oldEntityAttributeMap;

    // ADD Columns to newRow to set foreignKey and entityStatus values appropriately
    byte[] foreignKey;
    if (oldRow.isEmpty()) {
      oldEntityAttributeMap = null;
      foreignKey = generateUniqueForeignKey();
      newRow.addColumn(REPOSITORY_COLFAMILY, FOREIGN_KEY_COLUMN, foreignKey);
      newRow.addColumn(REPOSITORY_COLFAMILY, ENTITY_STATUS_COLUMN, ACTIVE_STATUS);
    } else {
      SchemaEntity entity = deserializeSchemaEntity(oldRow);
      oldEntityAttributeMap
              = buildEntityAttributeMap(entity.getValues(), entity.getConfiguration());
      foreignKey = oldRow.getValue(REPOSITORY_COLFAMILY, FOREIGN_KEY_COLUMN);
      if (!Bytes.equals(oldRow.getValue(REPOSITORY_COLFAMILY, ENTITY_STATUS_COLUMN), ACTIVE_STATUS)) {
        newRow.addColumn(REPOSITORY_COLFAMILY, ENTITY_STATUS_COLUMN, ACTIVE_STATUS);
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
          newRow.addColumn(REPOSITORY_COLFAMILY, newAttributeKey, null);
        } else {
          newRow.addColumn(REPOSITORY_COLFAMILY, newAttributeKey, newAttributeValue);
        }
      }
    }

    // ADD Columns to newRow to nullify value if attribute is in old map but NOT in new map
    if (oldEntityAttributeMap != null) {
      for (byte[] oldAttributeKey : oldEntityAttributeMap.keySet()) {
        if (!entityAttributeMap.containsKey(oldAttributeKey)) {
          newRow.addColumn(REPOSITORY_COLFAMILY, oldAttributeKey, null);
        }
      }
    }

    // PUT newRow to Repository
    if (!newRow.isEmpty()) {
      if (!suppressUserName) {
        newRow.addColumn(REPOSITORY_COLFAMILY, JAVA_USERNAME_PROPERTY_KEY, javaUsername);
      }
      repositoryTable.put(newRow);
    }
    return foreignKey;
  }

  MNamespaceDescriptor getMNamespaceDescriptor(String namespaceName)
          throws IOException {
    Result row = getActiveRow(SchemaEntityType.NAMESPACE.getRecordType(), NAMESPACE_PARENT_FOREIGN_KEY, Bytes.toBytes(namespaceName), null);
    if (row == null || row.isEmpty()) {
      // Note that ColumnManager can be installed atop an already-existing HBase
      //  installation, so namespace SchemaEntity might not yet have been captured in repository,
      //  or namespaceName may not represent active namespace (and so not stored in repository).
      //return new MNamespaceDescriptor(standardAdmin.getNamespaceDescriptor(namespaceName));
      if (isIncludedNamespace(namespaceName)) {
        putNamespace(standardAdmin.getNamespaceDescriptor(namespaceName));
        row = getActiveRow(SchemaEntityType.NAMESPACE.getRecordType(), NAMESPACE_PARENT_FOREIGN_KEY, Bytes.toBytes(namespaceName), null);
      } else {
        return null;
      }
    }
    MNamespaceDescriptor nd = new MNamespaceDescriptor(deserializeSchemaEntity(row));
    return nd;
  }

  Set<MNamespaceDescriptor> getMNamespaceDescriptors() throws IOException {
    Set<MNamespaceDescriptor> mNamespaceDescriptors = new TreeSet<>();
    for (Result row : getActiveRows(SchemaEntityType.NAMESPACE.getRecordType(), NAMESPACE_PARENT_FOREIGN_KEY)) {
      mNamespaceDescriptors.add(new MNamespaceDescriptor(deserializeSchemaEntity(row)));
    }
    return mNamespaceDescriptors;
  }

  MTableDescriptor getMTableDescriptor(TableName tn) throws IOException {
    byte[] namespaceForeignKey = getNamespaceForeignKey(tn.getNamespace());
    Result row = getActiveRow(SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tn.getName(), null);
    if (row == null || row.isEmpty()) {
      // Note that ColumnManager can be installed atop an already-existing HBase
      //  installation, so table SchemaEntity might not yet have been captured in repository,
      //  or TableName may not represent included Table (and so not stored in repository).
      if (isIncludedTable(tn)) {
        putTable(standardAdmin.getTableDescriptor(tn));
        row = getActiveRow(SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tn.getName(), null);
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

  private Set<MTableDescriptor> getMTableDescriptors(byte[] namespaceForeignKey)
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
    for (Result row : getActiveRows(SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey)) {
      MColumnDescriptor mcd = new MColumnDescriptor(deserializeSchemaEntity(row));
      mColumnDescriptors.add(mcd.addColumnAuditors(getColumnAuditors(mcd.getForeignKey()))
              .addColumnDefinitions(getColumnDefinitions(mcd.getForeignKey())));
    }
    return mColumnDescriptors;
  }

  Set<ColumnAuditor> getColumnAuditors(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
    byte[] colFamilyForeignKey
            = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
                    getTableForeignKey(htd), hcd.getName());
    return (colFamilyForeignKey == null) ? null : getColumnAuditors(colFamilyForeignKey);
  }

  private Set<ColumnAuditor> getColumnAuditors(byte[] colFamilyForeignKey)
          throws IOException {
    Set<ColumnAuditor> columnAuditors = new TreeSet<>();
    Result[] colAuditorRows = getActiveRows(SchemaEntityType.COLUMN_AUDITOR.getRecordType(), colFamilyForeignKey);
    if (colAuditorRows != null) {
      for (Result row : colAuditorRows) {
        columnAuditors.add(new ColumnAuditor(deserializeSchemaEntity(row)));
      }
    }
    return columnAuditors;
  }

  private ColumnAuditor getColumnAuditor(byte[] colFamilyForeignKey, byte[] colQualifier)
          throws IOException {
    Result row = getActiveRow(SchemaEntityType.COLUMN_AUDITOR.getRecordType(), colFamilyForeignKey, colQualifier, null);
    return (row == null) ? null : new ColumnAuditor(deserializeSchemaEntity(row));
  }

  Set<ColumnDefinition> getColumnDefinitions(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
    byte[] colFamilyForeignKey
            = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(), getTableForeignKey(htd), hcd.getName());
    return getColumnDefinitions(colFamilyForeignKey);
  }

  private Set<ColumnDefinition> getColumnDefinitions(byte[] colFamilyForeignKey) throws IOException {
    Set<ColumnDefinition> columnDefinitions = new TreeSet<>();
    for (Result row : getActiveRows(SchemaEntityType.COLUMN_DEFINITION.getRecordType(), colFamilyForeignKey)) {
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
    byte[] rowId = row.getRow();
    SchemaEntity entity
            = new SchemaEntity(rowId[0], extractNameFromRowId(rowId));
    for (Entry<byte[], byte[]> colEntry : row.getFamilyMap(REPOSITORY_COLFAMILY).entrySet()) {
      byte[] key = colEntry.getKey();
      byte[] value = colEntry.getValue();
      if (Bytes.equals(key, FOREIGN_KEY_COLUMN)) {
        entity.setForeignKey(colEntry.getValue());
      } else if (Bytes.equals(key, COL_DEFINITIONS_ENFORCED_COLUMN)) {
        entity.setColumnDefinitionsEnforced(Bytes.toBoolean(colEntry.getValue()));
      } else if (key.length > VALUE_COLUMN_PREFIX_BYTES.length
              && Bytes.startsWith(key, VALUE_COLUMN_PREFIX_BYTES)) {
        entity.setValue(Bytes.tail(key, key.length - VALUE_COLUMN_PREFIX_BYTES.length),
                value);
      } else if (key.length > CONFIG_COLUMN_PREFIX_BYTES.length
              && Bytes.startsWith(key, CONFIG_COLUMN_PREFIX_BYTES)) {
        entity.setConfiguration(
                Bytes.toString(Bytes.tail(key, key.length - CONFIG_COLUMN_PREFIX_BYTES.length)),
                Bytes.toString(colEntry.getValue()));
      }
    }
    return entity;
  }

  private static byte[] buildRowId(byte recordType, byte[] parentForeignKey,
          byte[] entityName) {
    ByteBuffer rowId;
    if (entityName == null) {
      rowId = ByteBuffer.allocate(1 + parentForeignKey.length);
      rowId.put(recordType).put(parentForeignKey);
    } else {
      rowId = ByteBuffer.allocate(1 + parentForeignKey.length + entityName.length);
      rowId.put(recordType).put(parentForeignKey).put(entityName);
    }
    return rowId.array();
  }

  /**
   * If submitted entityName is null, stopRow will be concatenation of startRow and a byte-array of
   * 0xff value bytes (making for a Scan intended to return one-to-many Rows, all with RowIds
   * prefixed with the startRow value); if entityName is NOT null, stopRow will be concatenation of
   * startRow and a byte-array of 0x00 value bytes (making for a Scan intended to return a single
   * Row with RowId precisely equal to startRow value).
   *
   * @param startRowValue
   * @param entityName may be null (see comments above)
   * @return stopRow value
   */
  private byte[] buildStopRow(byte[] startRowValue, byte[] entityName) {
    if (entityName == null) {
      return ByteBuffer.allocate(startRowValue.length + HEX_FF_ARRAY.length)
              .put(startRowValue).put(HEX_FF_ARRAY).array();
    } else {
      return ByteBuffer.allocate(startRowValue.length + HEX_00_ARRAY.length)
              .put(startRowValue).put(HEX_00_ARRAY).array();
    }
  }

  private Result getActiveRow(byte recordType, byte[] parentForeignKey, byte[] entityName,
          byte[] columnToGet)
          throws IOException {
    Result[] rows = getActiveRows(false, recordType, parentForeignKey, entityName, columnToGet);
    if (rows == null || rows.length == 0) {
      return null;
    }
    return rows[0];
  }

  private Result[] getActiveRows(byte recordType, byte[] parentForeignKey)
          throws IOException {
    return getActiveRows(false, recordType, parentForeignKey, null, null);
  }

  private Result[] getActiveRows(boolean getRowIdAndStatusOnly, byte recordType,
          byte[] parentForeignKey, byte[] entityName, byte[] columnToGet)
          throws IOException {
    Result[] allRows = getRows(getRowIdAndStatusOnly, recordType,
            parentForeignKey, entityName, columnToGet);
    if (allRows == null) {
      return null;
    }
    Set<Result> activeRows = new HashSet<>();
    for (Result row : allRows) {
      if (Bytes.equals(row.getValue(REPOSITORY_COLFAMILY, ENTITY_STATUS_COLUMN), ACTIVE_STATUS)) {
        activeRows.add(row);
      }
    }
    return activeRows.toArray(new Result[activeRows.size()]);
  }

  private Result[] getRows(byte recordType, byte[] parentForeignKey, byte[] columnToGet)
          throws IOException {
    return getRows(false, recordType, parentForeignKey, null, columnToGet);
  }

  private Result[] getRows(boolean getRowIdAndStatusOnly, byte recordType,
          byte[] parentForeignKey, byte[] entityName,
          byte[] columnToGet)
          throws IOException {
    if (parentForeignKey == null) {
      return null;
    }
    byte[] startRow = buildRowId(recordType, parentForeignKey, entityName);
    byte[] stopRow = buildStopRow(startRow, entityName);
    Scan scanParms = new Scan(startRow, stopRow);
    if (getRowIdAndStatusOnly || columnToGet != null) {
      scanParms.addColumn(REPOSITORY_COLFAMILY, FOREIGN_KEY_COLUMN);
      scanParms.addColumn(REPOSITORY_COLFAMILY, ENTITY_STATUS_COLUMN);
      if (columnToGet != null) {
        scanParms.addColumn(REPOSITORY_COLFAMILY, columnToGet);
      }
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
    Result row = repositoryTable.get(new Get(buildRowId(recordType, parentForeignKey, entityName)));
    if (row.isEmpty()) {
      return null;
    }
    return row.getValue(REPOSITORY_COLFAMILY, FOREIGN_KEY_COLUMN);
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
        namespaceForeignKey
                = putNamespace(standardAdmin.getNamespaceDescriptor(Bytes.toString(namespace)));
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
        tableForeignKey = putTable(standardAdmin.getTableDescriptor(tableName));
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
    return columnDefinitionsEnforced(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName), colFamily);
  }

  private boolean columnDefinitionsEnforced(byte recordType, byte[] parentForeignKey, byte[] entityName)
          throws IOException {
    Result row = getActiveRow(recordType, parentForeignKey, entityName, COL_DEFINITIONS_ENFORCED_COLUMN);
    if (row == null || row.isEmpty()) {
      return false;
    }
    byte[] colValue = row.getValue(REPOSITORY_COLFAMILY, COL_DEFINITIONS_ENFORCED_COLUMN);
    if (colValue == null || colValue.length == 0) {
      return false;
    }
    return Bytes.toBoolean(colValue);
  }

//    void setColumnDefinitionsEnforced (boolean enabled, TableName tableName)
//            throws IOException {
//        setColumnDefinitionsEnforced(enabled, TABLE_RECORD_TYPE,
//                        getNamespaceForeignKey(tableName.getNamespace()), tableName.getName());
//    }
//
  void setColumnDefinitionsEnforced(boolean enabled, TableName tableName, byte[] colFamily)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return; // should an Exception be thrown?
    }
    setColumnDefinitionsEnforced(enabled, SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName), colFamily);
  }

  private void setColumnDefinitionsEnforced(
          boolean enabled, byte recordType, byte[] parentForeignKey, byte[] entityName)
          throws IOException {
    if (columnDefinitionsEnforced(recordType, parentForeignKey, entityName) != enabled) {
      repositoryTable.put(new Put(buildRowId(recordType, parentForeignKey, entityName))
              .addColumn(REPOSITORY_COLFAMILY, COL_DEFINITIONS_ENFORCED_COLUMN, Bytes.toBytes(enabled))
              .addColumn(REPOSITORY_COLFAMILY, JAVA_USERNAME_PROPERTY_KEY, javaUsername));
    }
  }

  private byte[] extractNameFromRowId(byte[] rowId) {
    if (rowId.length < 2) {
      return null;
    }
    int position;
    switch (SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP.get(rowId[0])) {
      case NAMESPACE:
        position = 2;
        break;
      case TABLE:
      case COLUMN_FAMILY:
      case COLUMN_AUDITOR:
      case COLUMN_DEFINITION:
        position = 1 + UNIQUE_FOREIGN_KEY_LENGTH;
        break;
      default:
        return null;
    }
    byte[] name = new byte[rowId.length - position];
    ByteBuffer rowIdBuffer = ByteBuffer.wrap(rowId);
    rowIdBuffer.position(position);
    rowIdBuffer.get(name, 0, name.length);
    return name;
  }

  private byte[] extractParentForeignKeyFromRowId(byte[] rowId) {
    if (rowId.length < 2) {
      return null;
    }
    if (rowId[0] == SchemaEntityType.NAMESPACE.getRecordType()) {
      return NAMESPACE_PARENT_FOREIGN_KEY;
    } else {
      return Bytes.copy(rowId, 1, UNIQUE_FOREIGN_KEY_LENGTH);
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
    Repository.this.deleteNamespaceSchemaEntity(true, name);
  }

  void deleteNamespaceSchemaEntity(String name) throws IOException {
    Repository.this.deleteNamespaceSchemaEntity(false, name);
  }

  private void deleteNamespaceSchemaEntity(boolean purge, String name) throws IOException {
    deleteSchemaEntity(purge, false, SchemaEntityType.NAMESPACE.getRecordType(),
            NAMESPACE_PARENT_FOREIGN_KEY, Bytes.toBytes(name));
  }

  void purgeTableSchemaEntity(TableName tableName) throws IOException {
    Repository.this.deleteTableSchemaEntity(true, false, tableName);
  }

  void truncateTableColumns(TableName tableName) throws IOException {
    Repository.this.deleteTableSchemaEntity(false, true, tableName);
  }

  void deleteTableSchemaEntity(TableName tableName) throws IOException {
    Repository.this.deleteTableSchemaEntity(false, false, tableName);
  }

  private void deleteTableSchemaEntity(boolean purge, boolean truncateColumns,
          TableName tableName) throws IOException {
    byte[] namespaceForeignKey = getNamespaceForeignKey(tableName.getNamespace());
    deleteSchemaEntity(purge, truncateColumns, SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tableName.getName());
  }

  void deleteColumnFamily(TableName tableName, byte[] name) throws IOException {
    deleteSchemaEntity(false, false, SchemaEntityType.COLUMN_FAMILY.getRecordType(), getTableForeignKey(tableName), name);
  }

  /**
   * Used only in administrative deletion of {@link ColumnDefinition}
   *
   * @param tableName name of <i>Table</i> from which {@link ColumnDefinition} is to be deleted
   * @param colFamily <i>Column Family</i> from which {@link ColumnDefinition} is to be deleted
   * @param colQualifier qualifier that identifies the {@link ColumnDefinition} to be deleted
   * @throws IOException
   */
  void deleteColumnDefinition(TableName tableName, byte[] colFamily, byte[] colQualifier)
          throws IOException {
    byte[] colFamilyForeignKey = getForeignKey(SchemaEntityType.COLUMN_FAMILY.getRecordType(),
            getTableForeignKey(tableName),
            colFamily);
    if (colFamilyForeignKey == null) {
      return;
    }
    // TO DO: before (or as part of) conceptual deletion, reset ColumnDefinition's validation-related attributes

    deleteSchemaEntity(false, false, SchemaEntityType.COLUMN_DEFINITION.getRecordType(), colFamilyForeignKey, colQualifier);
  }

  private void deleteSchemaEntity(boolean purge, boolean truncateColumns, byte recordType,
          byte[] parentForeignKey, byte[] entityName)
          throws IOException {
    if (parentForeignKey == null) {
      return;
    }
    for (Result row : getRows(true, recordType, parentForeignKey, entityName, null)) {
      if (!truncateColumns || (truncateColumns && recordType == SchemaEntityType.COLUMN_AUDITOR.getRecordType())) {
        if (purge) {
          repositoryTable.delete(new Delete(row.getRow()));
        } else {
          if (!Bytes.equals(row.getValue(REPOSITORY_COLFAMILY, ENTITY_STATUS_COLUMN), DELETED_STATUS)) {
            repositoryTable.put(new Put(row.getRow())
                    .addColumn(REPOSITORY_COLFAMILY, ENTITY_STATUS_COLUMN, DELETED_STATUS));
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
              row.getValue(REPOSITORY_COLFAMILY, FOREIGN_KEY_COLUMN), null);
      if (childRecordType == SchemaEntityType.COLUMN_AUDITOR.getRecordType()) {
        deleteSchemaEntity(purge, truncateColumns,
                SchemaEntityType.COLUMN_DEFINITION.getRecordType(),
                row.getValue(REPOSITORY_COLFAMILY, FOREIGN_KEY_COLUMN), null);
      }
    }
  }

  void discoverSchema(TableName tableName, boolean includeColumnQualifiers)
          throws IOException {
    if (!isIncludedTable(tableName)) {
      return;
    }
    putTable(standardAdmin.getTableDescriptor(tableName));
    if (includeColumnQualifiers) {
      discoverColumnMetadata(tableName);
    }
  }

  void discoverSchema(boolean includeColumnQualifiers) throws IOException {
    for (NamespaceDescriptor nd : standardAdmin.listNamespaceDescriptors()) {
      if (!isIncludedNamespace(nd.getName())) {
        continue;
      }
      putNamespace(nd);
      for (HTableDescriptor htd : standardAdmin.listTableDescriptorsByNamespace(nd.getName())) {
        if (!isIncludedTable(htd.getTableName())) {
          continue;
        }
        putTable(htd);
      }
    }
    if (includeColumnQualifiers) {
      discoverColumnMetadata();
    }
  }

  private void discoverColumnMetadata(TableName tableName) throws IOException {
    MTableDescriptor mtd = getMTableDescriptor(tableName);
    if (mtd == null) {
      return;
    }
    // perform full scan w/ KeyOnlyFilter(true), so only col name & length returned
    Table table = hbaseConnection.getTable(tableName);
    try (ResultScanner rows
            = table.getScanner(new Scan().setFilter(new KeyOnlyFilter(true)))) {
      for (Result row : rows) {
        putColumnAuditors(mtd, row, true);
      }
    }
  }

  private void discoverColumnMetadata() throws IOException {
    for (MNamespaceDescriptor mnd : getMNamespaceDescriptors()) {
      for (MTableDescriptor mtd : getMTableDescriptors(mnd.getForeignKey())) {
        if (!isIncludedTable(mtd.getTableName())) {
          continue;
        }
        // perform full scan w/ KeyOnlyFilter(true), so only col name & length returned
        Table table = hbaseConnection.getTable(mtd.getTableName());
        try (ResultScanner rows
                = table.getScanner(new Scan().setFilter(new KeyOnlyFilter(true)))) {
          for (Result row : rows) {
            putColumnAuditors(mtd, row, true);
          }
        }
      }
    }
  }

  void exportSchema(String sourceNamespace, TableName sourceTableName,
          File targetFile, boolean formatted)
          throws IOException, JAXBException {
    String allLiteral = "";
    if ((sourceNamespace == null || sourceNamespace.isEmpty())
            && (sourceTableName == null || sourceTableName.getNameAsString().isEmpty())) {
      allLiteral = "ALL ";
    }
    logger.info("EXPORT of " + allLiteral
            + "ColumnManager repository schema to external XML file has been requested.");
    if (sourceNamespace != null && !sourceNamespace.isEmpty()) {
      logger.info("EXPORT source NAMESPACE: " + sourceNamespace);
    }
    if (sourceTableName != null && !sourceTableName.getNameAsString().isEmpty()) {
      logger.info("EXPORT source TABLE: " + sourceTableName.getNameAsString());
    }
    logger.info("EXPORT target FILE NAME: " + targetFile.getAbsolutePath());

    // Convert each object to SchemaEntity and add to schemaArchive
    HBaseSchemaArchive schemaArchive = new HBaseSchemaArchive();
    for (MNamespaceDescriptor mnd : getMNamespaceDescriptors()) {
      if (sourceNamespace != null && !sourceNamespace.equals(Bytes.toString(mnd.getName()))) {
        continue;
      }
      SchemaEntity namespaceEntity = schemaArchive.addSchemaEntity(new SchemaEntity(mnd));
      for (MTableDescriptor mtd : getMTableDescriptors(mnd.getForeignKey())) {
        if (sourceTableName != null
                && !sourceTableName.getNameAsString().equals(mtd.getNameAsString())) {
          continue;
        }
        SchemaEntity tableEntity = new SchemaEntity(mtd);
        namespaceEntity.addChild(tableEntity);
        for (MColumnDescriptor mcd : mtd.getMColumnDescriptors()) {
          SchemaEntity colFamilyEntity = new SchemaEntity(mcd);
          tableEntity.addChild(colFamilyEntity);
          for (ColumnAuditor colAuditor : mcd.getColumnAuditors()) {
            colFamilyEntity.addChild(new SchemaEntity(colAuditor));
          }
          for (ColumnDefinition colDef : mcd.getColumnDefinitions()) {
            colFamilyEntity.addChild(new SchemaEntity(colDef));
          }
        }
      }
    }
    HBaseSchemaArchive.exportToXmlFile(schemaArchive, targetFile, formatted);
    logger.info("EXPORT of ColumnManager repository schema has been completed.");
  }

  Set<Object> deserializeHBaseSchemaArchive(boolean includeColumnAuditors,
          String namespace, TableName tableName, File sourceFile)
          throws JAXBException {

    Set<SchemaEntity> deserializedObjects
            = HBaseSchemaArchive.deserializeXmlFile(sourceFile).getSchemaEntities();
    Set<Object> returnedObjects = new LinkedHashSet<>();
    for (SchemaEntity entity : deserializedObjects) {
      returnedObjects.addAll(convertSchemaEntityToDescriptorSet(
              entity, includeColumnAuditors, namespace, tableName));
    }
    return returnedObjects;
  }

  /**
   * Used exclusively in the deserialization of an HBaseSchemaArchive
   */
  private Set<Object> convertSchemaEntityToDescriptorSet(
          SchemaEntity entity, boolean includeColumnAuditors,
          String namespace, TableName tableName) {
    Set<Object> convertedObjects = new LinkedHashSet<>();
    if (entity.getEntityRecordType() == SchemaEntityType.NAMESPACE.getRecordType()) {
      if (namespace != null && !namespace.equals(entity.getNameAsString())) {
        return convertedObjects; // empty set
      }
      convertedObjects.add(new MNamespaceDescriptor(entity));
      for (SchemaEntity childEntity : entity.getChildren()) {
        convertedObjects.addAll(convertSchemaEntityToDescriptorSet(
                childEntity, includeColumnAuditors, namespace, tableName));
      }
    } else if (entity.getEntityRecordType() == SchemaEntityType.TABLE.getRecordType()) {
      if (tableName != null && !tableName.getNameAsString().equals(entity.getNameAsString())) {
        return convertedObjects; // empty set
      }
      MTableDescriptor mtd = new MTableDescriptor(entity);
      if (entity.getChildren() != null) {
        for (SchemaEntity childEntity : entity.getChildren()) {
          if (childEntity.getEntityRecordType() != SchemaEntityType.COLUMN_FAMILY.getRecordType()) {
            continue;
          }
          Set<Object> returnedMcdSet
                  = convertSchemaEntityToDescriptorSet(childEntity, includeColumnAuditors, namespace, tableName);
          for (Object returnedMcd : returnedMcdSet) {
            mtd.addFamily((MColumnDescriptor) returnedMcd);
          }
        }
      }
      convertedObjects.add(mtd);
    } else if (entity.getEntityRecordType() == SchemaEntityType.COLUMN_FAMILY.getRecordType()) {
      MColumnDescriptor mcd = new MColumnDescriptor(entity);
      if (entity.getChildren() != null) {
        for (SchemaEntity childEntity : entity.getChildren()) {
          if (childEntity.getEntityRecordType() == SchemaEntityType.COLUMN_AUDITOR.getRecordType()) {
            if (includeColumnAuditors) {
              mcd.addColumnAuditor(new ColumnAuditor(childEntity));
            }
          } else if (childEntity.getEntityRecordType()
                  == SchemaEntityType.COLUMN_DEFINITION.getRecordType()) {
            mcd.addColumnDefinition(new ColumnDefinition(childEntity));
          }
        }
      }
      convertedObjects.add(mcd);
    }
    return convertedObjects;
  }

  String getHBaseSchemaArchiveSummary(File sourceFile)
          throws JAXBException {
    HBaseSchemaArchive schemaArchive = HBaseSchemaArchive.deserializeXmlFile(sourceFile);
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("SUMMARY OF external HBase Schema Archive file*\n")
            .append(BLANKS, 0, TAB).append("SOURCE FILE: ")
            .append(sourceFile.getAbsolutePath()).append("\n")
            .append(BLANKS, 0, TAB).append("FILE TIMESTAMP: ")
            .append(schemaArchive.getArchiveFileTimestampString()).append("\n")
            .append(BLANKS, 0, TAB).append("FILE CONTENTS:\n");
    for (SchemaEntity entity : schemaArchive.getSchemaEntities()) {
      stringBuilder.append(appendSchemaEntityDescription(entity, TAB + TAB));
    }
    stringBuilder.append("\n").append(BLANKS, 0, TAB).append("*To examine the XML-formatted"
            + " HBase Schema Archive file in detail, simply open it in a browser or XML editor.");
    return stringBuilder.toString();
  }

  private StringBuilder appendSchemaEntityDescription(SchemaEntity entity, int indentSpaces) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(BLANKS, 0, indentSpaces).append(entity).append("\n");
    if (entity.getChildren() != null) {
      for (SchemaEntity childEntity : entity.getChildren()) {
        stringBuilder.append(appendSchemaEntityDescription(childEntity, indentSpaces + TAB));
      }
    }
    return stringBuilder;
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
        logger.info("  Element name: " + Bytes.toString(extractNameFromRowId(rowId)));
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

  ChangeEventMonitor buildChangeEventMonitor() throws IOException {
    ChangeEventMonitor changeEventMonitor = new ChangeEventMonitor();
    try (ResultScanner rows = repositoryTable.getScanner(new Scan().setMaxVersions())) {
      for (Result row : rows) {
        byte[] rowId = row.getRow();
        byte entityType = rowId[0];
        byte[] entityName = extractNameFromRowId(rowId);
        byte[] parentForeignKey = extractParentForeignKeyFromRowId(rowId);
        byte[] entityForeignKey = row.getValue(REPOSITORY_COLFAMILY, FOREIGN_KEY_COLUMN);
        Map<Long, byte[]> userNameKeyedByTimestampMap = new HashMap<>();
        for (Cell userNameCell : row.getColumnCells(REPOSITORY_COLFAMILY, JAVA_USERNAME_PROPERTY_KEY)) {
          userNameKeyedByTimestampMap.put(userNameCell.getTimestamp(),
                  Bytes.getBytes(CellUtil.getValueBufferShallowCopy(userNameCell)));
        }
        for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cfEntry : row.getMap().entrySet()) {
          for (Entry<byte[], NavigableMap<Long, byte[]>> colEntry
                  : cfEntry.getValue().entrySet()) {
            byte[] attributeName = colEntry.getKey();
            // bypass ColumnManager-maintained columns
            if (Bytes.equals(attributeName, FOREIGN_KEY_COLUMN)
                    || Bytes.equals(attributeName, JAVA_USERNAME_PROPERTY_KEY)
                    || Bytes.equals(attributeName, MAX_VALUE_COLUMN_NAME)) {
              continue;
            }
            for (Entry<Long, byte[]> cellEntry : colEntry.getValue().entrySet()) {
              long timestamp = cellEntry.getKey();
              byte[] attributeValue = cellEntry.getValue();
              byte[] userName = userNameKeyedByTimestampMap.get(timestamp);
              changeEventMonitor.add(new ChangeEvent(entityType, parentForeignKey, entityName,
                      entityForeignKey, attributeName,
                      timestamp, attributeValue, userName));
            }
          }
        }
      }
    }
    return changeEventMonitor.denormalize();
  }

  static void dropRepository(Admin hbaseAdmin, Logger logger) throws IOException {
    Admin standardAdmin = getStandardAdmin(hbaseAdmin);
    if (!standardAdmin.tableExists(REPOSITORY_TABLENAME)) {
      return;
    }
    logger.warn("DROP (disable/delete) of " + PRODUCT_NAME
            + " Repository table and namespace has been requested.");
    standardAdmin.disableTable(REPOSITORY_TABLENAME);
    standardAdmin.deleteTable(REPOSITORY_TABLENAME);
    logger.warn("DROP (disable/delete) of " + PRODUCT_NAME
            + " Repository table has been completed: "
            + REPOSITORY_TABLENAME.getNameAsString());
    standardAdmin.deleteNamespace(REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
    logger.warn("DROP (delete) of " + PRODUCT_NAME + " Repository namespace has been completed: "
            + REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
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

  private static boolean isPrintable(byte[] bytes) {
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
}
