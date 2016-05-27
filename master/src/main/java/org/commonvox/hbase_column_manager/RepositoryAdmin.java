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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTableMultiplexer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * A <b>RepositoryAdmin</b> provides ColumnManager repository maintenance and query facilities, as
 * well as schema metadata {@link #discoverSchema() discovery},
 * {@link #exportNamespaceSchema(java.lang.String, java.io.File, boolean) export}, and
 * {@link #importSchema(boolean, java.io.File) import}
 * facilities; it is used as a complement to the standard {@code Admin} interface, with an
 * {@code Admin} instance (provided by an {@link MConnectionFactory#createConnection()
 * MConnectionFactory-created Connection}) being used in a standard manner to maintain
 * <i>Namespace</i>, <i>Table</i>, and <i>Column Family</i> structures, and a
 * {@code RepositoryAdmin} instance being used to maintain {@link ColumnDefinition} structures and
 * to query {@link ColumnAuditor} structures.
 *
 * @author Daniel Vimont
 */
public class RepositoryAdmin implements Closeable {

  private final Logger logger;
  private final Connection hbaseConnection;
  private final Repository repository;

  /**
   * Initialize a RepositoryAdmin object using an already-created Connection. The Connection will
   * NOT be closed when this RepositoryAdmin object is closed.
   *
   * @param connection An HBase Connection.
   * @throws IOException if a remote or network exception occurs
   */
  public RepositoryAdmin(Connection connection) throws IOException {
    logger = Logger.getLogger(this.getClass().getPackage().getName());
    MConnection mConnection;
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      mConnection = (MConnection) connection;
    } else {
      mConnection = MConnectionFactory.getMConnection(connection);
    }
    this.hbaseConnection = mConnection.getStandardConnection();
    repository = mConnection.getRepository();
  }

  /**
   * Create namespace and table structures for Repository; note that these structures will
   * automatically be created with the first invocation of
   * {@link MConnectionFactory#createConnection()} if the structures do not already exist.
   *
   * @param hbaseAdmin standard HBase Admin object
   * @throws IOException if a remote or network exception occurs
   */
  public static void installRepositoryStructures(Admin hbaseAdmin)
          throws IOException {
    Repository.createRepositoryNamespace(hbaseAdmin);
    Repository.createRepositoryTable(hbaseAdmin);
  }

  /**
   * Allows administrative override of the default maxVersions setting for the Repository table. The
   * default setting is 50.
   *
   * @param hbaseAdmin Standard Admin object
   * @param maxVersions Maximum versions for repository to retain of each schema metadata attribute
   * @throws IOException if a remote or network exception occurs
   */
  public static void setRepositoryMaxVersions(Admin hbaseAdmin, int maxVersions)
          throws IOException {
    Repository.setRepositoryMaxVersions(hbaseAdmin, maxVersions);
  }

  /**
   * Get the maxVersions setting for the Repository table (maximum versions for repository to retain
   * of each schema metadata attribute).
   *
   * @param hbaseAdmin Standard Admin object
   * @return Maximum versions for repository to retain of each schema metadata attribute
   * @throws IOException if a remote or network exception occurs
   */
  public static int getRepositoryMaxVersions(Admin hbaseAdmin)
          throws IOException {
    return Repository.getRepositoryMaxVersions(hbaseAdmin);
  }

  /**
   * Disable and delete repository table and drop repository namespace (for uninstall or reinstall
   * of ColumnManager).
   *
   * @param hbaseAdmin standard HBase Admin
   * @throws IOException if a remote or network exception occurs
   */
  public static void uninstallRepositoryStructures(Admin hbaseAdmin) throws IOException {
    Repository.dropRepository(hbaseAdmin, Logger.getLogger(RepositoryAdmin.class.getPackage().getName()));
    InvalidColumnReport.dropTempReportNamespace(hbaseAdmin);
  }

  /**
   * Causes the complete contents (all cells) of the Repository table to be "dumped" (written) to
   * the logging facility, for maintenance or debugging purposes.
   *
   * @throws IOException if a remote or network exception occurs
   */
  public void dumpRepositoryTable() throws IOException {
    if (repository.isActivated()) {
      repository.dumpRepositoryTable();
    }
  }

  Repository getRepository() {
    return repository;
  }

  /**
   * Get {@link ColumnAuditor} objects from the repository for a specified <i>Table</i> and
   * <i>Column Family</i>.
   *
   * @param htd Table Descriptor
   * @param hcd Column [Family] Descriptor
   * @return set of {@link ColumnAuditor}s, or null if <i>Table</i> or <i>Column Family</i> not
   * found
   * @throws IOException if a remote or network exception occurs
   */
  public Set<ColumnAuditor> getColumnAuditors(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
    if (!repository.isIncludedTable(htd.getTableName())) {
      throw new TableNotIncludedForProcessingException(htd.getTableName().getName(), null);
    }
    if (MColumnDescriptor.class.isAssignableFrom(hcd.getClass())) {
      return ((MColumnDescriptor) hcd).getColumnAuditors();
    } else {
      return repository.getColumnAuditors(htd, hcd);
    }
  }

  /**
   * Get {@link ColumnAuditor} objects from the repository for a specified <i>Table</i> and
   * <i>Column Family</i>.
   *
   * @param tableName Table name
   * @param colFamily Column Family
   * @return set of {@link ColumnAuditor}s, or null if tableName or colFamily not found
   * @throws IOException if a remote or network exception occurs
   */
  public Set<ColumnAuditor> getColumnAuditors(TableName tableName, byte[] colFamily)
          throws IOException {
    if (!repository.isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    MTableDescriptor mtd = getRepository().getMTableDescriptor(tableName);
    if (mtd == null) {
      return null;
    }
    MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamily);
    if (mcd == null) {
      return null;
    }
    return RepositoryAdmin.this.getColumnAuditors(mtd, mcd);
  }

  /**
   * Get Column Qualifiers from the {@link ColumnAuditor} metadata in the repository for a specified
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param htd Table Descriptor
   * @param hcd Column [Family] Descriptor
   * @return set of Column Qualifiers, or null if <i>Table</i> or <i>Column Family</i> not found
   * @throws IOException if a remote or network exception occurs
   */
  public Set<byte[]> getColumnQualifiers(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
    if (!repository.isIncludedTable(htd.getTableName())) {
      throw new TableNotIncludedForProcessingException(htd.getTableName().getName(), null);
    }
     Set<byte[]> columnQualifiers = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
    if (MColumnDescriptor.class.isAssignableFrom(hcd.getClass())) {
      return ((MColumnDescriptor) hcd).getColumnQualifiers();
    } else {
      Set<ColumnAuditor> columnAuditors = repository.getColumnAuditors(htd, hcd);
      if (columnAuditors == null) {
        return null;
      }
      for (ColumnAuditor columnAuditor : columnAuditors) {
        columnQualifiers.add(columnAuditor.getName());
      }
      return columnQualifiers;
    }
  }

  /**
   * Get Column Qualifiers from the {@link ColumnAuditor} metadata in the repository for a specified
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param tableName Table name
   * @param colFamily Column Family
   * @return set of Column Qualifiers, or null if tableName or colFamily not found
   * @throws IOException if a remote or network exception occurs
   */
  public Set<byte[]> getColumnQualifiers(TableName tableName, byte[] colFamily)
          throws IOException {
    if (!repository.isIncludedTable(tableName)) {
      throw new TableNotIncludedForProcessingException(tableName.getName(), null);
    }
    MTableDescriptor mtd = repository.getMTableDescriptor(tableName);
    if (mtd == null) {
      return null;
    }
    MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamily);
    if (mcd == null) {
      return null;
    }
    return mcd.getColumnQualifiers();
  }

  /**
   * Add (or modify, if already existing) the submitted {@link ColumnDefinition} to the submitted
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param tableName name of <i>Table</i> to which {@link ColumnDefinition} is to be added
   * @param colFamily <i>Column Family</i> to which {@link ColumnDefinition} is to be added
   * @param colDefinition {@link ColumnDefinition} to be added or modified
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumnDefinition(TableName tableName, byte[] colFamily,
          final ColumnDefinition colDefinition)
          throws IOException {
    List<ColumnDefinition> colDefinitions
            = new ArrayList<ColumnDefinition>() { { add(colDefinition); } };
    repository.putColumnDefinitionSchemaEntities(tableName, colFamily, colDefinitions);
  }

  /**
   * Add (or modify, if already existing) the submitted {@link ColumnDefinition} to the submitted
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param htd <i>Table</i> to which {@link ColumnDefinition} is to be added
   * @param hcd <i>ColumnAuditor [Family] Descriptor</i> to which {@link ColumnDefinition} is to be
   * added
   * @param colDefinition {@link ColumnDefinition} to be added or modified
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumnDefinition(HTableDescriptor htd, HColumnDescriptor hcd,
          ColumnDefinition colDefinition)
          throws IOException {
    addColumnDefinition(htd.getTableName(), hcd.getName(), colDefinition);
  }

  /**
   * Add (or modify, if already existing) the submitted {@link ColumnDefinition}s to the submitted
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param tableName name of <i>Table</i> to which {@link ColumnDefinition}s are to be added
   * @param colFamily <i>Column Family</i> to which {@link ColumnDefinition}s are to be added
   * @param colDefinitions List of {@link ColumnDefinition}s to be added or modified
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumnDefinitions(TableName tableName, byte[] colFamily,
          List<ColumnDefinition> colDefinitions)
          throws IOException {
    repository.putColumnDefinitionSchemaEntities(tableName, colFamily, colDefinitions);
  }

  /**
   * Add (or modify, if already existing) the submitted {@link ColumnDefinition}s to the submitted
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param htd <i>Table</i> to which {@link ColumnDefinition}s are to be added
   * @param hcd <i>ColumnAuditor [Family] Descriptor</i> to which {@link ColumnDefinition}s are to
   * be added
   * @param colDefinitions List of {@link ColumnDefinition}s to be added or modified
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumnDefinitions(HTableDescriptor htd, HColumnDescriptor hcd,
          List<ColumnDefinition> colDefinitions)
          throws IOException {
    repository.putColumnDefinitionSchemaEntities(htd.getTableName(), hcd.getName(), colDefinitions);
  }

  /**
   * Get {@link ColumnDefinition}s belonging to the submitted <i>Table</i> and
   * <i>Column Family</i>.
   *
   * @param htd <i>Table</i> to which {@link ColumnDefinition}s belong
   * @param hcd <i>ColumnAuditor [Family] Descriptor</i> to which {@link ColumnDefinition}s belong
   * @return collection of {@link ColumnDefinition}s
   * @throws IOException if a remote or network exception occurs
   */
  public Collection<ColumnDefinition> getColumnDefinitions(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
    if (MColumnDescriptor.class.isAssignableFrom(hcd.getClass())) {
      return ((MColumnDescriptor) hcd).getColumnDefinitions();
    } else {
      return repository.getColumnDefinitions(htd, hcd);
    }
  }

  /**
   * Get {@link ColumnDefinition}s belonging to the submitted <i>Table</i> and
   * <i>Column Family</i>.
   *
   * @param tableName name of <i>Table</i> to which {@link ColumnDefinition}s belong
   * @param colFamily <i>Column Family</i> to which {@link ColumnDefinition}s belong
   * @return collection of {@link ColumnDefinition}s
   * @throws IOException if a remote or network exception occurs
   */
  public Collection<ColumnDefinition> getColumnDefinitions(TableName tableName, byte[] colFamily)
          throws IOException {
    MTableDescriptor mtd = getRepository().getMTableDescriptor(tableName);
    MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamily);
    return getColumnDefinitions(mtd, mcd);
  }

  /**
   * Delete the {@link ColumnDefinition} pertaining to the submitted <i>Column Qualifier</i>
   * from the submitted <i>Table</i> and <i>Column Family</i>.
   *
   * @param tableName name of <i>Table</i> from which {@link ColumnDefinition} is to be deleted
   * @param colFamily <i>Column Family</i> from which {@link ColumnDefinition} is to be deleted
   * @param colQualifier qualifier which uniquely identifies the {@link ColumnDefinition} to be
   * deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumnDefinition(TableName tableName, byte[] colFamily, byte[] colQualifier)
          throws IOException {
    repository.deleteColumnDefinition(tableName, colFamily, colQualifier);
  }

  /**
   * Delete the {@link ColumnDefinition} pertaining to the submitted <i>Column Qualifier</i>
   * from the submitted <i>Table</i> and <i>Column Family</i>.
   *
   * @param htd <i>Table</i> from which {@link ColumnDefinition} is to be deleted
   * @param hcd <i>ColumnAuditor [Family] Descriptor</i> from which {@link ColumnDefinition} is to
   * be deleted
   * @param colQualifier qualifier which uniquely identifies {@link ColumnDefinition} to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumnDefinition(HTableDescriptor htd, HColumnDescriptor hcd,
          byte[] colQualifier)
          throws IOException {
    repository.deleteColumnDefinition(htd.getTableName(), hcd.getName(), colQualifier);
  }

  /**
   * Ascertains whether or not column definition enforcement is enabled for a specified <i>Column
   * Family</i>.
   *
   * @param tableName Name of <i>Table</i> to which <i>Column Family</i> belongs.
   * @param colFamily Name of <i>Column Family</i> for which column definition enforcement is to be
   * ascertained
   * @return {@code true} if column definition enforcement is enabled for the specified <i>Column
   * Family</i>; {@code false} if column definition enforcement is not enabled
   * @throws IOException if a remote or network exception occurs
   */
  public boolean columnDefinitionsEnforced(TableName tableName, byte[] colFamily)
          throws IOException {
    return repository.columnDefinitionsEnforced(tableName, colFamily);
  }

  /**
   * Enable/disable column definition enforcement for the specified <i>Column Family</i>. When
   * <a href="package-summary.html#activate">ColumnManager is activated</a> and column definition
   * enforcement is enabled for a <i>Column Family</i>
   * of a <i>Table</i> that is <a href="package-summary.html#config">included in ColumnManager
   * processing</a>, then any <i>Column qualifier</i> submitted in a {@code Mutation} to the
   * <i>Table</i> and
   * <i>Column Family</i> (via the HBase API) must correspond to an existing
   * {@link ColumnDefinition}, and the corresponding <i>Column value</i> submitted must pass all
   * validations (if any) stipulated by the {@code ColumnDefinition}.
   *
   * @param enabled if {@code true}, column definition enforcement is enabled; if {@code false}, it
   * is disabled
   * @param tableName Name of <i>Table</i> to which <i>Column Family</i> belongs
   * @param colFamily Name of <i>Column Family</i> for which column definition enforcement is to be
   * enabled or disabled
   * @throws IOException if a remote or network exception occurs
   */
  public void setColumnDefinitionsEnforced(boolean enabled, TableName tableName, byte[] colFamily)
          throws IOException {
    repository.setColumnDefinitionsEnforced(enabled, tableName, colFamily);
  }

  /**
   * Get a {@link ChangeEventMonitor} object
   *
   * @return {@link ChangeEventMonitor} object
   * @throws IOException if a remote or network exception occurs
   */
  public ChangeEventMonitor getChangeEventMonitor() throws IOException {
    return repository.buildChangeEventMonitor();
  }

  MTableDescriptor getMTableDescriptor(TableName tn)
          throws IOException {
    if (!repository.isActivated()) {
      return null;
    }
    return new MTableDescriptor(repository.getAdmin().getTableDescriptor(tn), repository);
  }

  MTableDescriptor getMTableDescriptorFromRepository(TableName tn)
          throws IOException {
    if (!repository.isActivated()) {
      return null;
    }
    return repository.getMTableDescriptor(tn);
  }

  /**
   *
   * @param namespaceName
   * @return
   * @throws IOException if a remote or network exception occurs
   */
  MNamespaceDescriptor getMNamespaceDescriptorFromRepository(String namespaceName)
          throws IOException {
    if (!repository.isActivated()) {
      return null;
    }
    return repository.getMNamespaceDescriptor(namespaceName);
  }

  /**
   * Create an HTableMultiplexer object.<br><br>
   * <b>SPECIAL NOTE:</b> An HTableMultiplexer returned by this method will (1) validate submitted
   * <i>Column</i> qualifiers and values (if
   * {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
   * ColumnDefinitionsEnforced} is set to {@code true} for the related <i>Column Family</i>), (2)
   * process "put" requests in a standard manner (queuing them for subsequent <b>asynchronous</b>
   * processing by HBase) and then (3) perform <b>synchronous</b> ColumnManager repository
   * processing upon the metadata of each successfully queued "put". <i>Be advised that the third
   * step will not take into account any potentially failed "put" transactions among those queued in
   * the second step; instead it assumes that all "put" transactions succeeded, updating the
   * ColumnManager repository accordingly.</i>
   *
   * @param perRegionServerBufferQueueSize determines the max number of the buffered Put ops for
   * each region server before dropping a request.
   * @return HTableMultiplexer object
   * @throws IOException if a remote or network exception occurs
   */
  public HTableMultiplexer createHTableMultiplexer(int perRegionServerBufferQueueSize)
          throws IOException {
    return new MTableMultiplexer(repository, hbaseConnection.getConfiguration(),
            perRegionServerBufferQueueSize);
  }

  /**
   * Performs discovery of all ColumnManager-included user <i>Table</i>s and stores the schema
   * metadata in
   * the ColumnManager repository; includes discovery of {@link ColumnAuditor} schema metadata
   * (performing full scan [with KeyOnlyFilter] of all <i>Table</i>s that are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>).
   *
   * @throws IOException if a remote or network exception occurs
   */
  public void discoverSchema() throws IOException {
    repository.discoverSchema(true);
  }

  /**
   * Performs discovery of the specified <i>Table</i>'s schema metadata and stores it in the
   * ColumnManager repository; includes discovery of {@link ColumnAuditor} schema metadata
   * (performing full scan [with KeyOnlyFilter] of the <i>Table</i>).
   * Note that the specified <i>Table</i>
   * must be <a href="package-summary.html#config">included in ColumnManager processing</a>;
   * otherwise invocation of this method will have no effect.
   *
   * @param tableName <i>Table</i> for which schema metadata is to be discovered; submitted
   * <i>Table</i>
   * must be <a href="package-summary.html#config">included in ColumnManager processing</a>
   * @throws IOException if a remote or network exception occurs
   */
  public void discoverSchema(TableName tableName) throws IOException {
    repository.discoverSchema(tableName, true);
  }

  // make this method public if needs dictate
  void purgeTableSchemaEntity(TableName tn) throws IOException {
    repository.purgeTableSchemaEntity(tn);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * metadata contents (i.e., all <i>Namespace</i>, <i>Table</i>, <i>Column Family</i>,
   * {@link ColumnAuditor}, and {@link ColumnDefinition} metadata) of the ColumnManager metadata
   * repository. In effect, this is an XML-formatted serialization of objects of the following
   * classes: {@code NamespaceDescriptor}, {@link MTableDescriptor}, {@link MColumnDescriptor},
   * {@link ColumnAuditor}, and {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param targetFile target file
   * @param formatted if <b>true</b>, insert whitespace (linefeeds and hierarchical indentations)
   * between XML elements to produce human-readable XML.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void exportRepository(File targetFile,
          boolean formatted)
          throws IOException, JAXBException {
    repository.exportSchema(null, null, targetFile, formatted);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * schema contents (i.e., the <i>Namespace</i>, <i>Table</i>, <i>Column Family</i>,
   * {@link ColumnAuditor}, and {@link ColumnDefinition} metadata) of the specified HBase
   * <i>Namespace</i>. In effect, this is an XML-formatted serialization of objects of the following
   * classes: NamespaceDescriptor, {@link MTableDescriptor}, {@link MColumnDescriptor},
   * {@link ColumnAuditor}, and {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceNamespaceName namespace from which to export schema
   * @param targetFile target file
   * @param formatted if <b>true</b>, insert whitespace (linefeeds and hierarchical indentations)
   * between XML elements to produce human-readable XML.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void exportNamespaceSchema(
          String sourceNamespaceName, File targetFile, boolean formatted)
          throws IOException, JAXBException {
    repository.exportSchema(sourceNamespaceName, null, targetFile, formatted);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * schema contents (i.e., the <i>Table</i>, <i>Column Family</i>, {@link ColumnAuditor}, and
   * {@link ColumnDefinition} metadata) of the specified HBase <i>Table</i>. In effect, this is an
   * XML-formatted serialization of objects of the following classes:
   * {@link MTableDescriptor}, {@link MColumnDescriptor}, {@link ColumnAuditor}, and
   * {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceTableName table to exportSchema
   * @param targetFile target File
   * @param formatted if <b>true</b>, insert whitespace (linefeeds and hierarchical indentations)
   * between XML elements to produce human-readable XML.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void exportTableSchema(TableName sourceTableName, File targetFile, boolean formatted)
          throws IOException, JAXBException {
    repository.exportSchema(sourceTableName.getNamespaceAsString(), sourceTableName,
            targetFile, formatted);
  }

  /**
   * Import into HBase the complete contents of an external HBaseSchemaArchive (HSA) XML file*. This
   * process will NOT overlay any existing Namespace and Table structures in HBase. For any
   * Namespace definition in the external file which does NOT correspond to an existing HBase
   * Namespace, a new Namespace will be added to HBase along with any Tables and table components
   * belonging to the Namespace. For any Namespace definition in the external file which corresponds
   * to an existing HBase Namespace, any of its Table and table components will only be added to
   * HBase if the Table does NOT already exist in HBase.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param includeColumnAuditors if <b>true</b>, import {@link ColumnAuditor} metadata from the
   * HBaseSchemaArchive file into the ColumnManager repository.
   * @param sourceFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void importSchema(boolean includeColumnAuditors, File sourceFile)
          throws IOException, JAXBException {
    repository.importSchema(includeColumnAuditors, null, null, sourceFile);
  }

  /**
   * Import into HBase a Namespace and all its component objects as represented in an external
   * HBaseSchemaArchive (HSA) XML file*. This process will NOT overlay any existing Namespace and
   * Table structures in HBase. If the Namespace definition in the external file does NOT correspond
   * to an existing HBase Namespace, a new Namespace will be added to HBase along with any Tables
   * and table components belonging to the Namespace. If the Namespace definition in the external
   * file corresponds to an existing HBase Namespace, any of its Table and table components will
   * only be added to HBase if the Table does NOT already exist in HBase.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param includeColumnAuditors if <b>true</b>, import {@link ColumnAuditor} metadata from the
   * HBaseSchemaArchive file into the ColumnManager repository.
   * @param namespaceName namespace to import.
   * @param sourceFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void importNamespaceSchema(boolean includeColumnAuditors,
          String namespaceName, File sourceFile)
          throws IOException, JAXBException {
    repository.importSchema(includeColumnAuditors, namespaceName, null, sourceFile);
  }

  /**
   * Import into HBase a Table and all its component objects as represented in an external
   * HBaseSchemaArchive (HSA) XML file*. This process will NOT overlay any existing Namespace and
   * Table structures in HBase. If the Table's parent Namespace does NOT correspond to an existing
   * HBase Namespace, a new Namespace will be added to HBase. The Table and table components will
   * only be added to HBase if the Table does NOT already exist in HBase.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param includeColumnAuditors if <b>true</b>, import {@link ColumnAuditor} metadata from the
   * HBaseSchemaArchive file into the ColumnManager repository.
   * @param tableName Name of table to be imported.
   * @param sourceFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void importTableSchema(
          boolean includeColumnAuditors, TableName tableName, File sourceFile)
          throws IOException, JAXBException {
    repository.importSchema(
            includeColumnAuditors, tableName.getNamespaceAsString(), tableName, sourceFile);
  }

  /**
   * Generates a hierarchically-indented, text-based summary report of the contents of an external
   * HBaseSchemaArchive (HSA) XML file*.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceFile source file
   * @return A String containing a summary report suitable for printing/viewing.
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public String generateHsaFileSummary(File sourceFile) throws JAXBException {
    return repository.getHBaseSchemaArchiveSummary(sourceFile);
  }

  /**
   * Generates and outputs a CSV-formatted report of all invalid column qualifiers stored in a
   * Table, as stipulated by the Table's {@link ColumnDefinition}s.
   * If no {@link ColumnDefinition}s exist for any of the Table's Column Families, a
   * {@link ColumnDefinitionNotFoundException} will be thrown.
   *
   * @param tableName name of the Table to be reported on
   * @param targetFile file to which the CSV file is to be outputted
   * @param verbose if {@code true} the outputted CSV file will include an entry (identified by
   * the fully-qualified column name and rowId) for each explicit invalid column qualifier that is
   * found; otherwise the report will contain a summary, with one line for each invalid column
   * qualifier found, along with a count of the number of rows which contain that same invalid
   * column qualifier.
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   */
  public boolean generateReportOnInvalidColumnQualifiers(TableName tableName, File targetFile,
          boolean verbose, boolean useMapreduce) throws IOException {
    return generateReportOnInvalidColumnQualifiers(
            tableName, null, targetFile, verbose, useMapreduce);
  }

  /**
   * Generates and outputs a CSV-formatted report of all invalid column qualifiers stored in a
   * Column Family, as stipulated by the Column Family's {@link ColumnDefinition}s.
   * If no {@link ColumnDefinition}s exist for the Column Family, a
   * {@link ColumnDefinitionNotFoundException} will be thrown.
   *
   * @param targetFile file to which the CSV file is to be outputted
   * @param tableName name of the Table containing the Column Family to be reported on
   * @param colFamily name of the Column Family to be reported on
   * @param verbose if {@code true} the outputted CSV file will include an entry (identified by
   * the fully-qualified column name and rowId) for each explicit invalid column qualifier that is
   * found; otherwise the report will contain a summary, with one line for each invalid column
   * qualifier found, along with a count of the number of rows which contain that same invalid
   * column qualifier.
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   */
  public boolean generateReportOnInvalidColumnQualifiers(TableName tableName, byte[] colFamily,
          File targetFile, boolean verbose, boolean useMapreduce) throws IOException {
    return repository.generateReportOnInvalidColumns(InvalidColumnReport.ReportType.QUALIFIER,
            tableName, colFamily, targetFile, verbose, useMapreduce);
  }

  /**
   * Generates and outputs a CSV-formatted report of all invalid lengths of column values stored
   * in a Table, as stipulated by the {@link ColumnDefinition#setColumnLength(long) ColumnLength}
   * settings in the Table's {@link ColumnDefinition}s.
   * If no {@link ColumnDefinition}s exist for any of the Table's Column Families, a
   * {@link ColumnDefinitionNotFoundException} will be thrown.
   *
   * @param tableName name of the Table to be reported on
   * @param targetFile file to which the CSV file is to be outputted
   * @param verbose if {@code true} the outputted CSV file will include an entry (identified by
   * the fully-qualified column name and rowId) for each explicit invalid column length that is
   * found; otherwise the report will contain a summary, with one line for each column found
   * to contain at least a single instance of an invalid column length, along with a count of the
   * number of rows in which that column contains an invalid column length.
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   */
  public boolean generateReportOnInvalidColumnLengths(TableName tableName, File targetFile,
          boolean verbose, boolean useMapreduce) throws IOException {
    return generateReportOnInvalidColumnLengths(
            tableName, null, targetFile, verbose, useMapreduce);
  }

  /**
   * Generates and outputs a CSV-formatted report of all invalid lengths of column values stored
   * in a Column Family, as stipulated by the
   * {@link ColumnDefinition#setColumnLength(long) ColumnLength}
   * settings in the Column Family's {@link ColumnDefinition}s.
   * If no {@link ColumnDefinition}s exist for the Column Family, a
   * {@link ColumnDefinitionNotFoundException} will be thrown.
   *
   * @param targetFile file to which the CSV file is to be outputted
   * @param tableName name of the Table containing the Column Family to be reported on
   * @param colFamily name of the Column Family to be reported on
   * @param verbose if {@code true} the outputted CSV file will include an entry (identified by
   * the fully-qualified column name and rowId) for each explicit invalid column length that is
   * found; otherwise the report will contain a summary, with one line for each column found
   * to contain at least a single instance of an invalid column length, along with a count of the
   * number of rows in which that column contains an invalid column length.
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   */
  public boolean generateReportOnInvalidColumnLengths(TableName tableName, byte[] colFamily,
          File targetFile, boolean verbose, boolean useMapreduce) throws IOException {
    return repository.generateReportOnInvalidColumns(InvalidColumnReport.ReportType.LENGTH,
            tableName, colFamily, targetFile, verbose, useMapreduce);
  }

  /**
   * Generates and outputs a CSV-formatted report of all invalid column values stored
   * in a Table, as stipulated by the
   * {@link ColumnDefinition#setColumnValidationRegex(java.lang.String)
   * ValidationRegex} settings in the Table's {@link ColumnDefinition}s.
   * If no {@link ColumnDefinition}s exist for any of the Table's Column Families, a
   * {@link ColumnDefinitionNotFoundException} will be thrown.
   *
   * @param tableName name of the Table to be reported on
   * @param targetFile file to which the CSV file is to be outputted
   * @param verbose if {@code true} the outputted CSV file will include an entry (identified by
   * the fully-qualified column name and rowId) for each explicit invalid column value that is
   * found; otherwise the report will contain a summary, with one line for each column found
   * to contain at least a single instance of an invalid column value, along with a count of the
   * number of rows in which that column contains an invalid column value.
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   */
  public boolean generateReportOnInvalidColumnValues(TableName tableName, File targetFile,
          boolean verbose, boolean useMapreduce) throws IOException {
    return generateReportOnInvalidColumnValues(
            tableName, null, targetFile, verbose, useMapreduce);
  }

  /**
   * Generates and outputs a CSV-formatted report of all invalid column values stored
   * in a Column Family, as stipulated by the
   * {@link ColumnDefinition#setColumnValidationRegex(java.lang.String)
   * ValidationRegex} settings in the Column Family's {@link ColumnDefinition}s.
   * If no {@link ColumnDefinition}s exist for the Column Family, a
   * {@link ColumnDefinitionNotFoundException} will be thrown.
   *
   * @param targetFile file to which the CSV file is to be outputted
   * @param tableName name of the Table containing the Column Family to be reported on
   * @param colFamily name of the Column Family to be reported on
   * @param verbose if {@code true} the outputted CSV file will include an entry (identified by
   * the fully-qualified column name and rowId) for each explicit invalid column value that is
   * found; otherwise the report will contain a summary, with one line for each column found
   * to contain at least a single instance of an invalid column value, along with a count of the
   * number of rows in which that column contains an invalid column value.
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   */
  public boolean generateReportOnInvalidColumnValues(TableName tableName, byte[] colFamily,
          File targetFile, boolean verbose, boolean useMapreduce) throws IOException {
    return repository.generateReportOnInvalidColumns(InvalidColumnReport.ReportType.VALUE,
            tableName, colFamily, targetFile, verbose, useMapreduce);
  }

  /**
   * Namespace existence verification.
   *
   * @param nd NamespaceDescriptor
   * @return true if namespace exists
   * @throws IOException if a remote or network exception occurs
   */
  public boolean namespaceExists(NamespaceDescriptor nd) throws IOException {
    return repository.namespaceExists(nd.getName());
  }

  /**
   * Namespace existence verification.
   *
   * @param namespaceName name of namespace
   * @return true if namespace exists
   * @throws IOException if a remote or network exception occurs
   */
  public boolean namespaceExists(String namespaceName) throws IOException {
    return repository.namespaceExists(namespaceName);
  }

  @Override
  public void close() throws IOException {
    // hbaseConnection NOT closed here (Connection instantiated externally; is presumably shared)
  }
}
