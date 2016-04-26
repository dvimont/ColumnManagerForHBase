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
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTableMultiplexer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * A <b>RepositoryAdmin</b> provides ColumnManager repository maintenance and query facilities, as
 * well as metadata {@link #discoverMetadata() discovery},
 * {@link #exportNamespaceMetadata(java.lang.String, java.lang.String, java.lang.String, boolean)
 * export}, and {@link #importMetadata(boolean, java.lang.String, java.lang.String) import}
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
    if (MConnection.class.isAssignableFrom(connection.getClass())) {
      MConnection mConnection = (MConnection) connection;
      this.hbaseConnection = mConnection.getWrappedConnection();
      repository = mConnection.getRepository();
    } else {
      this.hbaseConnection = connection;
      repository = new Repository(this.hbaseConnection, this);
    }
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
   * @param maxVersions Maximum versions for repository to retain of each metadata attribute
   * @throws IOException if a remote or network exception occurs
   */
  public static void setRepositoryMaxVersions(Admin hbaseAdmin, int maxVersions)
          throws IOException {
    Repository.setRepositoryMaxVersions(hbaseAdmin, maxVersions);
  }

  /**
   * Get the maxVersions setting for the Repository table (maximum versions for repository to retain
   * of each metadata attribute).
   *
   * @param hbaseAdmin Standard Admin object
   * @return Maximum versions for repository to retain of each metadata attribute
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
  public Collection<ColumnAuditor> getColumnAuditors(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException {
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
  public Collection<ColumnAuditor> getColumnAuditors(TableName tableName, byte[] colFamily)
          throws IOException {
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
    MTableDescriptor mtd = getRepository().getMTableDescriptor(tableName);
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
            = new ArrayList<ColumnDefinition>() {
              {
                add(colDefinition);
              }
            };
    repository.putColumnDefinitions(tableName, colFamily, colDefinitions);
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
    repository.putColumnDefinitions(tableName, colFamily, colDefinitions);
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
    repository.putColumnDefinitions(htd.getTableName(), hcd.getName(), colDefinitions);
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
   * Get an {@link ChangeEventMonitor} object
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
    // repository.getAdmin().getTableDescriptor(tn); // throws TableNotFoundException if Table not found
    MTableDescriptor mtd = repository.getMTableDescriptor(tn);
//        if (mtd == null) {
//            throw new TableMetadataNotFoundException(tn.getNameAsString());
//        }
    return mtd;
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
    // repository.getAdmin().getNamespaceDescriptor(namespaceName); // throws NamespaceNotFoundException if not found
    MNamespaceDescriptor nd = repository.getMNamespaceDescriptor(namespaceName);
//        if (nd == null) {
//            throw new NamespaceMetadataNotFoundException(namespaceName);
//        }
    return nd;
  }

  /**
   * Create an HTableMultiplexer object.<br><br>
   * <b>SPECIAL NOTE:</b> An HTableMultiplexer returned by this method will (1) validate submitted
   * <i>Column</i> qualifiers and values (if {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
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
   * Performs discovery of all ColumnManager-included user <i>Table</i>s and stores the metadata in
   * the ColumnManager repository; includes discovery of {@link ColumnAuditor} metadata (performing
   * full scan of all <i>Table</i>s that are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>).
   *
   * @throws IOException if a remote or network exception occurs
   */
  public void discoverMetadata() throws IOException {
    repository.discoverMetadata(true);
  }

  /**
   * Performs discovery of the specified <i>Table</i>'s metadata and stores the metadata in the
   * ColumnManager repository; includes discovery of {@link ColumnAuditor} metadata (performing full
   * scan of the <i>Table</i>). Note that the specified <i>Table</i>
   * must be <a href="package-summary.html#config">included in ColumnManager processing</a>;
   * otherwise invocation of this method will have no effect.
   *
   * @param tableName <i>Table</i> for which metadata is to be discovered; submitted <i>Table</i>
   * must be <a href="package-summary.html#config">included in ColumnManager processing</a>
   * @throws IOException if a remote or network exception occurs
   */
  public void discoverMetadata(TableName tableName) throws IOException {
    repository.discoverMetadata(tableName, true);
  }

  // make this method public if needs dictate
  void purgeTableMetadata(TableName tn) throws IOException {
    repository.purgeTableMetadata(tn);
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
   * @param targetPathString path to which target file should be written.
   * @param targetFileNameString file name to assign to target file.
   * @param formatted if <b>true</b>, insert whitespace (linefeeds and hierarchical indentations)
   * between XML elements to produce human-readable XML.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void exportRepository(String targetPathString, String targetFileNameString,
          boolean formatted)
          throws IOException, JAXBException {
    repository.exportMetadata(null, null, targetPathString, targetFileNameString, formatted);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * metadata contents (i.e., the <i>Namespace</i>, <i>Table</i>, <i>Column Family</i>,
   * {@link ColumnAuditor}, and {@link ColumnDefinition} metadata) of the specified HBase
   * <i>Namespace</i>. In effect, this is an XML-formatted serialization of objects of the following
   * classes: NamespaceDescriptor, {@link MTableDescriptor}, {@link MColumnDescriptor},
   * {@link ColumnAuditor}, and {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceNamespaceName namespace from which to export metadata
   * @param targetPathString path to which target file should be written.
   * @param targetFileNameString file name to assign to target file.
   * @param formatted if <b>true</b>, insert whitespace (linefeeds and hierarchical indentations)
   * between XML elements to produce human-readable XML.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void exportNamespaceMetadata(String sourceNamespaceName,
          String targetPathString, String targetFileNameString,
          boolean formatted)
          throws IOException, JAXBException {
    repository.exportMetadata(sourceNamespaceName, null,
            targetPathString, targetFileNameString, formatted);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * metadata contents (i.e., the <i>Table</i>, <i>Column Family</i>, {@link ColumnAuditor}, and
   * {@link ColumnDefinition} metadata) of the specified HBase <i>Table</i>. In effect, this is an
   * XML-formatted serialization of objects of the following classes:
   * {@link MTableDescriptor}, {@link MColumnDescriptor}, {@link ColumnAuditor}, and
   * {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceTableName table to exportMetadata
   * @param targetPathString path to which target file should be written.
   * @param targetFileNameString file name to assign to target file.
   * @param formatted if <b>true</b>, insert whitespace (linefeeds and hierarchical indentations)
   * between XML elements to produce human-readable XML.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void exportTableMetadata(TableName sourceTableName,
          String targetPathString, String targetFileNameString,
          boolean formatted)
          throws IOException, JAXBException {
    repository.exportMetadata(sourceTableName.getNamespaceAsString(), sourceTableName,
            targetPathString, targetFileNameString, formatted);
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
   * @param sourcePathString path from which source file should be read.
   * @param sourceFileNameString name of source file.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void importMetadata(boolean includeColumnAuditors,
          String sourcePathString, String sourceFileNameString)
          throws IOException, JAXBException {
    submitImportLoggerMessages(includeColumnAuditors, null, null,
            sourcePathString, sourceFileNameString);
    Set<Object> importedDescriptors
            = repository.deserializeHBaseSchemaArchive(includeColumnAuditors, null, null,
                    sourcePathString, sourceFileNameString);
    createImportedStructures(includeColumnAuditors, importedDescriptors);
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
   * @param sourcePathString path from which source file should be read.
   * @param sourceFileNameString name of source file.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void importNamespaceMetadata(boolean includeColumnAuditors,
          String namespaceName, String sourcePathString,
          String sourceFileNameString)
          throws IOException, JAXBException {
    submitImportLoggerMessages(includeColumnAuditors, namespaceName, null,
            sourcePathString, sourceFileNameString);
    Set<Object> importedDescriptors
            = repository.deserializeHBaseSchemaArchive(includeColumnAuditors, namespaceName, null,
                    sourcePathString, sourceFileNameString);
    createImportedStructures(includeColumnAuditors, importedDescriptors);
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
   * @param sourcePathString path from which source file should be read.
   * @param sourceFileNameString name of source file.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void importTableMetadata(boolean includeColumnAuditors,
          TableName tableName, String sourcePathString,
          String sourceFileNameString)
          throws IOException, JAXBException {
    submitImportLoggerMessages(includeColumnAuditors, null, tableName,
            sourcePathString, sourceFileNameString);
    Set<Object> importedDescriptors
            = repository.deserializeHBaseSchemaArchive(includeColumnAuditors,
                    tableName.getNamespaceAsString(), tableName,
                    sourcePathString, sourceFileNameString);
    createImportedStructures(includeColumnAuditors, importedDescriptors);
  }

  private void submitImportLoggerMessages(boolean includeColumnAuditors,
          String namespaceName, TableName tableName,
          String sourcePathString, String sourceFileNameString) {
    logger.info("IMPORT of metadata "
            + ((includeColumnAuditors) ? "<INCLUDING COLUMN AUDITOR METADATA> " : "")
            + "from external HBaseSchemaArchive (XML) file has been requested.");
    if (namespaceName != null && !namespaceName.isEmpty()) {
      logger.info("IMPORT NAMESPACE: " + namespaceName);
    }
    if (tableName != null && !tableName.getNameAsString().isEmpty()) {
      logger.info("IMPORT TABLE: " + tableName.getNameAsString());
    }
    logger.info("IMPORT source PATH/FILE-NAME: "
            + sourcePathString + "/" + sourceFileNameString);
  }

  private void createImportedStructures(boolean includeColumnAuditors,
          Set<Object> importedDescriptors)
          throws IOException {
    for (Object descriptor : importedDescriptors) {
      if (MNamespaceDescriptor.class.isAssignableFrom(descriptor.getClass())) {
        NamespaceDescriptor nd
                = ((MNamespaceDescriptor) descriptor).getNamespaceDescriptor();
        if (!namespaceExists(nd)) {
          repository.getAdmin().createNamespace(nd);
          repository.putNamespace(nd);
          logger.info("IMPORT COMPLETED FOR NAMESPACE: " + nd.getName());
        }
      } else if (MTableDescriptor.class.isAssignableFrom(descriptor.getClass())) {
        MTableDescriptor mtd = (MTableDescriptor) descriptor;
        if (!repository.getAdmin().tableExists(mtd.getTableName())) {
          repository.getAdmin().createTable(mtd); // includes creation of Column Families
          repository.putTable(mtd);
          repository.putColumnDefinitions(mtd);
          if (includeColumnAuditors) {
            repository.putColumnAuditors(mtd);
          }
          logger.info("IMPORT COMPLETED FOR TABLE: " + mtd.getNameAsString()
                  + (includeColumnAuditors ? " <INCLUDING COLUMN AUDITOR METADATA>" : ""));
        }
      }
    }
  }

  /**
   * Generates a hierarchically-indented, text-based summary report of the contents of an external
   * HBaseSchemaArchive (HSA) XML file*.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourcePathString path in which source file is stored
   * @param sourceFileNameString name of source file
   * @return A String containing a summary report suitable for printing/viewing.
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public String generateHmaFileSummary(String sourcePathString, String sourceFileNameString)
          throws JAXBException {
    return repository
            .getHBaseSchemaArchiveSummary(sourcePathString, sourceFileNameString);
  }

  /**
   * Namespace existence verification.
   *
   * @param nd NamespaceDescriptor
   * @return true if namespace exists
   * @throws IOException if a remote or network exception occurs
   */
  public boolean namespaceExists(NamespaceDescriptor nd) throws IOException {
    return namespaceExists(nd.getName());
  }

  /**
   * Namespace existence verification.
   *
   * @param namespaceName name of namespace
   * @return true if namespace exists
   * @throws IOException if a remote or network exception occurs
   */
  public boolean namespaceExists(String namespaceName)
          throws IOException {
    try {
      repository.getAdmin().getNamespaceDescriptor(namespaceName);
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    // hbaseConnection NOT closed here (Connection instantiated externally; is presumably shared)
  }
}
