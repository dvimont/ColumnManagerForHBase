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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
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
 * A <b>RepositoryAdmin</b> provides ColumnManager Repository maintenance and query facilities,
 * as well as column-metadata {@link #discoverColumnMetadata(boolean) discovery} and full-schema
 * {@link #exportSchema(java.io.File) export}/{@link #importSchema(java.io.File, boolean) import}
 * facilities; it is used as a complement to the standard
 * {@link org.apache.hadoop.hbase.client.Admin} interface to provide for maintenance of optional
 * {@link ColumnDefinition} structures and querying of {@link ColumnAuditor} structures.
 *
 * @author Daniel Vimont
 */
public class RepositoryAdmin {

  private final Logger logger;
  private final Connection hbaseConnection;
  private final Repository repository;

  /**
   * Initialize a RepositoryAdmin object using a Connection provided by either a
   * {@link org.apache.hadoop.hbase.client.ConnectionFactory ConnectionFactory}
   * or {@link MConnectionFactory}.
   *
   * @param connection An HBase Connection.
   * @throws IOException if a remote or network exception occurs
   */
  public RepositoryAdmin(Connection connection) throws IOException {
    logger = Logger.getLogger(this.getClass().getName());
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
    new RepositoryAdmin(hbaseAdmin.getConnection()).repository.discoverSchema(false, false);
  }

  /**
   * Allows administrative override of the default maxVersions setting for the Repository table. The
   * default setting is 50.
   *
   * @param hbaseAdmin Standard Admin object
   * @param maxVersions Maximum versions for Repository to retain of each schema metadata attribute
   * @throws IOException if a remote or network exception occurs
   */
  public static void setRepositoryMaxVersions(Admin hbaseAdmin, int maxVersions)
          throws IOException {
    Repository.setRepositoryMaxVersions(hbaseAdmin, maxVersions);
  }

  /**
   * Get the maxVersions setting for the Repository table (maximum versions for Repository to retain
   * of each schema metadata attribute).
   *
   * @param hbaseAdmin Standard Admin object
   * @return Maximum versions for Repository to retain of each schema metadata attribute
   * @throws IOException if a remote or network exception occurs
   */
  public static int getRepositoryMaxVersions(Admin hbaseAdmin)
          throws IOException {
    return Repository.getRepositoryMaxVersions(hbaseAdmin);
  }

  /**
   * Disable and delete Repository table and drop Repository namespace (for uninstall or reinstall
   * of ColumnManager).
   *
   * @param hbaseAdmin standard HBase Admin
   * @throws IOException if a remote or network exception occurs
   */
  public static void uninstallRepositoryStructures(Admin hbaseAdmin) throws IOException {
    Repository.dropRepository(hbaseAdmin, Logger.getLogger(RepositoryAdmin.class.getName()));
    ColumnInvalidityReport.dropTempReportNamespace(hbaseAdmin);
  }

  /**
   * Delete temp report <i>Table</i>s which might remain from any abnormally terminated
   * invocations of the
   * {@link #outputReportOnInvalidColumnQualifiers(java.io.File, org.apache.hadoop.hbase.TableName, boolean, boolean)
   * outputReportOnInvalidColumn*} methods. Note that invocation of this method will cause any
   * currently-running invocations of the
   * {@link #outputReportOnInvalidColumnQualifiers(java.io.File, org.apache.hadoop.hbase.TableName, boolean, boolean)
   * outputReportOnInvalidColumn*} methods to abnormally terminate.
   *
   * @param hbaseAdmin standard HBase Admin
   * @throws IOException if a remote or network exception occurs
   */
  public static void deleteTempReportTables(Admin hbaseAdmin) throws IOException {
    ColumnInvalidityReport.dropTempReportTables(hbaseAdmin);
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
   * Get {@link ColumnAuditor} objects from the Repository for a specified <i>Table</i> and
   * <i>Column Family</i>.
   *
   * @param htd Table Descriptor
   * @param hcd Column [Family] Descriptor
   * @return set of {@link ColumnAuditor}s, or null if <i>Table</i> or <i>Column Family</i> not
   * found
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public Set<ColumnAuditor> getColumnAuditors(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException, TableNotIncludedForProcessingException {
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
   * Get {@link ColumnAuditor} objects from the Repository for a specified <i>Table</i> and
   * <i>Column Family</i>.
   *
   * @param tableName Table name
   * @param colFamily Column Family
   * @return set of {@link ColumnAuditor}s, or null if tableName or colFamily not found
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public Set<ColumnAuditor> getColumnAuditors(TableName tableName, byte[] colFamily)
          throws IOException, TableNotIncludedForProcessingException {
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
   * Get Column Qualifiers from the {@link ColumnAuditor} metadata in the Repository for a specified
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param htd Table Descriptor
   * @param hcd Column [Family] Descriptor
   * @return set of Column Qualifiers, or null if <i>Table</i> or <i>Column Family</i> not found
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public Set<byte[]> getColumnQualifiers(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException, TableNotIncludedForProcessingException {
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
   * Get Column Qualifiers from the {@link ColumnAuditor} metadata in the Repository for a specified
   * <i>Table</i> and <i>Column Family</i>.
   *
   * @param tableName Table name
   * @param colFamily Column Family
   * @return set of Column Qualifiers, or null if tableName or colFamily not found
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public Set<byte[]> getColumnQualifiers(TableName tableName, byte[] colFamily)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void addColumnDefinition(TableName tableName, byte[] colFamily,
          final ColumnDefinition colDefinition)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void addColumnDefinition(HTableDescriptor htd, HColumnDescriptor hcd,
          ColumnDefinition colDefinition)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void addColumnDefinitions(TableName tableName, byte[] colFamily,
          List<ColumnDefinition> colDefinitions)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void addColumnDefinitions(HTableDescriptor htd, HColumnDescriptor hcd,
          List<ColumnDefinition> colDefinitions)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public Collection<ColumnDefinition> getColumnDefinitions(HTableDescriptor htd, HColumnDescriptor hcd)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public Collection<ColumnDefinition> getColumnDefinitions(TableName tableName, byte[] colFamily)
          throws IOException, TableNotIncludedForProcessingException {
    MTableDescriptor mtd = getRepository().getMTableDescriptor(tableName);
    MColumnDescriptor mcd = mtd.getMColumnDescriptor(colFamily);
    return getColumnDefinitions(mtd, mcd);
  }

  /**
   * Delete the {@link ColumnDefinition} (pertaining to the submitted <i>Column Qualifier</i>)
   * from the submitted <i>Table</i> and <i>Column Family</i>.
   *
   * @param tableName name of <i>Table</i> from which {@link ColumnDefinition} is to be deleted
   * @param colFamily <i>Column Family</i> from which {@link ColumnDefinition} is to be deleted
   * @param colQualifier qualifier which uniquely identifies the {@link ColumnDefinition} to be
   * deleted
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void deleteColumnDefinition(TableName tableName, byte[] colFamily, byte[] colQualifier)
          throws IOException, TableNotIncludedForProcessingException {
    repository.deleteColumnDefinition(tableName, colFamily, colQualifier);
  }

  /**
   * Delete the {@link ColumnDefinition} (pertaining to the submitted <i>Column Qualifier</i>)
   * from the submitted <i>Table</i> and <i>Column Family</i>.
   *
   * @param htd <i>Table</i> from which {@link ColumnDefinition} is to be deleted
   * @param hcd <i>ColumnAuditor [Family] Descriptor</i> from which {@link ColumnDefinition} is to
   * be deleted
   * @param colQualifier qualifier which uniquely identifies {@link ColumnDefinition} to be deleted
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void deleteColumnDefinition(HTableDescriptor htd, HColumnDescriptor hcd,
          byte[] colQualifier)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean columnDefinitionsEnforced(TableName tableName, byte[] colFamily)
          throws IOException, TableNotIncludedForProcessingException {
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
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void setColumnDefinitionsEnforced(boolean enabled, TableName tableName, byte[] colFamily)
          throws IOException, TableNotIncludedForProcessingException {
    repository.setColumnDefinitionsEnforced(enabled, tableName, colFamily);
  }

  /**
   * Get a {@link ChangeEventMonitor} object
   *
   * @return {@link ChangeEventMonitor} object
   * @throws IOException if a remote or network exception occurs
   */
  public ChangeEventMonitor getChangeEventMonitor() throws IOException {
    return new ChangeEventMonitor(repository.getRepositoryTable());
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
   * processing by HBase) and then (3) perform <b>synchronous</b> ColumnManager Repository
   * processing upon the metadata of each successfully queued "put". <i>Be advised that the third
   * step will not take into account any potentially failed "put" transactions among those queued in
   * the second step; instead it assumes that all "put" transactions succeeded, updating the
   * ColumnManager Repository accordingly.</i>
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
   * Performs discovery of Column metadata for all
   * <a href="package-summary.html#config">ColumnManager-included</a> user <i>Table</i>s,
   * storing the results for each unique Column Qualifier as a {@link ColumnAuditor} object in
   * the ColumnManager Repository; all such metadata is then retrievable via the
   * {@link #getColumnAuditors(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
   * getColumnAuditors} and
   * {@link #getColumnQualifiers(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
   * getColumnQualifiers} methods. The discovery process entails a
   * {@link org.apache.hadoop.hbase.filter.KeyOnlyFilter} scan of the <i>Table</i>s, either
   * via direct scanning or via mapreduce. Note that when the mapreduce option is
   * utilized, the default row-cache setting is 500, which may be overridden by setting the
   * {@link org.apache.hadoop.conf.Configuration} parameter
   * {@link org.apache.hadoop.hbase.mapreduce.TableInputFormat#SCAN_CACHEDROWS}.
   *
   * @param useMapreduce if {@code true}, discovery is done via mapreduce; otherwise, discovery
   * is done via direct-scan
   * @throws IOException if a remote or network exception occurs
   */
  public void discoverColumnMetadata(boolean useMapreduce) throws Exception {
    repository.discoverSchema(true, useMapreduce);
  }

  /**
   * Performs discovery of Column metadata for all
   * <a href="package-summary.html#config">ColumnManager-included</a> user <i>Table</i>s in the
   * submitted namespace,
   * storing the results for each unique Column Qualifier as a {@link ColumnAuditor} object in
   * the ColumnManager Repository; all such metadata is then retrievable via the
   * {@link #getColumnAuditors(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
   * getColumnAuditors} and
   * {@link #getColumnQualifiers(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
   * getColumnQualifiers} methods. The discovery process entails a
   * {@link org.apache.hadoop.hbase.filter.KeyOnlyFilter} scan of the <i>Table</i>s, either
   * via direct scanning or via mapreduce. Note that when the mapreduce option is
   * utilized, the default row-cache setting is 500, which may be overridden by setting the
   * {@link org.apache.hadoop.conf.Configuration} parameter
   * {@link org.apache.hadoop.hbase.mapreduce.TableInputFormat#SCAN_CACHEDROWS}.
   *
   * @param namespace Namespace for which schema metadata is to be discovered; submitted
   * namespace must have at least one <i>Table</i> that is
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   * @param useMapreduce if {@code true}, discovery is done via mapreduce; otherwise, discovery
   * is done via direct-scan
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if no Tables from the Namespace are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void discoverColumnMetadata(String namespace, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    repository.discoverSchema(namespace, true, useMapreduce);
  }

  /**
   * Performs discovery of Column metadata for the specified <i>Table</i>,
   * storing the results for each unique Column Qualifier as a {@link ColumnAuditor} object in
   * the ColumnManager Repository; all such metadata is then retrievable via the
   * {@link #getColumnAuditors(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
   * getColumnAuditors} and
   * {@link #getColumnQualifiers(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.HColumnDescriptor)
   * getColumnQualifiers} methods. The discovery process entails a
   * {@link org.apache.hadoop.hbase.filter.KeyOnlyFilter} scan of the <i>Table</i>,
   * either via direct scanning or via mapreduce. Note that when the mapreduce option is
   * utilized, the default row-cache setting is 500, which may be overridden by setting the
   * {@link org.apache.hadoop.conf.Configuration} parameter
   * {@link org.apache.hadoop.hbase.mapreduce.TableInputFormat#SCAN_CACHEDROWS}.
   *
   * @param tableName <i>Table</i> for which schema metadata is to be discovered; submitted
   * <i>Table</i> must be
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   * @param useMapreduce if {@code true}, discovery is done via mapreduce; otherwise, discovery
   * is done via direct-scan
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void discoverColumnMetadata(TableName tableName, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    repository.discoverSchema(tableName, true, useMapreduce);
  }

  // make this method public if needs dictate
  void purgeTableSchemaEntity(TableName tn) throws IOException {
    repository.purgeTableSchemaEntity(tn);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * metadata contents (i.e., all <i>{@link NamespaceDescriptor Namespace}</i>,
   * <i>{@link HTableDescriptor Table}</i>, <i>{@link HColumnDescriptor Column Family}</i>,
   * <i>{@link ColumnAuditor}</i>, and <i>{@link ColumnDefinition}</i> metadata) of the
   * ColumnManager metadata Repository. This constitutes an XML-formatted serialization of
   * objects of the following classes: {@link NamespaceDescriptor}, {@link HTableDescriptor},
   * {@link HColumnDescriptor}, {@link ColumnAuditor}, and {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param targetFile target file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws XMLStreamException if an exception occurs in the context of JAXB processing
   */
  public void exportSchema(File targetFile)
          throws IOException, JAXBException, XMLStreamException {
    repository.exportSchema(null, null, targetFile);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * schema contents of the specified HBase <i>Namespace</i>. This constitutes an XML-formatted
   * serialization of objects of the following
   * classes: {@link NamespaceDescriptor}, {@link HTableDescriptor}, {@link HColumnDescriptor},
   * {@link ColumnAuditor}, and {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param targetFile target file
   * @param sourceNamespaceName namespace from which to export schema entities
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws XMLStreamException if an exception occurs in the context of JAXB processing
   * @throws TableNotIncludedForProcessingException if no Tables from the Namespace are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void exportSchema(File targetFile, String sourceNamespaceName)
          throws IOException, JAXBException, XMLStreamException,
          TableNotIncludedForProcessingException {
    repository.exportSchema(sourceNamespaceName, null, targetFile);
  }

  /**
   * Creates an external HBaseSchemaArchive (HSA) file in XML format* containing the complete
   * schema contents of the specified HBase <i>Table</i>. This constitutes an
   * XML-formatted serialization of objects of the following classes:
   * {@link HTableDescriptor}, {@link HColumnDescriptor}, {@link ColumnAuditor}, and
   * {@link ColumnDefinition}.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param targetFile target File
   * @param sourceTableName table to export (along with its component schema-entities)
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws XMLStreamException if an exception occurs in the context of JAXB processing
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void exportSchema(File targetFile, TableName sourceTableName)
          throws IOException, JAXBException, XMLStreamException,
          TableNotIncludedForProcessingException {
    repository.exportSchema(sourceTableName.getNamespaceAsString(), sourceTableName, targetFile);
  }

  /**
   * Import into HBase the complete contents of an external HBaseSchemaArchive (HSA) XML file*;
   * this process will NOT overlay any <b>existing</b> Namespace and Table structures in HBase. Only
   * Tables which are <a href="package-summary.html#config">included in ColumnManager processing</a>
   * will be imported.
   * <br><br>*An HSA file is created with the one of the
   * {@link #exportSchema(java.io.File) #exportSchema} methods and adheres to
   * the XML Schema layout in <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">
   * HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceHsaFile source file
   * @param includeColumnAuditors if <b>true</b>, import {@link ColumnAuditor} metadata from the
   * HBaseSchemaArchive file into the ColumnManager Repository.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public void importSchema(File sourceHsaFile, boolean includeColumnAuditors)
          throws IOException, JAXBException {
    repository.importSchema(sourceHsaFile, null, null, null, includeColumnAuditors, false);
  }

  /**
   * Import a Namespace into HBase with all its component schema-objects as serialized in an
   * external HBaseSchemaArchive (HSA) XML file*; this process will NOT overlay any <b>existing</b>
   * Namespace and Table structures in HBase. Only Tables which are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   * will be imported.
   * <br><br>*An HSA file is created with the one of the
   * {@link #exportSchema(java.io.File) #exportSchema} methods and adheres to
   * the XML Schema layout in <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">
   * HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceHsaFile source file
   * @param namespaceName namespace to import.
   * @param includeColumnAuditors if <b>true</b>, import {@link ColumnAuditor} metadata from the
   * HBaseSchemaArchive file into the ColumnManager Repository.
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws TableNotIncludedForProcessingException if no Tables from the Namespace are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void importSchema(File sourceHsaFile, String namespaceName, boolean includeColumnAuditors)
          throws IOException, JAXBException, TableNotIncludedForProcessingException {
    repository.importSchema(sourceHsaFile, namespaceName, null, null, includeColumnAuditors, false);
  }

  /**
   * Import a Table into HBase with all its component schema-objects as serialized in an external
   * HBaseSchemaArchive (HSA) XML file*; this process will NOT overlay an <b>existing</b> Table
   * (and its component structures) in HBase. Only a Table which is
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   * will be imported.
   * <br><br>*An HSA file is created with the one of the
   * {@link #exportSchema(java.io.File) #exportSchema} methods and adheres to
   * the XML Schema layout in <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">
   * HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param includeColumnAuditors if <b>true</b>, import {@link ColumnAuditor} metadata from the
   * HBaseSchemaArchive file into the ColumnManager Repository.
   * @param tableName Name of table to be imported.
   * @param sourceHsaFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public void importSchema(File sourceHsaFile, TableName tableName, boolean includeColumnAuditors)
          throws IOException, JAXBException, TableNotIncludedForProcessingException {
    repository.importSchema(sourceHsaFile, tableName.getNamespaceAsString(), tableName, null,
            includeColumnAuditors, false);
  }

  /**
   * Import into the ColumnManager Repository all {@link ColumnDefinition}s that are found in the
   * submitted HBaseSchemaArchive (HSA) XML file*;
   * for a {@link ColumnDefinition} to be imported, it must belong to an existing
   * Table/ColumnFamily which is
   * <a href="package-summary.html#config">included in ColumnManager processing</a>.
   * <br><br>*An HSA file is created with the one of the
   * {@link #exportSchema(java.io.File) #exportSchema} methods and adheres to
   * the XML Schema layout in <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">
   * HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceHsaFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
    */
  public void importColumnDefinitions(File sourceHsaFile)
          throws IOException, JAXBException {
    repository.importSchema(sourceHsaFile, null, null, null, false, true);
  }

  /**
   * Import into the ColumnManager Repository all of a specified Namespace's
   * {@link ColumnDefinition}s that are found in the submitted HBaseSchemaArchive (HSA) XML file*;
   * for a {@link ColumnDefinition} to be imported, it must belong to an existing
   * Table/ColumnFamily which is
   * <a href="package-summary.html#config">included in ColumnManager processing</a>.
   * <br><br>*An HSA file is created with the one of the
   * {@link #exportSchema(java.io.File) #exportSchema} methods and adheres to
   * the XML Schema layout in <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">
   * HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param namespace namespace for which {@link ColumnDefinition}s are to be imported
   * @param sourceHsaFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws TableNotIncludedForProcessingException if no Tables from the Namespace are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
    */
  public void importColumnDefinitions(File sourceHsaFile, String namespace)
          throws IOException, JAXBException, TableNotIncludedForProcessingException {
    repository.importSchema(sourceHsaFile, namespace, null, null, false, true);
  }

  /**
   * Import into the ColumnManager Repository all of a specified Table's
   * {@link ColumnDefinition}s that are found in the submitted HBaseSchemaArchive (HSA) XML file*;
   * for a {@link ColumnDefinition} to be imported, it must belong to an existing
   * Table/ColumnFamily which is
   * <a href="package-summary.html#config">included in ColumnManager processing</a>.
   * <br><br>*An HSA file is created with the one of the
   * {@link #exportSchema(java.io.File) #exportSchema} methods and adheres to
   * the XML Schema layout in <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">
   * HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param tableName table for which {@link ColumnDefinition}s are to be imported
   * @param sourceHsaFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
    */
  public void importColumnDefinitions(File sourceHsaFile, TableName tableName)
          throws IOException, JAXBException, TableNotIncludedForProcessingException {
    repository.importSchema(sourceHsaFile, tableName.getNamespaceAsString(),
            tableName, null, false, true);
  }

  /**
   * Import into the ColumnManager Repository all of a specified ColumnFamily's
   * {@link ColumnDefinition}s that are found in the submitted HBaseSchemaArchive (HSA) XML file*;
   * for a {@link ColumnDefinition} to be imported, it must belong to an existing
   * Table/ColumnFamily which is
   * <a href="package-summary.html#config">included in ColumnManager processing</a>.
   * <br><br>*An HSA file is created with the one of the
   * {@link #exportSchema(java.io.File) #exportSchema} methods and adheres to
   * the XML Schema layout in <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">
   * HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param tableName table of ColumnFamily for which {@link ColumnDefinition}s are to be imported
   * @param colFamily ColumnFamily for which {@link ColumnDefinition}s are to be imported
   * @param sourceHsaFile source file
   * @throws IOException if a remote or network exception occurs
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
    */
  public void importColumnDefinitions(
          File sourceHsaFile, TableName tableName, byte[] colFamily)
          throws IOException, JAXBException, TableNotIncludedForProcessingException {
    repository.importSchema(sourceHsaFile, tableName.getNamespaceAsString(),
            tableName, colFamily, false, true);
  }

  /**
   * Generates a hierarchically-indented, text-based summary report of the contents of an external
   * HBaseSchemaArchive (HSA) XML file*.
   * <br><br>*An HSA file adheres to the XML Schema layout in
   * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
   *
   * @param sourceHsaFile source HBaseSchemaArchive file
   * @return A String containing a summary report suitable for printing/viewing.
   * @throws JAXBException if an exception occurs in the context of JAXB processing
   */
  public static String generateHsaFileSummary(File sourceHsaFile) throws JAXBException {
    return HBaseSchemaArchive.getSummaryReport(sourceHsaFile);
  }

  /**
   * Generates and outputs a CSV-formatted report on all {@link ColumnAuditor}s that have been
   * {@link ColumnAuditor captured} or
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean) discovered}
   * pertaining to all <a href="package-summary.html#config">ColumnManager-included</a> user
   * <i>Tables</i> within the submitted <i>Namespace</i>.
   *
   * @param targetFile file to which the CSV file is to be outputted
   * @param namespace namespace for which {@link ColumnAuditor} report is to be generated/outputted
   * @return {@code true} if {@link ColumnAuditor}s found and reported on; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if no Tables from the Namespace are
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnColumnQualifiers(File targetFile, String namespace)
          throws IOException, TableNotIncludedForProcessingException {
    return repository.outputReportOnColumnQualifiers(namespace, null, null, targetFile);
  }

  /**
   * Generates and outputs a CSV-formatted report on all {@link ColumnAuditor}s that have been
   * {@link ColumnAuditor captured} or
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean) discovered}
   * pertaining to the submitted user <i>Table</i>.
   *
   * @param targetFile file to which the CSV file is to be outputted
   * @param tableName name of the Table for which {@link ColumnAuditor} report is to be
   * generated/outputted
   * @return {@code true} if {@link ColumnAuditor}s found and reported on; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnColumnQualifiers(File targetFile, TableName tableName)
          throws IOException, TableNotIncludedForProcessingException {
    return repository.outputReportOnColumnQualifiers(
            tableName.getNamespaceAsString(), tableName, null, targetFile);
  }

  /**
   * Generates and outputs a CSV-formatted report on all {@link ColumnAuditor}s that have been
   * {@link ColumnAuditor captured} or
   * {@link RepositoryAdmin#discoverColumnMetadata(boolean) discovered}
   * pertaining to the submitted <i>Column Family</i> within the submitted user <i>Table</i>.
   *
   * @param targetFile file to which the CSV file is to be outputted
   * @param tableName name of the Table for which {@link ColumnAuditor} report is to be
   * generated/outputted
   * @param colFamily name of the Column Family within the Table for which {@link ColumnAuditor}
   * report is to be generated/outputted
   * @return {@code true} if {@link ColumnAuditor}s found and reported on; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnColumnQualifiers(
          File targetFile, TableName tableName, byte[] colFamily)
          throws IOException, TableNotIncludedForProcessingException {
    return repository.outputReportOnColumnQualifiers(
            tableName.getNamespaceAsString(), tableName, colFamily, targetFile);
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
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs;
   * otherwise, analysis is done via direct-scan
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnInvalidColumnQualifiers(File targetFile, TableName tableName,
          boolean verbose, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    return outputReportOnInvalidColumnQualifiers(
            targetFile, tableName, null, verbose, useMapreduce);
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
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs;
   * otherwise, analysis is done via direct-scan
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnInvalidColumnQualifiers(File targetFile, TableName tableName,
          byte[] colFamily, boolean verbose, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    return repository.outputReportOnInvalidColumns(ColumnInvalidityReport.ReportType.QUALIFIER,
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
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs;
   * otherwise, analysis is done via direct-scan
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnInvalidColumnLengths(File targetFile, TableName tableName,
          boolean verbose, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    return outputReportOnInvalidColumnLengths(
            targetFile, tableName, null, verbose, useMapreduce);
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
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs;
   * otherwise, analysis is done via direct-scan
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnInvalidColumnLengths(File targetFile, TableName tableName,
          byte[] colFamily, boolean verbose, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    return repository.outputReportOnInvalidColumns(ColumnInvalidityReport.ReportType.LENGTH,
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
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs;
   * otherwise, analysis is done via direct-scan
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnInvalidColumnValues(File targetFile, TableName tableName,
          boolean verbose, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    return outputReportOnInvalidColumnValues(targetFile, tableName, null, verbose, useMapreduce);
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
   * @param useMapreduce if {@code true}, analysis will be done on servers via mapreduce jobs;
   * otherwise, analysis is done via direct-scan
   * @return {@code true} if invalid column qualifiers found; otherwise, {@code false}
   * @throws IOException if a remote or network exception occurs
   * @throws TableNotIncludedForProcessingException if Table not
   * <a href="package-summary.html#config">included in ColumnManager processing</a>
   */
  public boolean outputReportOnInvalidColumnValues(File targetFile, TableName tableName,
          byte[] colFamily, boolean verbose, boolean useMapreduce)
          throws Exception, TableNotIncludedForProcessingException {
    return repository.outputReportOnInvalidColumns(ColumnInvalidityReport.ReportType.VALUE,
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
}
