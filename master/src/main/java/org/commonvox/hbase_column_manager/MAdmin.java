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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Wrapper for standard HBase Admin object; in addition to standard Admin functionality, an MAdmin
 * object interfaces with the ColumnManager repository for tracking of metadata changes.
 *
 * @author Daniel Vimont
 */
class MAdmin implements Admin {

  private final Admin wrappedHbaseAdmin;
  private final Repository repository;
  private static final String UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG
          = "Method not supported when ColumnManager repository is ACTIVATED.";
  private static final String UNSUPPORTED_MASS_DISABLE_DELETE_METHOD
          = " This method's signature does not properly and explicitly support namespaces, "
          + "and its use could accidently disable or delete ColumnManager table(s).";

  MAdmin(Admin hBaseAdmin, Repository repository) {
    wrappedHbaseAdmin = hBaseAdmin;
    this.repository = repository;
  }

  Admin getWrappedAdmin() {
    return wrappedHbaseAdmin;
  }

    // Start of overrides which provide Repository functionality in addition to
  //    wrapping standard Admin functionality
  @Override
  public void createTable(HTableDescriptor htd) throws IOException {
    wrappedHbaseAdmin.createTable(htd);
    if (repository.isActivated()) {
      repository.putTableSchemaEntity(htd);
    }
  }

  @Override
  public void createTable(HTableDescriptor htd, byte[] bytes, byte[] bytes1, int i) throws IOException {
    wrappedHbaseAdmin.createTable(htd, bytes, bytes1, i);
    if (repository.isActivated()) {
      repository.putTableSchemaEntity(htd);
    }
  }

  @Override
  public void createTable(HTableDescriptor htd, byte[][] bytes) throws IOException {
    wrappedHbaseAdmin.createTable(htd, bytes);
    if (repository.isActivated()) {
      repository.putTableSchemaEntity(htd);
    }
  }

  /**
   * In HBase 2.0, this method will return a Future object, which can be handed off to another
   * thread to await completion of the table creation and then invoke the appropriate Repository
   * logic. For now, this method not supported when Repository is ACTIVATED.
   *
   * @param htd
   * @param bytes
   * @throws java.io.IOException
   * @throws UnsupportedOperationException
   */
  @Override
  public void createTableAsync(HTableDescriptor htd, byte[][] bytes)
          throws IOException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG);
    }

    wrappedHbaseAdmin.createTableAsync(htd, bytes);
  }

  @Override
  public void deleteTable(TableName tn) throws IOException {
    wrappedHbaseAdmin.deleteTable(tn);
    if (repository.isActivated()) {
      repository.deleteTableSchemaEntity(tn);
    }
  }

  @Override
  public HTableDescriptor[] deleteTables(String tableNameRegex) throws IOException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG + UNSUPPORTED_MASS_DISABLE_DELETE_METHOD);
    }

    return wrappedHbaseAdmin.deleteTables(tableNameRegex);
  }

  @Override
  public HTableDescriptor[] deleteTables(Pattern ptrn) throws IOException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG + UNSUPPORTED_MASS_DISABLE_DELETE_METHOD);
    }

    return wrappedHbaseAdmin.deleteTables(ptrn);
  }

  @Override
  public HTableDescriptor[] disableTables(String string) throws IOException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG + UNSUPPORTED_MASS_DISABLE_DELETE_METHOD);
    }

    return wrappedHbaseAdmin.disableTables(string);
  }

  @Override
  public HTableDescriptor[] disableTables(Pattern ptrn) throws IOException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG + UNSUPPORTED_MASS_DISABLE_DELETE_METHOD);
    }

    return wrappedHbaseAdmin.disableTables(ptrn);
  }

  @Override
  public void truncateTable(TableName tn, boolean bln) throws IOException {
    wrappedHbaseAdmin.truncateTable(tn, bln);
    if (repository.isActivated()) {
      repository.truncateTableColumns(tn);
    }
  }

  /**
   * This method slated for deprecation in a future HBase release.
   *
   * @param tn
   * @param hcd
   * @throws IOException
   */
  @Override
  // @Deprecated // deprecated commented out - this method not yet deprecated in HBase 1.0.1.1
  public void addColumn(TableName tn, HColumnDescriptor hcd) throws IOException {
    wrappedHbaseAdmin.addColumn(tn, hcd);
    if (repository.isActivated()) {
      repository.putColumnFamilySchemaEntity(tn, hcd);
    }
  }

  //@Override // override commented out - this method not yet included in HBase 1.0.1.1
  public void addColumnFamily(TableName tn, HColumnDescriptor hcd) throws IOException {
    wrappedHbaseAdmin.addColumn(tn, hcd); // with future HBase version should be changed to "addColumnFamily"
    if (repository.isActivated()) {
      repository.putColumnFamilySchemaEntity(tn, hcd);
    }
  }

  @Override
  // @Deprecated // deprecated commented out - this method not yet deprecated in HBase 1.0.1.1
  public void modifyColumn(TableName tn, HColumnDescriptor hcd) throws IOException {
    wrappedHbaseAdmin.modifyColumn(tn, hcd);
    if (repository.isActivated()) {
      repository.putColumnFamilySchemaEntity(tn, hcd);
    }
  }

  //@Override // override commented out - this method not yet included in HBase 1.0.1.1
  public void modifyColumnFamily(TableName tn, HColumnDescriptor hcd) throws IOException {
    wrappedHbaseAdmin.modifyColumn(tn, hcd);
    if (repository.isActivated()) {
      repository.putColumnFamilySchemaEntity(tn, hcd);
    }
  }

  /**
   * This method slated for deprecation in a future HBase release.
   *
   * @param tn
   * @param name
   * @throws IOException
   */
  @Override
  // @Deprecated // deprecated commented out - this method not yet deprecated in HBase 1.0.1.1
  public void deleteColumn(TableName tn, byte[] name) throws IOException {
    wrappedHbaseAdmin.deleteColumn(tn, name);
    if (repository.isActivated()) {
      repository.deleteColumnFamily(tn, name);
    }
  }

  /**
   *
   * @param tn
   * @param name
   * @throws IOException
   */
  //@Override // override commented out - this method not yet included in HBase 1.0.1.1
  public void deleteColumnFamily(TableName tn, byte[] name) throws IOException {
    wrappedHbaseAdmin.deleteColumn(tn, name); // with future HBase version should changed to "deleteColumnFamily"
    if (repository.isActivated()) {
      repository.deleteColumnFamily(tn, name);
    }
  }

  @Override
  public void modifyTable(TableName tn, HTableDescriptor htd) throws IOException {
    wrappedHbaseAdmin.modifyTable(tn, htd);
    if (repository.isActivated()) {
      repository.putTableSchemaEntity(htd);
    }
  }

  @Override
  public void createNamespace(NamespaceDescriptor nd) throws IOException {
    wrappedHbaseAdmin.createNamespace(nd);
    if (repository.isActivated()) {
      repository.putNamespaceSchemaEntity(nd);
    }
  }

  @Override
  public void modifyNamespace(NamespaceDescriptor nd) throws IOException {
    wrappedHbaseAdmin.modifyNamespace(nd);
    if (repository.isActivated()) {
      repository.putNamespaceSchemaEntity(nd);
    }
  }

  @Override
  public void deleteNamespace(String string) throws IOException {
    wrappedHbaseAdmin.deleteNamespace(string);
    if (repository.isActivated()) {
      repository.deleteNamespaceSchemaEntity(string);
    }
  }

  // Start of overrides which simply pass on standard Admin functionality
  @Override
  public int getOperationTimeout() {
    return wrappedHbaseAdmin.getOperationTimeout();
  }

  @Override
  public void abort(String string, Throwable thrwbl) {
    wrappedHbaseAdmin.abort(string, thrwbl);
  }

  @Override
  public boolean isAborted() {
    return wrappedHbaseAdmin.isAborted();
  }

  @Override
  public Connection getConnection() {
    return wrappedHbaseAdmin.getConnection();
  }

  @Override
  public boolean tableExists(TableName tn) throws IOException {
    return wrappedHbaseAdmin.tableExists(tn);
  }

  @Override
  public HTableDescriptor getTableDescriptor(TableName tn)
          throws TableNotFoundException, IOException {
    return wrappedHbaseAdmin.getTableDescriptor(tn);
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return wrappedHbaseAdmin.listTables();
  }

  @Override
  public HTableDescriptor[] listTables(Pattern ptrn) throws IOException {
    return wrappedHbaseAdmin.listTables(ptrn);
  }

  @Override
  public HTableDescriptor[] listTables(String string) throws IOException {
    return wrappedHbaseAdmin.listTables(string);
  }

  @Override
  public HTableDescriptor[] listTables(Pattern ptrn, boolean bln) throws IOException {
    return wrappedHbaseAdmin.listTables(ptrn, bln);
  }

  @Override
  public HTableDescriptor[] listTables(String string, boolean bln) throws IOException {
    return wrappedHbaseAdmin.listTables(string, bln);
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return wrappedHbaseAdmin.listTableNames();
  }

  @Override
  public TableName[] listTableNames(Pattern ptrn) throws IOException {
    return wrappedHbaseAdmin.listTableNames(ptrn);
  }

  @Override
  public TableName[] listTableNames(String string) throws IOException {
    return wrappedHbaseAdmin.listTableNames(string);
  }

  @Override
  public TableName[] listTableNames(Pattern ptrn, boolean bln) throws IOException {
    return wrappedHbaseAdmin.listTableNames(ptrn, bln);
  }

  @Override
  public TableName[] listTableNames(String string, boolean bln) throws IOException {
    return wrappedHbaseAdmin.listTableNames(string, bln);
  }

  @Override
  public void enableTable(TableName tn) throws IOException {
    wrappedHbaseAdmin.enableTable(tn);
  }

  @Override
  public void enableTableAsync(TableName tn) throws IOException {
    wrappedHbaseAdmin.enableTableAsync(tn);
  }

  @Override
  public HTableDescriptor[] enableTables(String string) throws IOException {
    return wrappedHbaseAdmin.enableTables(string);
  }

  @Override
  public HTableDescriptor[] enableTables(Pattern ptrn) throws IOException {
    return wrappedHbaseAdmin.enableTables(ptrn);
  }

  @Override
  public void disableTableAsync(TableName tn) throws IOException {
    wrappedHbaseAdmin.disableTableAsync(tn);
  }

  @Override
  public void disableTable(TableName tn) throws IOException {
    wrappedHbaseAdmin.disableTable(tn);
  }

  @Override
  public boolean isTableEnabled(TableName tn) throws IOException {
    return wrappedHbaseAdmin.isTableEnabled(tn);
  }

  @Override
  public boolean isTableDisabled(TableName tn) throws IOException {
    return wrappedHbaseAdmin.isTableDisabled(tn);
  }

  @Override
  public boolean isTableAvailable(TableName tn) throws IOException {
    return wrappedHbaseAdmin.isTableAvailable(tn);
  }

  @Override
  public boolean isTableAvailable(TableName tn, byte[][] bytes) throws IOException {
    return wrappedHbaseAdmin.isTableAvailable(tn, bytes);
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(TableName tn) throws IOException {
    return wrappedHbaseAdmin.getAlterStatus(tn);
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(byte[] bytes) throws IOException {
    return wrappedHbaseAdmin.getAlterStatus(bytes);
  }

  @Override
  public void closeRegion(String string, String string1) throws IOException {
    wrappedHbaseAdmin.closeRegion(string, string1);
  }

  @Override
  public void closeRegion(byte[] bytes, String string) throws IOException {
    wrappedHbaseAdmin.closeRegion(bytes, string);
  }

  @Override
  public boolean closeRegionWithEncodedRegionName(String string, String string1) throws IOException {
    return wrappedHbaseAdmin.closeRegionWithEncodedRegionName(string, string1);
  }

  @Override
  public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
    wrappedHbaseAdmin.closeRegion(sn, hri);
  }

  @Override
  public List<HRegionInfo> getOnlineRegions(ServerName sn) throws IOException {
    return wrappedHbaseAdmin.getOnlineRegions(sn);
  }

  @Override
  public void flush(TableName tn) throws IOException {
    wrappedHbaseAdmin.flush(tn);
  }

  @Override
  public void flushRegion(byte[] bytes) throws IOException {
    wrappedHbaseAdmin.flushRegion(bytes);
  }

  @Override
  public void compact(TableName tn) throws IOException {
    wrappedHbaseAdmin.compact(tn);
  }

  @Override
  public void compactRegion(byte[] bytes) throws IOException {
    wrappedHbaseAdmin.compactRegion(bytes);
  }

  @Override
  public void compact(TableName tn, byte[] bytes) throws IOException {
    wrappedHbaseAdmin.compact(tn, bytes);
  }

  @Override
  public void compactRegion(byte[] bytes, byte[] bytes1) throws IOException {
    wrappedHbaseAdmin.compactRegion(bytes, bytes1);
  }

  @Override
  public void majorCompact(TableName tn) throws IOException {
    wrappedHbaseAdmin.majorCompact(tn);
  }

  @Override
  public void majorCompactRegion(byte[] bytes) throws IOException {
    wrappedHbaseAdmin.majorCompactRegion(bytes);
  }

  @Override
  public void majorCompact(TableName tn, byte[] bytes) throws IOException {
    wrappedHbaseAdmin.majorCompact(tn, bytes);
  }

  @Override
  public void majorCompactRegion(byte[] bytes, byte[] bytes1) throws IOException {
    wrappedHbaseAdmin.majorCompactRegion(bytes, bytes1);
  }

  @Override
  public void compactRegionServer(ServerName sn, boolean bln) throws IOException, InterruptedException {
    wrappedHbaseAdmin.compactRegionServer(sn, bln);
  }

  @Override
  public void move(byte[] bytes, byte[] bytes1) throws IOException {
    wrappedHbaseAdmin.move(bytes, bytes1);
  }

  @Override
  public void assign(byte[] bytes) throws IOException {
    wrappedHbaseAdmin.assign(bytes);
  }

  @Override
  public void unassign(byte[] bytes, boolean bln) throws IOException {
    wrappedHbaseAdmin.unassign(bytes, bln);
  }

  @Override
  public void offline(byte[] bytes) throws IOException {
    wrappedHbaseAdmin.offline(bytes);
  }

  @Override
  public boolean setBalancerRunning(boolean bln, boolean bln1) throws IOException {
    return wrappedHbaseAdmin.setBalancerRunning(bln, bln1);
  }

  @Override
  public boolean balancer() throws IOException {
    return wrappedHbaseAdmin.balancer();
  }

  @Override
  public boolean enableCatalogJanitor(boolean bln) throws IOException {
    return wrappedHbaseAdmin.enableCatalogJanitor(bln);
  }

  @Override
  public int runCatalogScan() throws IOException {
    return wrappedHbaseAdmin.runCatalogScan();
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws IOException {
    return wrappedHbaseAdmin.isCatalogJanitorEnabled();
  }

  @Override
  public void mergeRegions(byte[] bytes, byte[] bytes1, boolean bln) throws IOException {
    wrappedHbaseAdmin.mergeRegions(bytes, bytes1, bln);
  }

  @Override
  public void split(TableName tn) throws IOException {
    wrappedHbaseAdmin.split(tn);
  }

  @Override
  public void splitRegion(byte[] bytes) throws IOException {
    wrappedHbaseAdmin.splitRegion(bytes);
  }

  @Override
  public void split(TableName tn, byte[] bytes) throws IOException {
    wrappedHbaseAdmin.split(tn, bytes);
  }

  @Override
  public void splitRegion(byte[] bytes, byte[] bytes1) throws IOException {
    wrappedHbaseAdmin.splitRegion(bytes, bytes1);
  }

  @Override
  public void shutdown() throws IOException {
    wrappedHbaseAdmin.shutdown();
  }

  @Override
  public void stopMaster() throws IOException {
    wrappedHbaseAdmin.stopMaster();
  }

  @Override
  public void stopRegionServer(String string) throws IOException {
    wrappedHbaseAdmin.stopRegionServer(string);
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    return wrappedHbaseAdmin.getClusterStatus();
  }

  @Override
  public Configuration getConfiguration() {
    return wrappedHbaseAdmin.getConfiguration();
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String string) throws IOException {
    return wrappedHbaseAdmin.getNamespaceDescriptor(string);
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return wrappedHbaseAdmin.listNamespaceDescriptors();
  }

  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(String string) throws IOException {
    return wrappedHbaseAdmin.listTableDescriptorsByNamespace(string);
  }

  @Override
  public TableName[] listTableNamesByNamespace(String string) throws IOException {
    return wrappedHbaseAdmin.listTableNamesByNamespace(string);
  }

  @Override
  public List<HRegionInfo> getTableRegions(TableName tn) throws IOException {
    return wrappedHbaseAdmin.getTableRegions(tn);
  }

  @Override
  public void close() throws IOException {
    wrappedHbaseAdmin.close();
  }

  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> list) throws IOException {
    return wrappedHbaseAdmin.getTableDescriptorsByTableName(list);
  }

  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> list) throws IOException {
    return wrappedHbaseAdmin.getTableDescriptors(list);
  }

  @Override
  public void rollWALWriter(ServerName sn) throws IOException, FailedLogCloseException {
    wrappedHbaseAdmin.rollWALWriter(sn);
  }

  @Override
  public String[] getMasterCoprocessors() throws IOException {
    return wrappedHbaseAdmin.getMasterCoprocessors();
  }

  @Override
  public CompactionState getCompactionState(TableName tn) throws IOException {
    return wrappedHbaseAdmin.getCompactionState(tn);
  }

  @Override
  public CompactionState getCompactionStateForRegion(byte[] bytes) throws IOException {
    return wrappedHbaseAdmin.getCompactionStateForRegion(bytes);
  }

  @Override
  public void snapshot(String string, TableName tn) throws IOException, SnapshotCreationException, IllegalArgumentException {
    wrappedHbaseAdmin.snapshot(string, tn);
  }

  @Override
  public void snapshot(byte[] name, TableName tn) throws IOException, SnapshotCreationException, IllegalArgumentException {
    wrappedHbaseAdmin.snapshot(name, tn);
  }

  @Override
  public void snapshot(String string, TableName tn, Type type) throws IOException, SnapshotCreationException, IllegalArgumentException {
    wrappedHbaseAdmin.snapshot(string, tn, type);
  }

  @Override
  public void snapshot(SnapshotDescription sd) throws IOException, SnapshotCreationException, IllegalArgumentException {
    wrappedHbaseAdmin.snapshot(sd);
  }

  @Override
  public SnapshotResponse takeSnapshotAsync(SnapshotDescription sd) throws IOException, SnapshotCreationException {
    return wrappedHbaseAdmin.takeSnapshotAsync(sd);
  }

  @Override
  public boolean isSnapshotFinished(SnapshotDescription sd) throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    return wrappedHbaseAdmin.isSnapshotFinished(sd);
  }

  @Override
  public void restoreSnapshot(byte[] name)
          throws IOException, RestoreSnapshotException, UnsupportedOperationException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG);
    }

    wrappedHbaseAdmin.restoreSnapshot(name);
  }

  @Override
  public void restoreSnapshot(String string)
          throws IOException, RestoreSnapshotException, UnsupportedOperationException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG);
    }

    wrappedHbaseAdmin.restoreSnapshot(string);
  }

  @Override
  public void restoreSnapshot(byte[] name, boolean bln)
          throws IOException, RestoreSnapshotException, UnsupportedOperationException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG);
    }

    wrappedHbaseAdmin.restoreSnapshot(name, bln);
  }

  @Override
  public void restoreSnapshot(String string, boolean bln)
          throws IOException, RestoreSnapshotException, UnsupportedOperationException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG);
    }

    wrappedHbaseAdmin.restoreSnapshot(string, bln);
  }

  @Override
  public void cloneSnapshot(byte[] name, TableName tn)
          throws IOException, TableExistsException, RestoreSnapshotException,
          UnsupportedOperationException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG);
    }

    wrappedHbaseAdmin.cloneSnapshot(name, tn);
  }

  @Override
  public void cloneSnapshot(String string, TableName tn)
          throws IOException, TableExistsException, RestoreSnapshotException,
          UnsupportedOperationException {
    if (repository.isActivated()) {
      throw new UnsupportedOperationException(UNSUPPORTED_WHEN_COLMANAGER_ACTIVATED_MSG);
    }

    wrappedHbaseAdmin.cloneSnapshot(string, tn);
  }

  @Override
  public void execProcedure(String string, String string1, Map<String, String> map) throws IOException {
    wrappedHbaseAdmin.execProcedure(string, string1, map);
  }

  @Override
  public byte[] execProcedureWithRet(String string, String string1, Map<String, String> map) throws IOException {
    return wrappedHbaseAdmin.execProcedureWithRet(string, string1, map);
  }

  @Override
  public boolean isProcedureFinished(String string, String string1, Map<String, String> map) throws IOException {
    return wrappedHbaseAdmin.isProcedureFinished(string, string1, map);
  }

  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    return wrappedHbaseAdmin.listSnapshots();
  }

  @Override
  public List<HBaseProtos.SnapshotDescription> listSnapshots(String string) throws IOException {
    return wrappedHbaseAdmin.listSnapshots(string);
  }

  @Override
  public List<SnapshotDescription> listSnapshots(Pattern ptrn) throws IOException {
    return wrappedHbaseAdmin.listSnapshots(ptrn);
  }

  @Override
  public void deleteSnapshot(byte[] name) throws IOException {
    wrappedHbaseAdmin.deleteSnapshot(name);
  }

  @Override
  public void deleteSnapshot(String string) throws IOException {
    wrappedHbaseAdmin.deleteSnapshot(string);
  }

  @Override
  public void deleteSnapshots(String string) throws IOException {
    wrappedHbaseAdmin.deleteSnapshots(string);
  }

  @Override
  public void deleteSnapshots(Pattern ptrn) throws IOException {
    wrappedHbaseAdmin.deleteSnapshots(ptrn);
  }

  @Override
  public CoprocessorRpcChannel coprocessorService() {
    return wrappedHbaseAdmin.coprocessorService();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName sn) {
    return wrappedHbaseAdmin.coprocessorService(sn);
  }

  @Override
  public void updateConfiguration(ServerName sn) throws IOException {
    wrappedHbaseAdmin.updateConfiguration(sn);
  }

  @Override
  public void updateConfiguration() throws IOException {
    wrappedHbaseAdmin.updateConfiguration();
  }

  @Override
  public int getMasterInfoPort() throws IOException {
    return wrappedHbaseAdmin.getMasterInfoPort();
  }

  @Override
  public boolean equals(Object otherObject) {
    return wrappedHbaseAdmin.equals(otherObject);
  }

  @Override
  public int hashCode() {
    return wrappedHbaseAdmin.hashCode();
  }
}
