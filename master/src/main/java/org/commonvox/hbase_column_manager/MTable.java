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

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Daniel Vimont
 */
class MTable implements Table {

  private final Table wrappedTable;
  private final Repository repository;
  private final MTableDescriptor mTableDescriptor;
  private final boolean includedInRepositoryProcessing;

  MTable(Table userTable, Repository repository)
          throws IOException {
    wrappedTable = userTable;

    this.repository = repository;
    if (repository.isActivated()) {
      mTableDescriptor = repository.getMTableDescriptor(wrappedTable.getName());
      includedInRepositoryProcessing = repository.isIncludedTable(wrappedTable.getName());
    } else {
      mTableDescriptor = null;
      includedInRepositoryProcessing = false;
    }
  }

  @Override
  public TableName getName() {
    return wrappedTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return wrappedTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return wrappedTable.getTableDescriptor();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    return wrappedTable.exists(get);
  }

  @Override
  public boolean[] existsAll(List<Get> list) throws IOException {
    return wrappedTable.existsAll(list);
  }

  @Override
  public void batch(List<? extends Row> list, Object[] os) throws IOException, InterruptedException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Row action : list) {
        if (Mutation.class.isAssignableFrom(action.getClass())) {
          repository.validateColumns(mTableDescriptor, (Mutation) action);
        }
      }
    }
    // Standard HBase processing
    wrappedTable.batch(list, os);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditorSchemaEntities(mTableDescriptor, (Mutation) action);
          }
        }
        rowCount++;
      }
    }
  }

  @Override
  @Deprecated
  public Object[] batch(List<? extends Row> list) throws IOException, InterruptedException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Row action : list) {
        if (Mutation.class.isAssignableFrom(action.getClass())) {
          repository.validateColumns(mTableDescriptor, (Mutation) action);
        }
      }
    }
    // Standard HBase processing
    Object[] os = wrappedTable.batch(list);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditorSchemaEntities(mTableDescriptor, (Mutation) action);
          }
        }
        rowCount++;
      }
    }
    return os;
  }

  @Override
  public <R> void batchCallback(List<? extends Row> list, Object[] os, Callback<R> clbck) throws IOException, InterruptedException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Row action : list) {
        if (Mutation.class.isAssignableFrom(action.getClass())) {
          repository.validateColumns(mTableDescriptor, (Mutation) action);
        }
      }
    }
    // Standard HBase processing
    wrappedTable.batchCallback(list, os, clbck);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditorSchemaEntities(mTableDescriptor, (Mutation) action);
          }
        }
        rowCount++;
      }
    }
  }

  @Override
  @Deprecated
  public <R> Object[] batchCallback(List<? extends Row> list, Callback<R> clbck) throws IOException, InterruptedException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Row action : list) {
        if (Mutation.class.isAssignableFrom(action.getClass())) {
          repository.validateColumns(mTableDescriptor, (Mutation) action);
        }
      }
    }
    // Standard HBase processing
    Object[] os = wrappedTable.batchCallback(list, clbck);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditorSchemaEntities(mTableDescriptor, (Mutation) action);
          }
        }
        rowCount++;
      }
    }
    return os;
  }

  @Override
  public Result get(Get get) throws IOException {
    return wrappedTable.get(get);
  }

  @Override
  public Result[] get(List<Get> list) throws IOException {
    return wrappedTable.get(list);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      NavigableSet<byte[]> aliasEnabledFamiliesInScan = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      if (scan.hasFamilies()) {
        for (byte[] colFamily : scan.getFamilies()) {
          if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
            aliasEnabledFamiliesInScan.add(colFamily);
          }
        }
      } else {
        for (MColumnDescriptor mColumnDescriptor : mTableDescriptor.getMColumnDescriptors()) {
          if (mColumnDescriptor.columnAliasesEnabled()) {
            aliasEnabledFamiliesInScan.add(mColumnDescriptor.getName());
          }
        }
      }
      if (!aliasEnabledFamiliesInScan.isEmpty()) {
        // build familyQualifierToAliasMap
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
                = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        if (scan.hasFamilies()) {
          for (Entry<byte[],NavigableSet<byte[]>> familyEntry : scan.getFamilyMap().entrySet()) {
            byte[] colFamily = familyEntry.getKey();
            NavigableSet<byte[]> colQualifiers = familyEntry.getValue();
            if (aliasEnabledFamiliesInScan.contains(colFamily)) {
              familyQualifierToAliasMap.put(colFamily,
                      repository.getQualifierToAliasMap(
                              mTableDescriptor.getTableName(), colFamily, colQualifiers, false));
            }
          }
        } else {
          for (byte[] aliasEnabledFamilyInScan : aliasEnabledFamiliesInScan) {
            NavigableSet<byte[]> nullSet = null;
            familyQualifierToAliasMap.put(aliasEnabledFamilyInScan,
                    repository.getQualifierToAliasMap(mTableDescriptor.getTableName(),
                            aliasEnabledFamilyInScan, nullSet, false));
          }
        }
        // derive familyAliasToQualifierMap
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap
                = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for (Entry<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasEntry
                : familyQualifierToAliasMap.entrySet()) {
          NavigableMap<byte[], byte[]> aliasToQualifierMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
          for (Entry<byte[], byte[]> qualifierToAliasMap
                  : familyQualifierToAliasEntry.getValue().entrySet()) {
            aliasToQualifierMap.put(qualifierToAliasMap.getValue(), qualifierToAliasMap.getKey());
          }
          familyAliasToQualifierMap.put(familyQualifierToAliasEntry.getKey(), aliasToQualifierMap);
        }
        Scan convertedScan = convertQualifiersToAliases(scan, familyQualifierToAliasMap);
        return new ResultScannerForAliasedFamilies(
                wrappedTable.getScanner(convertedScan), familyAliasToQualifierMap);
      }
    }
    return wrappedTable.getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] bytes) throws IOException {
    return wrappedTable.getScanner(bytes);
  }

  @Override
  public ResultScanner getScanner(byte[] bytes, byte[] bytes1) throws IOException {
    return wrappedTable.getScanner(bytes, bytes1);
  }

  @Override
  public void put(Put put) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }

    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      wrappedTable.put(convertQualifiersToAliases(put));
    } else {
      wrappedTable.put(put); // Standard HBase processing
    }

    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, put); // ColumnManager auditing
    }
  }

  @Override
  public void put(List<Put> list) throws IOException {
    for (Put put : list) {
      this.put(put); // replaced: wrappedTable.put(put);
    }
  }

  @Override
  public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Put put)
          throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing
    boolean putPerformed;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      putPerformed = wrappedTable.checkAndPut(
              bytes, bytes1, bytes2, bytes3, convertQualifiersToAliases(put));
    } else {
      putPerformed = wrappedTable.checkAndPut(bytes, bytes1, bytes2, bytes3, put);
    }
    // ColumnManager auditing
    if (putPerformed && includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, put);
    }
    return putPerformed;
  }

  @Override
  public boolean checkAndPut(
          byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp co, byte[] bytes3, Put put)
          throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing
    boolean putPerformed;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      putPerformed = wrappedTable.checkAndPut(
              bytes, bytes1, bytes2, co, bytes3, convertQualifiersToAliases(put));
    } else {
      putPerformed = wrappedTable.checkAndPut(bytes, bytes1, bytes2, co, bytes3, put);
    }
    // ColumnManager auditing
    if (putPerformed && includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, put);
    }
    return putPerformed;
  }

  @Override
  public void delete(Delete delete) throws IOException {
    wrappedTable.delete(delete);
    /*
     NOTE: Even when ALL cells pertaining to a given Column Qualifier are physically
     deleted from a Row, there is no way of telling whether the same Column Qualifier
     still exists on other Row(s) in the Table, so no Repository action is taken with
     Table delete methods!
     */
  }

  @Override
  public void delete(List<Delete> list) throws IOException {
    wrappedTable.delete(list);
  }

  @Override
  public boolean checkAndDelete(
          byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Delete delete)
          throws IOException {
    return wrappedTable.checkAndDelete(bytes, bytes1, bytes2, bytes3, delete);
  }

  @Override
  public boolean checkAndDelete(
          byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp co, byte[] bytes3, Delete delete)
          throws IOException {
    return wrappedTable.checkAndDelete(bytes, bytes1, bytes2, co, bytes3, delete);
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, rm);
    }
    // Standard HBase processing
    wrappedTable.mutateRow(rm);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, rm);
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, append);
    }
    // Standard HBase processing
    Result result = wrappedTable.append(append);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, append);
    }
    return result;
  }

  @Override
  public Result increment(Increment i) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, i);
    }
    // Standard HBase processing
    Result result = wrappedTable.increment(i);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, i);
    }
    return result;
  }

  @Override
  public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l) throws IOException {
    // ColumnManager validation
    Increment increment = null;
    if (includedInRepositoryProcessing) {
      increment = new Increment(bytes).addColumn(bytes1, bytes2, l);
      if (mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
        repository.validateColumns(mTableDescriptor, increment);
      }
    }
    // Standard HBase processing
    long returnedLong = wrappedTable.incrementColumnValue(bytes, bytes1, bytes2, l);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, increment);
    }
    return returnedLong;
  }

  @Override
  public long incrementColumnValue(
          byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability drblt) throws IOException {
    // ColumnManager validation
    Increment increment = null;
    if (includedInRepositoryProcessing) {
      increment = new Increment(bytes).addColumn(bytes1, bytes2, l);
      if (mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
        repository.validateColumns(mTableDescriptor, increment);
      }
    }
    // Standard HBase processing
    long returnedLong = wrappedTable.incrementColumnValue(bytes, bytes1, bytes2, l, drblt);
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, increment);
    }
    return returnedLong;
  }

  @Override
  public void close() throws IOException {
    wrappedTable.close();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
    return wrappedTable.coprocessorService(bytes);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> type, byte[] bytes, byte[] bytes1, Call<T, R> call) throws ServiceException, Throwable {
    return wrappedTable.coprocessorService(type, bytes, bytes1, call);
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> type, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call, Batch.Callback<R> clbck) throws ServiceException, Throwable {
    wrappedTable.coprocessorService(type, bytes, bytes1, call, clbck);
  }

  @Override
  @Deprecated
  public long getWriteBufferSize() {
    return wrappedTable.getWriteBufferSize();
  }

  @Override
  @Deprecated
  public void setWriteBufferSize(long l) throws IOException {
    wrappedTable.setWriteBufferSize(l);
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(MethodDescriptor md, Message msg, byte[] bytes, byte[] bytes1, R r) throws ServiceException, Throwable {
    return wrappedTable.batchCoprocessorService(md, msg, bytes, bytes1, r);
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
          MethodDescriptor md, Message msg, byte[] bytes, byte[] bytes1, R r,
          Batch.Callback<R> clbck) throws ServiceException, Throwable {
    wrappedTable.batchCoprocessorService(md, msg, bytes, bytes1, r, clbck);
  }

  @Override
  public boolean checkAndMutate(
          byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp co, byte[] bytes3, RowMutations rm)
          throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, rm);
    }
    // Standard HBase processing
    boolean mutationsPerformed = wrappedTable.checkAndMutate(bytes, bytes1, bytes2, co, bytes3, rm);
    // ColumnManager auditing
    if (mutationsPerformed && includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, rm);
    }
    return mutationsPerformed;
  }

  @Override
  public boolean equals(Object otherObject) {
    return wrappedTable.equals(otherObject);
  }

  @Override
  public int hashCode() {
    return wrappedTable.hashCode();
  }

  // beginning of overrides of methods introduced in HBase 1.2.2
  @Override
  public void setOperationTimeout(int i) {
    wrappedTable.setOperationTimeout(i);
  }

  @Override
  public int getOperationTimeout() {
    return wrappedTable.getOperationTimeout();
  }

  @Override
  public void setRpcTimeout(int i) {
    wrappedTable.setRpcTimeout(i);
  }

  @Override
  public int getRpcTimeout() {
    return wrappedTable.getRpcTimeout();
  }
  // end of overrides of methods introduced in HBase 1.2.2

  /**
   * IMPORTANT NOTE: In standard HBase processing, it is perfectly valid for a Scan to be submitted
   * which designates column qualifiers which do not exist on any row of the targeted Table; but
   * in column-alias processing, such a column qualifier will have no corresponding entry on the
   * ColumnManager aliasTable. Thus, any column qualifier found in the user-submitted Scan which
   * does NOT correspond to an entry in the ColumnManager aliasTable will simply be removed from
   * the Scan.
   *
   * @param originalScan Scan with standard user (full-length) column qualifiers
   * @param familyQualifierToAliasMap
   * @return converted Scan (with each user column qualifiers replace with its 4-byte alias)
   * @throws IOException
   */
  private Scan convertQualifiersToAliases(final Scan originalScan,
          NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap)
          throws IOException {
    if (!originalScan.hasFamilies()) {
      return originalScan;
    }
    NavigableMap<byte [], NavigableSet<byte[]>> modifiedFamilyMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry<byte [], NavigableSet<byte[]>> familyToQualifiersMap
            : originalScan.getFamilyMap().entrySet()) {
      byte[] colFamily = familyToQualifiersMap.getKey();
      NavigableSet<byte[]> qualifierSet = familyToQualifiersMap.getValue();
      if (qualifierSet == null
              || !mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        modifiedFamilyMap.put(colFamily, qualifierSet);
      } else {
        NavigableMap<byte[], byte[]> qualifierToAliasMap
                 = familyQualifierToAliasMap.get(colFamily);
        NavigableSet<byte[]> aliasSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (byte[] qualifier : qualifierSet) {
          byte[] alias = qualifierToAliasMap.get(qualifier);
          // NOTE that alias will be null if current qualifier was never Put into Table
          if (alias != null) {
            aliasSet.add(alias);
          }
        }
        modifiedFamilyMap.put(colFamily, aliasSet);
      }
    }
    // clone original Scan, but assign modifiedFamilyMap that has qualifiers replaced by aliases
    return new Scan(originalScan).setFamilyMap(modifiedFamilyMap);
  }

  private Put convertQualifiersToAliases(final Put originalPut) throws IOException {
    // clone Put, but remove all cell entries by setting familyToCellsMap to empty Map
    Put modifiedPut = new Put(originalPut).setFamilyCellMap(
            new TreeMap<byte [], List<Cell>>(Bytes.BYTES_COMPARATOR));

    for (Entry<byte[], List<Cell>> familyToCellsMap : originalPut.getFamilyCellMap().entrySet()) {
      byte[] colFamily = familyToCellsMap.getKey();
      List<Cell> cellList = familyToCellsMap.getValue();
      if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        NavigableMap<byte[], byte[]> qualifierToAliasMap
                = repository.getQualifierToAliasMap(
                        mTableDescriptor.getTableName(), colFamily, cellList, true);
        for (Cell originalCell : cellList) {
          modifiedPut.addColumn(colFamily,
                  qualifierToAliasMap.get(Bytes.copy(originalCell.getQualifierArray(),
                          originalCell.getQualifierOffset(), originalCell.getQualifierLength())),
                  originalCell.getTimestamp(),
                  Bytes.copy(originalCell.getValueArray(), originalCell.getValueOffset(),
                          originalCell.getValueLength()));
        }
      } else {
        for (Cell originalCell : cellList) {
          modifiedPut.add(originalCell);
        }
      }
    }
    return modifiedPut;
  }

  private Append convertQualifiersToAliases(final Append originalAppend) throws IOException {
    // clone Append, but remove all cell entries by setting familyToCellsMap to empty Map
    Append modifiedAppend = new Append(originalAppend).setFamilyCellMap(
            new TreeMap<byte [], List<Cell>>(Bytes.BYTES_COMPARATOR));

    for (Entry<byte[], List<Cell>> familyToCellsMap
            : originalAppend.getFamilyCellMap().entrySet()) {
      byte[] colFamily = familyToCellsMap.getKey();
      List<Cell> cellList = familyToCellsMap.getValue();
      if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        NavigableMap<byte[], byte[]> qualifierToAliasMap
                = repository.getQualifierToAliasMap(
                        mTableDescriptor.getTableName(), colFamily, cellList, true);
        for (Cell originalCell : cellList) {
          modifiedAppend.add(colFamily,
                  qualifierToAliasMap.get(Bytes.copy(originalCell.getQualifierArray(),
                          originalCell.getQualifierOffset(), originalCell.getQualifierLength())),
                  Bytes.copy(originalCell.getValueArray(), originalCell.getValueOffset(),
                          originalCell.getValueLength()));
        }
      } else {
        for (Cell originalCell : cellList) {
          modifiedAppend.add(originalCell);
        }
      }
    }
    return modifiedAppend;
  }

  private Increment convertQualifiersToAliases(final Increment originalIncrement)
          throws IOException {
    // clone Increment, but remove all cell entries by setting familyToCellsMap to empty Map
    Increment modifiedIncrement = new Increment(originalIncrement).setFamilyCellMap(
            new TreeMap<byte [], List<Cell>>(Bytes.BYTES_COMPARATOR));

    for (Entry<byte[], List<Cell>> familyToCellsMap
            : originalIncrement.getFamilyCellMap().entrySet()) {
      byte[] colFamily = familyToCellsMap.getKey();
      List<Cell> cellList = familyToCellsMap.getValue();
      if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        NavigableMap<byte[], byte[]> qualifierToAliasMap
                = repository.getQualifierToAliasMap(
                        mTableDescriptor.getTableName(), colFamily, cellList, true);
        for (Cell originalCell : cellList) {
          modifiedIncrement.addColumn(colFamily,
                  qualifierToAliasMap.get(Bytes.copy(originalCell.getQualifierArray(),
                          originalCell.getQualifierOffset(), originalCell.getQualifierLength())),
                  Bytes.toLong(Bytes.copy(originalCell.getValueArray(),
                          originalCell.getValueOffset(), originalCell.getValueLength())));
        }
      } else {
        for (Cell originalCell : cellList) {
          modifiedIncrement.add(originalCell);
        }
      }
    }
    return modifiedIncrement;
  }

  /**
   * IMPORTANT NOTE: In standard HBase processing, it is perfectly valid for a Delete to be
   * submitted which designates column qualifiers which do not exist on any row of the targeted
   * Table; but in column-alias processing, such a column qualifier will have no corresponding
   * entry on the ColumnManager aliasTable. Thus, any column qualifier found in the user-submitted
   * Delete which does NOT correspond to an entry in the ColumnManager aliasTable will simply
   * not be included in the alias-converted Delete.
   *
   * @param originalDelete the Delete as submitted by the user application
   * @return Delete object with column qualifiers replaced by corresponding aliases (for columns in
   * aliasEnabled Column Families)
   * @throws IOException
   */
  private Delete convertQualifiersToAliases(final Delete originalDelete) throws IOException {
    // clone Delete, but remove all cell entries by setting familyToCellsMap to empty Map
    Delete modifiedDelete = new Delete(originalDelete).setFamilyCellMap(
            new TreeMap<byte [], List<Cell>>(Bytes.BYTES_COMPARATOR));

    for (Entry<byte[], List<Cell>> familyToCellsMap : originalDelete.getFamilyCellMap().entrySet()) {
      byte[] colFamily = familyToCellsMap.getKey();
      List<Cell> cellList = familyToCellsMap.getValue();
      if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        NavigableMap<byte[], byte[]> qualifierToAliasMap
                = repository.getQualifierToAliasMap(
                        mTableDescriptor.getTableName(), colFamily, cellList, false);
        for (Cell originalCell : cellList) {
          byte[] colQualifier = Bytes.copy(originalCell.getQualifierArray(),
                  originalCell.getQualifierOffset(), originalCell.getQualifierLength());
          byte[] colAlias = qualifierToAliasMap.get(colQualifier);
          if (originalCell.getTypeByte() == KeyValue.Type.DeleteFamilyVersion.getCode()) {
            modifiedDelete.addFamilyVersion(colFamily, originalCell.getTimestamp());
          } else if (originalCell.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
            modifiedDelete.addFamily(colFamily);
          } else if (originalCell.getTypeByte() == KeyValue.Type.DeleteColumn.getCode()) {
            if (colAlias != null) {
              modifiedDelete.addColumns(colFamily, colAlias, originalCell.getTimestamp());
            }
          } else if (originalCell.getTypeByte() == KeyValue.Type.Delete.getCode()) {
            if (colAlias != null) {
              modifiedDelete.addColumn(colFamily, colAlias, originalCell.getTimestamp());
            }
          }
        }
      } else {  // colFamily NOT aliasEnabled, so "clone" cells using standard Delete interface
        for (Cell originalCell : cellList) {
          byte[] colQualifier = Bytes.copy(originalCell.getQualifierArray(),
                  originalCell.getQualifierOffset(), originalCell.getQualifierLength());
          if (originalCell.getTypeByte() == KeyValue.Type.DeleteFamilyVersion.getCode()) {
            modifiedDelete.addFamilyVersion(colFamily, originalCell.getTimestamp());
          } else if (originalCell.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
            modifiedDelete.addFamily(colFamily);
          } else if (originalCell.getTypeByte() == KeyValue.Type.DeleteColumn.getCode()) {
            modifiedDelete.addColumns(colFamily, colQualifier, originalCell.getTimestamp());
          } else if (originalCell.getTypeByte() == KeyValue.Type.Delete.getCode()) {
            modifiedDelete.addColumn(colFamily, colQualifier, originalCell.getTimestamp());
          }
        }
      }
    }
    return modifiedDelete;
  }

  private Result convertAliasesToQualifiers(Result result,
          NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap) {
    NavigableSet<Cell> convertedCellSet = new TreeSet<Cell>(KeyValue.COMPARATOR);
    for (Cell originalCell : result.rawCells()) {
      byte[] cellFamily = Bytes.copy(originalCell.getFamilyArray(),
                      originalCell.getFamilyOffset(), originalCell.getFamilyLength());
      NavigableMap<byte[], byte[]> aliasToQualifierMap = familyAliasToQualifierMap.get(cellFamily);
      if (aliasToQualifierMap == null) {
        convertedCellSet.add(originalCell); // if no aliasToQualifierMap, no conversion done
      } else {
        convertedCellSet.add(CellUtil.createCell(
                Bytes.copy(originalCell.getRowArray(), originalCell.getRowOffset(),
                        originalCell.getRowLength()),
                cellFamily,
                aliasToQualifierMap.get(Bytes.copy(originalCell.getQualifierArray(),
                        originalCell.getQualifierOffset(), originalCell.getQualifierLength())),
                originalCell.getTimestamp(), KeyValue.Type.codeToType(originalCell.getTypeByte()),
                Bytes.copy(originalCell.getValueArray(), originalCell.getValueOffset(),
                        originalCell.getValueLength()),
                Bytes.copy(originalCell.getTagsArray(), originalCell.getTagsOffset(),
                        originalCell.getTagsLength())));
      }
    }
    return Result.create(convertedCellSet.toArray(new Cell[convertedCellSet.size()]));
  }

  /**
   * ResultScannerForAliasedFamilies wraps the ResultScanner returned by Table#getScanner
   * and converts each of its component Result objects by replacing any column-aliases with
   * user-column-qualifiers.
   */
  class ResultScannerForAliasedFamilies implements ResultScanner {

    private final ResultScanner wrappedResultScanner;
    private final NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap;

    ResultScannerForAliasedFamilies(ResultScanner resultScanner,
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap) {
      wrappedResultScanner = resultScanner;
      this.familyAliasToQualifierMap = familyAliasToQualifierMap;
    }

    @Override
    public Result next() throws IOException {
      return convertAliasesToQualifiers(wrappedResultScanner.next(), familyAliasToQualifierMap);
    }

    @Override
    public Result[] next(int i) throws IOException {
      List<Result> convertedResultList = new ArrayList<>();
      for (Result originalResult : wrappedResultScanner.next(i)) {
        convertedResultList.add(convertAliasesToQualifiers(
                originalResult, familyAliasToQualifierMap));
      }
      return convertedResultList.toArray(new Result[convertedResultList.size()]);
    }

    @Override
    public void close() {
      wrappedResultScanner.close();
    }

    @Override
    public Iterator<Result> iterator() {
      return new MResultIterator(wrappedResultScanner.iterator());
    }

    class MResultIterator implements Iterator<Result> {
      private final Iterator<Result> wrappedIterator;

      MResultIterator(Iterator<Result> wrappedIterator) {
        this.wrappedIterator = wrappedIterator;
      }

      @Override
      public boolean hasNext() {
        return wrappedIterator.hasNext();
      }

      @Override
      public Result next() {
        return convertAliasesToQualifiers(wrappedIterator.next(), familyAliasToQualifierMap);
      }
    }
  }
}
