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
import java.util.Iterator;
import java.util.LinkedList;
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
import org.apache.hadoop.hbase.io.TimeRange;
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
  private static final NavigableSet<byte[]> NULL_NAVIGABLE_SET = null;

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
  public void batch(List<? extends Row> rowList, Object[] os)
          throws IOException, InterruptedException {
    batchProcessing(rowList, os, null, false);
  }

  @Override
  @Deprecated
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    Object[] results = new Object[actions.size()];
    batch(actions, results);
    return results;
  }

  @Override
  public <R> void batchCallback(List<? extends Row> list, Object[] os, Callback<R> clbck)
          throws IOException, InterruptedException {
    batchProcessing(list, os, clbck, true);
  }

  @Override
  @Deprecated
  public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
          throws IOException, InterruptedException {
    Object[] results = new Object[actions.size()];
    batchCallback(actions, results, callback);
    return results;
  }

  private <R> void batchProcessing(List<? extends Row> actions, Object[] results,
          Callback<R> callback, boolean includeCallback)
          throws IOException, InterruptedException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      for (Row action : actions) {
        if (Mutation.class.isAssignableFrom(action.getClass())) {
          repository.validateColumns(mTableDescriptor, (Mutation) action);
        }
      }
    }
    // Alias processing
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      // NOTE: alias-conversion for Mutations must be done before alias-conversion for Gets
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = getFamilyQualifierToAliasMap(actions, 0);
      List<Row> convertedRows = new LinkedList<>();
      for (Row row : actions) {
        if (Mutation.class.isAssignableFrom(row.getClass())) {
          convertedRows.add(convertQualifiersToAliases(row, familyQualifierToAliasMap, 0));
        }
      }
      int listIndex = 0;
      for (Row row : actions) {
        if (Get.class.isAssignableFrom(row.getClass())) {
          convertedRows.add(
                  listIndex, convertQualifiersToAliases((Get)row, familyQualifierToAliasMap));
        }
        listIndex++;
      }
      // invoke "native" method
      if (includeCallback) {
        wrappedTable.batchCallback(convertedRows, results, callback);
      } else {
        wrappedTable.batch(convertedRows, results);
      }
      // convert Result objects (if any)
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap
              = getFamilyAliasToQualifierMap(familyQualifierToAliasMap);
      Object[] convertedResults = new Object[results.length];
      int objectIndex = 0;
      for (Object returnedObject : results) {
        if (Result.class.isAssignableFrom(results.getClass())) {
          convertedResults[objectIndex]
                  = convertAliasesToQualifiers((Result)returnedObject, familyAliasToQualifierMap);
        } else {
          convertedResults[objectIndex] = results[objectIndex];
        }
        objectIndex++;
      }
      results = convertedResults;
    } else { // NO alias processing
      if (includeCallback) {
        wrappedTable.batchCallback(actions, results, callback);
      } else {
        wrappedTable.batch(actions, results);
      }
    }
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : results) {
        if (actionSucceeded != null) {
          Row action = actions.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditorSchemaEntities(mTableDescriptor, (Mutation) action);
          }
        }
        rowCount++;
      }
    }
  }

  @Override
  public Result get(Get get) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = getFamilyQualifierToAliasMap(get);
      Result result = wrappedTable.get(convertQualifiersToAliases(get, familyQualifierToAliasMap));
      return convertAliasesToQualifiers(result,
              getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
    } else {
      return wrappedTable.get(get);
    }
  }

  @Override
  public Result[] get(List<Get> listOfGets) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      // convert Gets before invoking "native" method
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = getFamilyQualifierToAliasMap(listOfGets);
      if (!familyQualifierToAliasMap.isEmpty()) {
        List<Get> convertedGets = new LinkedList<>();
        for (Get originalGet : listOfGets) {
          convertedGets.add(convertQualifiersToAliases(originalGet, familyQualifierToAliasMap));
        }

        // invoke "native" HBase method with aliased Gets
        Result[] originalResults = wrappedTable.get(convertedGets);

        // do alias-to-qualifier conversion of Results before returning them
        List<Result> convertedResults = new LinkedList<>();
        for (Result originalResult : originalResults) {
          convertedResults.add(
                  convertAliasesToQualifiers(originalResult, familyQualifierToAliasMap));
        }
        return convertedResults.toArray(new Result[convertedResults.size()]);
      }
    }
    return wrappedTable.get(listOfGets);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = getFamilyQualifierToAliasMap(scan);
      if (!familyQualifierToAliasMap.isEmpty()) {
        return new ResultScannerForAliasedFamilies(
                wrappedTable.getScanner(
                        convertQualifiersToAliases(scan, familyQualifierToAliasMap)),
                getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
      }
    }
    return wrappedTable.getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] colFamily) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
      return new ResultScannerForAliasedFamilies(
              wrappedTable.getScanner(colFamily), getFamilyAliasToQualifierMap(colFamily));
    } else {
      return wrappedTable.getScanner(colFamily);
    }
  }

  @Override
  public ResultScanner getScanner(byte[] colFamily, byte[] colQualifier) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
      return new ResultScannerForAliasedFamilies(
              wrappedTable.getScanner(colFamily,
                      repository.getAlias(mTableDescriptor, colFamily, colQualifier)),
              getFamilyAliasToQualifierMap(colFamily, colQualifier));
    } else {
      return wrappedTable.getScanner(colFamily, colQualifier);
    }
  }

  @Override
  public void put(Put put) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing (with aliasing, if necessary)
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      wrappedTable.put(convertQualifiersToAliases(put));
    } else {
      wrappedTable.put(put);
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
  public boolean checkAndPut(
          byte[] rowId, byte[] colFamily, byte[] colQualifier, byte[] colValue, Put put)
          throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing (with aliasing, if necessary)
    boolean putPerformed;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      putPerformed = wrappedTable.checkAndPut(rowId, colFamily,
              repository.getAlias(mTableDescriptor, colFamily, colQualifier),
              colValue, convertQualifiersToAliases(put));
    } else {
      putPerformed = wrappedTable.checkAndPut(rowId, colFamily, colQualifier, colValue, put);
    }
    // ColumnManager auditing
    if (putPerformed && includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, put);
    }
    return putPerformed;
  }

  @Override
  public boolean checkAndPut(byte[] rowId, byte[] colFamily, byte[] colQualifier, CompareOp co,
          byte[] colValue, Put put)
          throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing (with aliasing, if necessary)
    boolean putPerformed;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      putPerformed = wrappedTable.checkAndPut(rowId, colFamily,
              repository.getAlias(mTableDescriptor, colFamily, colQualifier),
              co, colValue, convertQualifiersToAliases(put));
    } else {
      putPerformed = wrappedTable.checkAndPut(rowId, colFamily, colQualifier, co, colValue, put);
    }
    // ColumnManager auditing
    if (putPerformed && includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, put);
    }
    return putPerformed;
  }

  @Override
  public void delete(Delete delete) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      wrappedTable.delete(convertQualifiersToAliases(delete));
    } else {
      wrappedTable.delete(delete);
    }
  }

  @Override
  public void delete(List<Delete> deleteList) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      List<Delete> convertedDeleteList = new LinkedList<>();
      for (Delete delete : deleteList) {
        convertedDeleteList.add(convertQualifiersToAliases(delete));
      }
      wrappedTable.delete(convertedDeleteList);
    } else {
      wrappedTable.delete(deleteList);
    }
  }

  @Override
  public boolean checkAndDelete(
          byte[] rowId, byte[] colFamily, byte[] colQualifier, byte[] colValue, Delete delete)
          throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      return wrappedTable.checkAndDelete(rowId, colFamily,
              repository.getAlias(mTableDescriptor, colFamily, colQualifier),
              colValue, convertQualifiersToAliases(delete));
    } else {
      return wrappedTable.checkAndDelete(rowId, colFamily, colQualifier, colValue, delete);
    }
  }

  @Override
  public boolean checkAndDelete(byte[] rowId, byte[] colFamily, byte[] colQualifier,
          CompareOp co, byte[] colValue, Delete delete)
          throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      return wrappedTable.checkAndDelete(rowId, colFamily,
              repository.getAlias(mTableDescriptor, colFamily, colQualifier),
              co, colValue, convertQualifiersToAliases(delete));
    } else {
      return wrappedTable.checkAndDelete(rowId, colFamily, colQualifier, co, colValue, delete);
    }
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, rm);
    }
    // Standard HBase processing (with aliasing, if necessary)
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      wrappedTable.mutateRow(convertQualifiersToAliases(rm));
    } else {
      wrappedTable.mutateRow(rm);
    }
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
    // Standard HBase processing (with aliasing, if necessary)
    Result result;
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap = null;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      familyQualifierToAliasMap = getFamilyQualifierToAliasMap(append);
      result = wrappedTable.append(convertQualifiersToAliases(append, familyQualifierToAliasMap));
    } else {
      result = wrappedTable.append(append);
    }
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, append);
    }
    // Return processing (with aliasing, if necessary)
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      return convertAliasesToQualifiers(result,
              getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
    } else {
      return result;
    }
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, increment);
    }
    // Standard HBase processing (with aliasing, if necessary)
    Result result;
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap = null;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      familyQualifierToAliasMap = getFamilyQualifierToAliasMap(increment);
      result = wrappedTable.increment(
              convertQualifiersToAliases(increment, familyQualifierToAliasMap));
    } else {
      result = wrappedTable.increment(increment);
    }
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, increment);
    }
    // Return processing (with aliasing, if necessary)
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      return convertAliasesToQualifiers(result,
              getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
    } else {
      return result;
    }
  }

  @Override
  public long incrementColumnValue(byte[] rowId, byte[] colFamily, byte[] colQualifier, long l)
          throws IOException {
    // ColumnManager validation
    Increment increment = null;
    if (includedInRepositoryProcessing) {
      increment = new Increment(rowId).addColumn(colFamily, colQualifier, l);
      if (mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
        repository.validateColumns(mTableDescriptor, increment);
      }
    }
    // Standard HBase processing (with aliasing, if necessary)
    long returnedLong;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      returnedLong = wrappedTable.incrementColumnValue(rowId, colFamily,
              repository.getAlias(mTableDescriptor, colFamily, colQualifier), l);
    } else {
      returnedLong = wrappedTable.incrementColumnValue(rowId, colFamily, colQualifier, l);
    }
    // ColumnManager auditing
    if (includedInRepositoryProcessing) {
      repository.putColumnAuditorSchemaEntities(mTableDescriptor, increment);
    }
    return returnedLong;
  }

  @Override
  public long incrementColumnValue(
          byte[] rowId, byte[] colFamily, byte[] colQualifier, long l, Durability drblt)
          throws IOException {
    // ColumnManager validation
    Increment increment = null;
    if (includedInRepositoryProcessing) {
      increment = new Increment(rowId).addColumn(colFamily, colQualifier, l);
      if (mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
        repository.validateColumns(mTableDescriptor, increment);
      }
    }
    // Standard HBase processing (with aliasing, if necessary)
    long returnedLong;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      returnedLong = wrappedTable.incrementColumnValue(rowId, colFamily,
              repository.getAlias(mTableDescriptor, colFamily, colQualifier), l, drblt);
    } else {
      returnedLong = wrappedTable.incrementColumnValue(rowId, colFamily, colQualifier, l, drblt);
    }
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
  public boolean checkAndMutate(byte[] rowId, byte[] colFamily, byte[] colQualifier,
          CompareOp co, byte[] colValue, RowMutations rm)
          throws IOException {
    // ColumnManager validation
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, rm);
    }
    // Standard HBase processing (with aliasing, if necessary)
    boolean mutationsPerformed;
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      mutationsPerformed = wrappedTable.checkAndMutate(rowId, colFamily,
              repository.getAlias(mTableDescriptor, colFamily, colQualifier), co, colValue,
              convertQualifiersToAliases(rm));
    } else {
      mutationsPerformed
              = wrappedTable.checkAndMutate(rowId, colFamily, colQualifier, co, colValue, rm);
    }
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

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getFamilyQualifierToAliasMap(Get get)
          throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    NavigableSet<byte[]> aliasEnabledFamiliesInScan = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    if (get.hasFamilies()) {
      for (byte[] colFamily : get.familySet()) {
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
      if (get.hasFamilies()) {
        for (Entry<byte[],NavigableSet<byte[]>> familyEntry : get.getFamilyMap().entrySet()) {
          byte[] colFamily = familyEntry.getKey();
          NavigableSet<byte[]> colQualifiers = familyEntry.getValue(); // could be null
          if (aliasEnabledFamiliesInScan.contains(colFamily)) {
            familyQualifierToAliasMap.put(colFamily,
                    repository.getQualifierToAliasMap(
                            mTableDescriptor.getTableName(), colFamily, colQualifiers, false));
          }
        }
      } else {
        for (byte[] aliasEnabledFamilyInScan : aliasEnabledFamiliesInScan) {
          familyQualifierToAliasMap.put(aliasEnabledFamilyInScan,
                  repository.getQualifierToAliasMap(mTableDescriptor.getTableName(),
                          aliasEnabledFamilyInScan, NULL_NAVIGABLE_SET, false));
        }
      }
    }
    return familyQualifierToAliasMap;
  }

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getFamilyQualifierToAliasMap(
          List<? extends Row> rowList, int intForUniqueSignature)
          throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> masterFamilyQualifierToAliasMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Row row : rowList) {
      Class<?> rowClass = row.getClass();
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> partialFamilyQualifierToAliasMap
              = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      if (Append.class.isAssignableFrom(rowClass)) {
        partialFamilyQualifierToAliasMap = getFamilyQualifierToAliasMap((Append)row);
      } else if (Get.class.isAssignableFrom(rowClass)) {
        partialFamilyQualifierToAliasMap = getFamilyQualifierToAliasMap((Get)row);
      } else if (Increment.class.isAssignableFrom(rowClass)) {
        partialFamilyQualifierToAliasMap = getFamilyQualifierToAliasMap((Increment)row);
      } else if (Delete.class.isAssignableFrom(rowClass)
              || Put.class.isAssignableFrom(rowClass)
              || RowMutations.class.isAssignableFrom(rowClass)) {
        continue;
      }
      for (Entry<byte[], NavigableMap<byte[], byte[]>> partialFamilyEntry
              : partialFamilyQualifierToAliasMap.entrySet()) {
        byte[] colFamily = partialFamilyEntry.getKey();
        NavigableMap<byte[], byte[]> masterQualifierToAliasMap
                = masterFamilyQualifierToAliasMap.get(colFamily);
        if (masterQualifierToAliasMap == null) {
          masterFamilyQualifierToAliasMap.put(colFamily, partialFamilyEntry.getValue());
        } else {
          masterQualifierToAliasMap.putAll(partialFamilyEntry.getValue());
        }
      }
    }
    return masterFamilyQualifierToAliasMap;
  }

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getFamilyQualifierToAliasMap(
          List<Get> gets)
          throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    NavigableSet<byte[]> aliasEnabledFamiliesInScan = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (Get get : gets) {
      if (get.hasFamilies()) {
        for (byte[] colFamily : get.familySet()) {
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
    }
    if (!aliasEnabledFamiliesInScan.isEmpty()) {
      for (Get get : gets) {
        if (get.hasFamilies()) {
          for (Entry<byte[],NavigableSet<byte[]>> familyEntry : get.getFamilyMap().entrySet()) {
            byte[] colFamily = familyEntry.getKey();
            NavigableSet<byte[]> colQualifiers = familyEntry.getValue(); // could be null
            if (aliasEnabledFamiliesInScan.contains(colFamily)) {
              NavigableMap<byte[], byte[]> qualifierToAliasMap
                      = familyQualifierToAliasMap.get(colFamily);
              if (qualifierToAliasMap == null) {
                familyQualifierToAliasMap.put(colFamily,
                        repository.getQualifierToAliasMap(
                                mTableDescriptor.getTableName(), colFamily, colQualifiers, false));
              } else {
                qualifierToAliasMap.putAll(repository.getQualifierToAliasMap(
                                mTableDescriptor.getTableName(), colFamily, colQualifiers, false));
              }
            }
          }
        } else {
          // if no families specified in Get, need all alias entries for entire family
          for (byte[] aliasEnabledFamilyInScan : aliasEnabledFamiliesInScan) {
            familyQualifierToAliasMap.put(aliasEnabledFamilyInScan,
                    repository.getQualifierToAliasMap(mTableDescriptor.getTableName(),
                            aliasEnabledFamilyInScan, NULL_NAVIGABLE_SET, false));
          }
        }
      }
    }
    return familyQualifierToAliasMap;
  }

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getFamilyQualifierToAliasMap(Scan scan)
          throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
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
      if (scan.hasFamilies()) {
        for (Entry<byte[],NavigableSet<byte[]>> familyEntry : scan.getFamilyMap().entrySet()) {
          byte[] colFamily = familyEntry.getKey();
          NavigableSet<byte[]> colQualifiers = familyEntry.getValue(); // could be null
          if (aliasEnabledFamiliesInScan.contains(colFamily)) {
            familyQualifierToAliasMap.put(colFamily,
                    repository.getQualifierToAliasMap(
                            mTableDescriptor.getTableName(), colFamily, colQualifiers, false));
          }
        }
      } else {
        for (byte[] aliasEnabledFamilyInScan : aliasEnabledFamiliesInScan) {
          familyQualifierToAliasMap.put(aliasEnabledFamilyInScan,
                  repository.getQualifierToAliasMap(mTableDescriptor.getTableName(),
                          aliasEnabledFamilyInScan, NULL_NAVIGABLE_SET, false));
        }
      }
    }
    return familyQualifierToAliasMap;
  }

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getFamilyQualifierToAliasMap(
          Append append)
          throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry<byte[], List<Cell>> familyToCellsMap : append.getFamilyCellMap().entrySet()) {
      byte[] colFamily = familyToCellsMap.getKey();
      List<Cell> cellList = familyToCellsMap.getValue();
      if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        familyQualifierToAliasMap.put (colFamily,
                repository.getQualifierToAliasMap(mTableDescriptor.getTableName(),
                        colFamily, cellList, true));
      }
    }
    return familyQualifierToAliasMap;
  }

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> getFamilyQualifierToAliasMap(
          Increment increment)
          throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry<byte[], List<Cell>> familyToCellsMap : increment.getFamilyCellMap().entrySet()) {
      byte[] colFamily = familyToCellsMap.getKey();
      List<Cell> cellList = familyToCellsMap.getValue();
      if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        familyQualifierToAliasMap.put(colFamily, repository.getQualifierToAliasMap(
                        mTableDescriptor.getTableName(), colFamily, cellList, true));
      }
    }
    return familyQualifierToAliasMap;
  }


  private NavigableMap<byte[], NavigableMap<byte[], byte[]>>  getFamilyAliasToQualifierMap(
          NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap) {
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
    return familyAliasToQualifierMap;
  }

  private NavigableMap<byte[],NavigableMap<byte[],byte[]>> getFamilyAliasToQualifierMap(
          byte[] colFamily)
          throws IOException {
    return getFamilyAliasToQualifierMap(colFamily, NULL_NAVIGABLE_SET);
  }

  private NavigableMap<byte[],NavigableMap<byte[],byte[]>> getFamilyAliasToQualifierMap(
          byte[] colFamily, byte[] colQualifier)
          throws IOException {
    NavigableSet<byte[]> colQualifierSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    colQualifierSet.add(colQualifier);
    return getFamilyAliasToQualifierMap(colFamily, colQualifierSet);
  }

  private NavigableMap<byte[],NavigableMap<byte[],byte[]>> getFamilyAliasToQualifierMap(
          byte[] colFamily, NavigableSet<byte[]> colQualifierSet)
          throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      familyQualifierToAliasMap.put(colFamily,
              repository.getQualifierToAliasMap(mTableDescriptor.getTableName(),
                      colFamily, colQualifierSet, false));
      familyAliasToQualifierMap = getFamilyAliasToQualifierMap(familyQualifierToAliasMap);
    }
    return familyAliasToQualifierMap;
  }

  private Row convertQualifiersToAliases(final Row originalRow,
          NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap,
          int intForUniqueSignature)
          throws IOException {
    // Append, Delete, Get, Increment, Mutation, Put, RowMutations
    Class<?> originalRowClass = originalRow.getClass();
    if (Append.class.isAssignableFrom(originalRowClass)) {
      return convertQualifiersToAliases((Append)originalRow, familyQualifierToAliasMap);
    } else if (Delete.class.isAssignableFrom(originalRowClass)) {
      return convertQualifiersToAliases((Delete)originalRow);
    } else if (Get.class.isAssignableFrom(originalRowClass)) {
      return convertQualifiersToAliases((Get)originalRow, familyQualifierToAliasMap);
    } else if (Increment.class.isAssignableFrom(originalRowClass)) {
      return convertQualifiersToAliases((Increment)originalRow, familyQualifierToAliasMap);
    } else if (Put.class.isAssignableFrom(originalRowClass)) {
      return convertQualifiersToAliases((Put)originalRow);
    } else if (RowMutations.class.isAssignableFrom(originalRowClass)) {
      return convertQualifiersToAliases((RowMutations)originalRow);
    }
    return null;
  }


  private Get convertQualifiersToAliases(final Get originalGet,
          NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap)
          throws IOException {
    if (!originalGet.hasFamilies()) {
      return originalGet;
    }
    NavigableMap<byte [], NavigableSet<byte[]>> modifiedFamilyMap
            = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry<byte [], NavigableSet<byte[]>> familyToQualifiersMap
            : originalGet.getFamilyMap().entrySet()) {
      byte[] colFamily = familyToQualifiersMap.getKey();
      NavigableSet<byte[]> colQualifierSet = familyToQualifiersMap.getValue();
      if (colQualifierSet == null
              || !mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        modifiedFamilyMap.put(colFamily, colQualifierSet); // no modifications
      } else {
        NavigableMap<byte[], byte[]> qualifierToAliasMap = familyQualifierToAliasMap.get(colFamily);
        NavigableSet<byte[]> aliasSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (byte[] qualifier : colQualifierSet) {
          byte[] alias = qualifierToAliasMap.get(qualifier);
          aliasSet.add(alias);
        }
        modifiedFamilyMap.put(colFamily, aliasSet);
      }
    }
    Get convertedGet = cloneGetWithoutFamilyMap(originalGet);
    for (Entry<byte [], NavigableSet<byte[]>> modifiedFamilyToQualifiersMap
            : modifiedFamilyMap.entrySet()) {
      byte[] colFamily = modifiedFamilyToQualifiersMap.getKey();
      NavigableSet<byte[]> colQualifierSet = modifiedFamilyToQualifiersMap.getValue();
      if (colQualifierSet == null) {
        convertedGet.addFamily(colFamily);
      } else {
        for (byte[] colQualifier : colQualifierSet) {
          convertedGet.addColumn(colFamily, colQualifier);
        }
      }
    }
    return convertedGet;
  }

  /**
   * Method may need modification if Get attributes are added or removed in future HBase releases.
   *
   * @param originalGet
   * @return convertedGet
   * @throws IOException
   */
  private Get cloneGetWithoutFamilyMap(Get originalGet) throws IOException {
    Get convertedGet = new Get(originalGet.getRow());
    // from Query
    convertedGet.setFilter(originalGet.getFilter());
    convertedGet.setReplicaId(originalGet.getReplicaId());
    convertedGet.setConsistency(originalGet.getConsistency());
    // from Get
    convertedGet.setCacheBlocks(originalGet.getCacheBlocks());
    convertedGet.setMaxVersions(originalGet.getMaxVersions());
    convertedGet.setMaxResultsPerColumnFamily(originalGet.getMaxResultsPerColumnFamily());
    convertedGet.setRowOffsetPerColumnFamily(originalGet.getRowOffsetPerColumnFamily());
    convertedGet.setCheckExistenceOnly(originalGet.isCheckExistenceOnly());
    convertedGet.setClosestRowBefore(originalGet.isClosestRowBefore());
    for (Map.Entry<String, byte[]> attr : originalGet.getAttributesMap().entrySet()) {
      convertedGet.setAttribute(attr.getKey(), attr.getValue());
    }
    for (Map.Entry<byte[], TimeRange> entry : originalGet.getColumnFamilyTimeRange().entrySet()) {
      TimeRange tr = entry.getValue();
      convertedGet.setColumnFamilyTimeRange(entry.getKey(), tr.getMin(), tr.getMax());
    }
    return convertedGet;
  }

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
      NavigableSet<byte[]> colQualifierSet = familyToQualifiersMap.getValue();
      if (colQualifierSet == null
              || !mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        modifiedFamilyMap.put(colFamily, colQualifierSet);
      } else {
        NavigableMap<byte[], byte[]> qualifierToAliasMap
                 = familyQualifierToAliasMap.get(colFamily);
        NavigableSet<byte[]> aliasSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (byte[] qualifier : colQualifierSet) {
          byte[] alias = qualifierToAliasMap.get(qualifier);
          aliasSet.add(alias);
        }
        modifiedFamilyMap.put(colFamily, aliasSet);
      }
    }
    // clone original Scan, but assign modifiedFamilyMap that has qualifiers replaced by aliases
    return new Scan(originalScan).setFamilyMap(modifiedFamilyMap);
  }

  private RowMutations convertQualifiersToAliases(final RowMutations originalRowMutations)
          throws IOException{
    RowMutations modifiedRowMutations = new RowMutations(originalRowMutations.getRow());
    for (Mutation originalMutation : originalRowMutations.getMutations()) {
      Class<?> mutationClass = originalMutation.getClass();
      if (Put.class.isAssignableFrom(mutationClass)) {
        modifiedRowMutations.add(convertQualifiersToAliases((Put)originalMutation));
      } else if (Delete.class.isAssignableFrom(mutationClass)) {
        modifiedRowMutations.add(convertQualifiersToAliases((Delete)originalMutation));
      }
    }
    return modifiedRowMutations;
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

  private Append convertQualifiersToAliases(final Append originalAppend,
          NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap)
          throws IOException {
    // clone Append, but remove all cell entries by setting familyToCellsMap to empty Map
    Append modifiedAppend = new Append(originalAppend).setFamilyCellMap(
            new TreeMap<byte [], List<Cell>>(Bytes.BYTES_COMPARATOR));

    for (Entry<byte[], List<Cell>> familyToCellsMap
            : originalAppend.getFamilyCellMap().entrySet()) {
      byte[] colFamily = familyToCellsMap.getKey();
      List<Cell> cellList = familyToCellsMap.getValue();
      if (mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
        for (Cell originalCell : cellList) {
          modifiedAppend.add(colFamily,
                  familyQualifierToAliasMap.get(colFamily).get(
                          Bytes.copy(originalCell.getQualifierArray(),
                                  originalCell.getQualifierOffset(),
                                  originalCell.getQualifierLength())),
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

  private Increment convertQualifiersToAliases(final Increment originalIncrement,
          NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap)
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
                = familyQualifierToAliasMap.get(colFamily);
        for (Cell originalCell : cellList) {
          modifiedIncrement.addColumn(colFamily,
                  familyQualifierToAliasMap.get(colFamily).get(
                          Bytes.copy(originalCell.getQualifierArray(),
                                  originalCell.getQualifierOffset(),
                                  originalCell.getQualifierLength())),
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
            modifiedDelete.addColumns(colFamily, colAlias, originalCell.getTimestamp());
          } else if (originalCell.getTypeByte() == KeyValue.Type.Delete.getCode()) {
            modifiedDelete.addColumn(colFamily, colAlias, originalCell.getTimestamp());
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
      List<Result> convertedResultList = new LinkedList<>();
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
