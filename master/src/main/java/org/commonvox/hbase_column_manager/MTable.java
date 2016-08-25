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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
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
    // Alias processing
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      return wrappedTable.exists(repository.convertQualifiersToAliases(
              mTableDescriptor, get,
              repository.getFamilyQualifierToAliasMap(mTableDescriptor, get)));
    } else {
      return wrappedTable.exists(get);
    }
  }

  @Override
  public boolean[] existsAll(List<Get> listOfGets) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColAliasesEnabled()) {
      // convert Gets before invoking "native" method
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyQualifierToAliasMap
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, listOfGets);
      if (!familyQualifierToAliasMap.isEmpty()) {
        List<Get> convertedGets = new LinkedList<>();
        for (Get originalGet : listOfGets) {
          convertedGets.add(repository.convertQualifiersToAliases(
                  mTableDescriptor, originalGet, familyQualifierToAliasMap));
        }
        // invoke "native" HBase method with aliased Gets
        return wrappedTable.existsAll(convertedGets);
      }
    }
    return wrappedTable.existsAll(listOfGets);
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
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, actions, 0);
      List<Row> convertedRows = new LinkedList<>();
      for (Row row : actions) {
        if (Mutation.class.isAssignableFrom(row.getClass())) {
          convertedRows.add(repository.convertQualifiersToAliases(
                  mTableDescriptor, row, familyQualifierToAliasMap, 0));
        }
      }
      int listIndex = 0;
      for (Row row : actions) {
        if (Get.class.isAssignableFrom(row.getClass())) {
          convertedRows.add(
                  listIndex, repository.convertQualifiersToAliases(
                          mTableDescriptor, (Get)row, familyQualifierToAliasMap));
        }
        listIndex++;
      }
      // invoke "native" method
      if (includeCallback) {
        wrappedTable.batchCallback(convertedRows, results, callback);
      } else {
        wrappedTable.batch(convertedRows, results);
      }
      // convert Result objects
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap
              = repository.getFamilyAliasToQualifierMap(familyQualifierToAliasMap);
      Object[] convertedResults = new Object[results.length];
      int objectIndex = 0;
      for (Object returnedObject : results) {
        if (Result.class.isAssignableFrom(returnedObject.getClass())) {
          convertedResults[objectIndex]
                  = repository.convertAliasesToQualifiers(
                          (Result)returnedObject, familyAliasToQualifierMap);
        } else {
          convertedResults[objectIndex] = results[objectIndex];
        }
        objectIndex++;
      }
      objectIndex = 0;
      for (Object convertedResult : convertedResults) {
        results[objectIndex++] = convertedResult;
      }
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
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, get);
      Result result = wrappedTable.get(repository.convertQualifiersToAliases(
              mTableDescriptor, get, familyQualifierToAliasMap));
      return repository.convertAliasesToQualifiers(result,
              repository.getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
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
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, listOfGets);
      if (!familyQualifierToAliasMap.isEmpty()) {
        List<Get> convertedGets = new LinkedList<>();
        for (Get originalGet : listOfGets) {
          convertedGets.add(repository.convertQualifiersToAliases(
                  mTableDescriptor, originalGet, familyQualifierToAliasMap));
        }

        // invoke "native" HBase method with aliased Gets
        Result[] originalResults = wrappedTable.get(convertedGets);

        // do alias-to-qualifier conversion of Results before returning them
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyAliasToQualifierMap
                = repository.getFamilyAliasToQualifierMap(familyQualifierToAliasMap);
        List<Result> convertedResults = new LinkedList<>();
        for (Result originalResult : originalResults) {
          convertedResults.add(
                  repository.convertAliasesToQualifiers(originalResult, familyAliasToQualifierMap));
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
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, scan);
      if (!familyQualifierToAliasMap.isEmpty()) {
        return new ResultScannerForAliasedFamilies(
                wrappedTable.getScanner(
                        repository.convertQualifiersToAliases(
                                mTableDescriptor, scan, familyQualifierToAliasMap)),
                repository.getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
      }
    }
    return wrappedTable.getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] colFamily) throws IOException {
    if (includedInRepositoryProcessing
            && mTableDescriptor.getMColumnDescriptor(colFamily).columnAliasesEnabled()) {
      return new ResultScannerForAliasedFamilies(
              wrappedTable.getScanner(colFamily),
              repository.getFamilyAliasToQualifierMap(mTableDescriptor, colFamily));
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
              repository.getFamilyAliasToQualifierMap(
                      mTableDescriptor, colFamily, colQualifier));
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
      wrappedTable.put(repository.convertQualifiersToAliases(mTableDescriptor, put));
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
      this.put(put);
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
              colValue, repository.convertQualifiersToAliases(mTableDescriptor, put));
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
              co, colValue, repository.convertQualifiersToAliases(mTableDescriptor, put));
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
      wrappedTable.delete(repository.convertQualifiersToAliases(mTableDescriptor, delete));
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
        convertedDeleteList.add(repository.convertQualifiersToAliases(mTableDescriptor, delete));
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
              colValue, repository.convertQualifiersToAliases(mTableDescriptor, delete));
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
              co, colValue, repository.convertQualifiersToAliases(mTableDescriptor, delete));
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
      wrappedTable.mutateRow(repository.convertQualifiersToAliases(mTableDescriptor, rm));
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
      familyQualifierToAliasMap = repository.getFamilyQualifierToAliasMap(mTableDescriptor, append);
      result = wrappedTable.append(repository.convertQualifiersToAliases(
              mTableDescriptor, append, familyQualifierToAliasMap));
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
      return repository.convertAliasesToQualifiers(result,
              repository.getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
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
      familyQualifierToAliasMap
              = repository.getFamilyQualifierToAliasMap(mTableDescriptor, increment);
      result = wrappedTable.increment(
              repository.convertQualifiersToAliases(
                      mTableDescriptor, increment, familyQualifierToAliasMap));
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
      return repository.convertAliasesToQualifiers(result,
              repository.getFamilyAliasToQualifierMap(familyQualifierToAliasMap));
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
              repository.convertQualifiersToAliases(mTableDescriptor, rm));
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
      return repository.convertAliasesToQualifiers(
              wrappedResultScanner.next(), familyAliasToQualifierMap);
    }

    @Override
    public Result[] next(int i) throws IOException {
      List<Result> convertedResultList = new LinkedList<>();
      for (Result originalResult : wrappedResultScanner.next(i)) {
        convertedResultList.add(repository.convertAliasesToQualifiers(
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
        return repository.convertAliasesToQualifiers(
                wrappedIterator.next(), familyAliasToQualifierMap);
      }
    }
  }
}
