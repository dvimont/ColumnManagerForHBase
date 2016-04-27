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
import java.util.List;
import java.util.Map;
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

/**
 *
 * @author Daniel Vimont
 */
class MTable implements Table {

  private final Table wrappedTable;
  private final Repository repository;
  private final MTableDescriptor mTableDescriptor;
  private final boolean includeInRepositoryProcessing;

  MTable(Table userTable, Repository repository)
          throws IOException {
    wrappedTable = userTable;

    this.repository = repository;
    if (this.repository.isActivated()) {
      mTableDescriptor
              = this.repository.getMTableDescriptor(wrappedTable.getTableDescriptor().getTableName());
      if (mTableDescriptor == null) {
        includeInRepositoryProcessing = false;
      } else {
        includeInRepositoryProcessing = true;
      }
    } else {
      mTableDescriptor = null;
      includeInRepositoryProcessing = false;
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
    if (repository.isActivated()
            && includeInRepositoryProcessing
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
    if (repository.isActivated() && includeInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditors(mTableDescriptor, (Mutation) action);
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
    if (repository.isActivated()
            && includeInRepositoryProcessing
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
    if (repository.isActivated() && includeInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditors(mTableDescriptor, (Mutation) action);
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
    if (repository.isActivated()
            && includeInRepositoryProcessing
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
    if (repository.isActivated() && includeInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditors(mTableDescriptor, (Mutation) action);
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
    if (repository.isActivated()
            && includeInRepositoryProcessing
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
    if (repository.isActivated() && includeInRepositoryProcessing) {
      int rowCount = 0;
      for (Object actionSucceeded : os) {
        if (actionSucceeded != null) {
          Row action = list.get(rowCount);
          if (Mutation.class.isAssignableFrom(action.getClass())) {
            repository.putColumnAuditors(mTableDescriptor, (Mutation) action);
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
    if (repository.isActivated()
            && includeInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing
    wrappedTable.put(put);
    // ColumnManager auditing
    if (repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, put);
    }
  }

  @Override
  public void put(List<Put> list) throws IOException {
    for (Put put : list) {
      this.put(put); // v1.0.1 fix replaced: wrappedTable.put(put);
    }
  }

  @Override
  public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Put put) throws IOException {
    // ColumnManager validation
    if (repository.isActivated()
            && includeInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing
    boolean putPerformed = wrappedTable.checkAndPut(bytes, bytes1, bytes2, bytes3, put);
    // ColumnManager auditing
    if (putPerformed && repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, put);
    }
    return putPerformed;
  }

  @Override
  public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp co, byte[] bytes3, Put put) throws IOException {
    // ColumnManager validation
    if (repository.isActivated()
            && includeInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, put);
    }
    // Standard HBase processing
    boolean putPerformed = wrappedTable.checkAndPut(bytes, bytes1, bytes2, co, bytes3, put);
    // ColumnManager auditing
    if (putPerformed && repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, put);
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
  public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Delete delete) throws IOException {
    return wrappedTable.checkAndDelete(bytes, bytes1, bytes2, bytes3, delete);
  }

  @Override
  public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp co, byte[] bytes3, Delete delete) throws IOException {
    return wrappedTable.checkAndDelete(bytes, bytes1, bytes2, co, bytes3, delete);
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    // ColumnManager validation
    if (repository.isActivated()
            && includeInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, rm);
    }
    // Standard HBase processing
    wrappedTable.mutateRow(rm);
    // ColumnManager auditing
    if (repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, rm);
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    // ColumnManager validation
    if (repository.isActivated()
            && includeInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, append);
    }
    // Standard HBase processing
    Result result = wrappedTable.append(append);
    // ColumnManager auditing
    if (repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, append);
    }
    return result;
  }

  @Override
  public Result increment(Increment i) throws IOException {
    // ColumnManager validation
    if (repository.isActivated()
            && includeInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, i);
    }
    // Standard HBase processing
    Result result = wrappedTable.increment(i);
    // ColumnManager auditing
    if (repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, i);
    }
    return result;
  }

  @Override
  public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l) throws IOException {
    // ColumnManager validation
    Increment increment = null;
    if (repository.isActivated() && includeInRepositoryProcessing) {
      increment = new Increment(bytes).addColumn(bytes1, bytes2, l);
      if (mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
        repository.validateColumns(mTableDescriptor, increment);
      }
    }
    // Standard HBase processing
    long returnedLong = wrappedTable.incrementColumnValue(bytes, bytes1, bytes2, l);
    // ColumnManager auditing
    if (repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, increment);
    }
    return returnedLong;
  }

  @Override
  public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability drblt) throws IOException {
    // ColumnManager validation
    Increment increment = null;
    if (repository.isActivated() && includeInRepositoryProcessing) {
      increment = new Increment(bytes).addColumn(bytes1, bytes2, l);
      if (mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
        repository.validateColumns(mTableDescriptor, increment);
      }
    }
    // Standard HBase processing
    long returnedLong = wrappedTable.incrementColumnValue(bytes, bytes1, bytes2, l, drblt);
    // ColumnManager auditing
    if (repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, increment);
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
  public <R extends Message> void batchCoprocessorService(MethodDescriptor md, Message msg, byte[] bytes, byte[] bytes1, R r, Batch.Callback<R> clbck) throws ServiceException, Throwable {
    wrappedTable.batchCoprocessorService(md, msg, bytes, bytes1, r, clbck);
  }

  @Override
  public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareOp co, byte[] bytes3, RowMutations rm) throws IOException {
    // ColumnManager validation
    if (repository.isActivated()
            && includeInRepositoryProcessing
            && mTableDescriptor.hasColDescriptorWithColDefinitionsEnforced()) {
      repository.validateColumns(mTableDescriptor, rm);
    }
    // Standard HBase processing
    boolean mutationsPerformed = wrappedTable.checkAndMutate(bytes, bytes1, bytes2, co, bytes3, rm);
    // ColumnManager auditing
    if (mutationsPerformed && repository.isActivated() && includeInRepositoryProcessing) {
      repository.putColumnAuditors(mTableDescriptor, rm);
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
}
