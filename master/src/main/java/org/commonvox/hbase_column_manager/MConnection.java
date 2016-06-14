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
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;

/**
 * This class serves as a wrapper for a standard HBase Connection, offering all standard Connection
 * functionality, while in the background assuring proper connectivity with the ColumnManager
 * repository. The Admin and Table objects it returns are {@link MAdmin} and {@link MTable} objects,
 * respectively. It also privately contains a {@link Repository} instance, which it passes to all
 * {@link MAdmin} and {@link MTable} instances it generates.
 *
 * @author Daniel Vimont
 */
class MConnection implements Connection, Closeable, AutoCloseable {

  private final Connection STANDARD_HBASE_CONNECTION;
  private final Repository REPOSITORY;

  MConnection(Connection hBaseClientConnection) throws IOException {
    STANDARD_HBASE_CONNECTION = hBaseClientConnection;
    REPOSITORY = new Repository(STANDARD_HBASE_CONNECTION, this);
  }

  Repository getRepository() {
    return REPOSITORY;
  }

  Connection getStandardConnection() {
    return STANDARD_HBASE_CONNECTION;
  }

  /* Start of ColumnManager-related overrides */
  @Override
  public Admin getAdmin() throws IOException {
    return new MAdmin(STANDARD_HBASE_CONNECTION.getAdmin(), REPOSITORY);
  }

  @Override
  public Table getTable(TableName tn) throws IOException {
    return new MTable(STANDARD_HBASE_CONNECTION.getTable(tn), REPOSITORY);
  }

  @Override
  public Table getTable(TableName tn, ExecutorService es) throws IOException {
    return new MTable(STANDARD_HBASE_CONNECTION.getTable(tn, es), REPOSITORY);
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tn) throws IOException {
    return new MBufferedMutator(STANDARD_HBASE_CONNECTION.getBufferedMutator(tn), REPOSITORY);
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bmp) throws IOException {
    return new MBufferedMutator(STANDARD_HBASE_CONNECTION.getBufferedMutator(bmp), REPOSITORY);
  }

  /* Start of overrides which simply pass on standard Connection functionality */
  @Override
  public Configuration getConfiguration() {
    return STANDARD_HBASE_CONNECTION.getConfiguration();
  }

  @Override
  public RegionLocator getRegionLocator(TableName tn) throws IOException {
    return STANDARD_HBASE_CONNECTION.getRegionLocator(tn);
  }

  @Override
  public void close() throws IOException {
    STANDARD_HBASE_CONNECTION.close();
  }

  @Override
  public boolean isClosed() {
    return STANDARD_HBASE_CONNECTION.isClosed();
  }

  @Override
  public void abort(String string, Throwable thrwbl) {
    STANDARD_HBASE_CONNECTION.abort(string, thrwbl);
  }

  @Override
  public boolean isAborted() {
    return STANDARD_HBASE_CONNECTION.isAborted();
  }

  @Override
  public boolean equals(Object otherObject) {
    return STANDARD_HBASE_CONNECTION.equals(otherObject);
  }

  @Override
  public int hashCode() {
    return STANDARD_HBASE_CONNECTION.hashCode();
  }
}
