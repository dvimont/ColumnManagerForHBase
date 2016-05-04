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
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

/**
 * The <b>MConnectionFactory</b> provides exactly the same static methods as those provided by the
 * standard HBase {@code ConnectionFactory}, but a {@code Connection} object
 * {@link #createConnection() created} by the {@code MConnectionFactory} generates special
 * {@code Admin}, {@code Table}, and {@code BufferedMutator} objects which, in addition to providing
 * all standard HBase API functionality, also: (a) transparently interface with the ColumnManager
 * repository for tracking of <i>Namespace</i>,
 * <i>Table</i>, <i>Column Family</i>, and <i>{@link ColumnAuditor}</i> metadata, and (b)
 * {@link RepositoryAdmin#setColumnDefinitionsEnforced(boolean, org.apache.hadoop.hbase.TableName, byte[])
 * optionally enforce} administrator-specified {@link ColumnDefinition}s when <i>Columns</i> are
 * submitted in a {@code put} (i.e., insert/update) via the standard {@code Table},
 * {@code BufferedMutator}, and
 * {@link RepositoryAdmin#createHTableMultiplexer(int)  HTableMultiplexer} interfaces.
 *
 * @author Daniel Vimont
 */
public class MConnectionFactory {

  static final String COLUMN_MANAGER_CONFIG_FILE = "hbase-column-manager.xml";

  // no public constructor
  MConnectionFactory() {
  }

  /**
   * Create a new Connection instance using default HBaseConfiguration.
   *
   * @return Connection object
   * @throws IOException if a remote or network exception occurs
   */
  public static Connection createConnection() throws IOException {
    Configuration conf = HBaseConfiguration.create(); // get standard HBase configuration
    conf.addResource(COLUMN_MANAGER_CONFIG_FILE); // add column-manager configuration
    return new MConnection(ConnectionFactory.createConnection(conf));
  }

  /**
   * Create a new Connection instance using the passed Configuration instance instead of the default
   * HBaseConfiguration.
   *
   * @param conf Configuration
   * @return Connection object
   * @throws IOException if a remote or network exception occurs
   */
  public static Connection createConnection(Configuration conf) throws IOException {
    return new MConnection(ConnectionFactory.createConnection(conf));
  }

  /**
   * Create a new Connection instance using the passed Configuration instance instead of the default
   * HBaseConfiguration, and using the passed thread pool for batch operations.
   *
   * @param conf Configuration
   * @param pool The thread pool to use for batch operations
   * @return Connection object
   * @throws IOException if a remote or network exception occurs
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool)
          throws IOException {
    return new MConnection(ConnectionFactory.createConnection(conf, pool));
  }

  /**
   * Create a new Connection instance using the passed Configuration instance instead of the default
   * HBaseConfiguration, and using the passed thread pool for batch operations.
   *
   * @param conf Configuration
   * @param pool The thread pool to use for batch operations
   * @param user The user the connection is for
   * @return Connection object
   * @throws IOException - if a remote or network exception occurs
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool, User user)
          throws IOException {
    return new MConnection(ConnectionFactory.createConnection(conf, pool, user));
  }

  /**
   * Create a new Connection instance using the passed Configuration instance instead of the default
   * HBaseConfiguration.
   *
   * @param conf Configuration
   * @param user The user the connection is for
   * @return Connection object
   * @throws IOException - if a remote or network exception occurs
   */
  public static Connection createConnection(Configuration conf, User user)
          throws IOException {
    return new MConnection(ConnectionFactory.createConnection(conf, user));
  }
}
