/*
 * Copyright 2016 Daniel Vimont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonvox.hbase_column_manager;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * The {@code MHBaseTestingUtility} (which is only functional with HBase 2.0 or later)
 * provides a ColumnManager-enabled extension to the
 * <a href="http://hbase.apache.org/testapidocs/org/apache/hadoop/hbase/HBaseTestingUtility.html?is-external=true">
 * HBaseTestingUtility</a>.
 * <br><br>
 * <b>IMPORTANT NOTE: This class is only viable with the HBase 2.0 (or later) implementation of
 * ColumnManagerAPI;
 * in pre-2.0 implementations, this class will throw an UnsupportedOperationException.</b>
 *
 * @author Daniel Vimont
 */
public class MHBaseTestingUtility extends HBaseTestingUtility {

  /**
   * Shared cluster connection.
   */
  private volatile Connection connection = null;
  private Admin admin = null;

  public MHBaseTestingUtility() {
    this(HBaseConfiguration.create());
  }

  public MHBaseTestingUtility(Configuration conf) {
    super(conf);

    // prevent contention for ports if other hbase thread(s) already running
    if (conf != null) {
      conf.addResource(MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE); // add column-manager conf

      if (conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT)
              == HConstants.DEFAULT_MASTER_INFOPORT) {
        conf.setInt(HConstants.MASTER_INFO_PORT, -1);
        LOG.debug("Config property " + HConstants.MASTER_INFO_PORT + " changed to -1");
      }
      if (conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT)
              == HConstants.DEFAULT_REGIONSERVER_PORT) {
        conf.setInt(HConstants.REGIONSERVER_PORT, -1);
        LOG.debug("Config property " + HConstants.REGIONSERVER_PORT + " changed to -1");
      }
    }
  }

  /**
   * Returns a {@link org.apache.hadoop.hbase.client.Connection} created by
   * {@link MConnectionFactory}.
   *
   * @return a {@link org.apache.hadoop.hbase.client.Connection} created by
   * {@link MConnectionFactory}
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public Connection getConnection() throws IOException {
    if (this.connection == null) {
      this.connection = MConnectionFactory.createConnection(this.conf);
    }
    return this.connection;
  }

  /**
   * Note that this method is deprecated as of HBase 2.0, and the ColumnManagerAPI does NOT
   * support it. Its invocation will result in throwing of an UnsupportedOperationException.
   *
   * @return {@link org.apache.hadoop.hbase.client.HBaseAdmin} object; note that this method
   * is deprecated as of HBase 2.0
   * @throws UnsupportedOperationException in all cases within the ColumnManagerAPI framework
   */
  @Override
  // @Deprecated // deprecated in 2.0 and later
  public synchronized HBaseAdmin getHBaseAdmin() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
            "ColumnManagerAPI does not support this method, deprecated as of HBase 2.0.");
  }

  /**
   * Returns {@link org.apache.hadoop.hbase.client.Admin} object.
   *
   * @return {@link org.apache.hadoop.hbase.client.Admin} object
   * @throws IOException if a remote or network exception occurs
   */
  // @Override // override in 2.0 and later
  public synchronized Admin getAdmin() throws IOException {
    if (admin == null){
      this.admin = getConnection().getAdmin();
    }
    return this.admin;
  }
}
