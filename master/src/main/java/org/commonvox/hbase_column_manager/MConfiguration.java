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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Provides {@link org.apache.hadoop.conf.Configuration}s with properties from classpath-accessible
 * HBase and ColumnManager ({@code hbase-column-manager.xml}) configuration files.
 *
 * @author Daniel Vimont
 */
public class MConfiguration {

  private MConfiguration() { // no public constructor; use static #create methods
  }

  /**
   * Creates a {@link org.apache.hadoop.conf.Configuration} with HBase and ColumnManager resources.
   *
   * @return A {@link org.apache.hadoop.conf.Configuration} with HBase and ColumnManager resources.
   */
  public static Configuration create() {
    Configuration conf = HBaseConfiguration.create(); // get standard HBase configuration
    conf.addResource(MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE); // column-manager conf
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.reduce.speculative", false);
    return conf;
  }

  /**
   * Creates a {@link org.apache.hadoop.conf.Configuration} with HBase resources, the submitted
   * {@link org.apache.hadoop.conf.Configuration}'s resources, and ColumnManager resources.
   *
   * @param addedConfiguration Configuration object with resources to be added.
   * @return A {@link org.apache.hadoop.conf.Configuration} with HBase resources, the submitted
   * {@link org.apache.hadoop.conf.Configuration}'s resources, and ColumnManager resources.
   */
  public static Configuration create(Configuration addedConfiguration) {
    Configuration conf = create();
    conf.addResource(addedConfiguration);
    return conf;
  }
}
