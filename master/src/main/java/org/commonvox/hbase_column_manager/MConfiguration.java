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
 *
 * @author Daniel Vimont
 */
public class MConfiguration {

  /**
   * Creates a {@link Configuration} with HBase and ColumnManager resources.
   *
   * @return A {@link Configuration} with HBase and ColumnManager resources.
   */
  public static Configuration create() {
    Configuration conf = HBaseConfiguration.create(); // get standard HBase configuration
    conf.addResource(MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE); // column-manager conf
    return conf;
  }

  /**
   * Creates a {@link Configuration} with HBase resources, the submitted {@link Configuration}'s
   * resources, and ColumnManager resources.
   *
   * @param addedConfiguration Configuration object with resources to be added.
   * @return A {@link Configuration} with HBase resources, the submitted {@link Configuration}'s
   * resources, and ColumnManager resources.
   */
  public static Configuration create(Configuration addedConfiguration) {
    Configuration conf = HBaseConfiguration.create(addedConfiguration);
    conf.addResource(MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE); // column-manager conf
    return conf;
  }
}
