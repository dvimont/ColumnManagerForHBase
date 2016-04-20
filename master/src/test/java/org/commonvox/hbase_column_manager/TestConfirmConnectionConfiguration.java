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

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

//@Category(MediumTests.class)
public class TestConfirmConnectionConfiguration {

  private static final String TEST_ENVIRONMENT_SETUP_ERROR =
          "TEST ENVIRONMENT APPARENTLY NOT SET UP CORRECTLY!! ==>>  ";
  private static final String CONNECTION_CONFIGURATION_MALFUNCTION =
          "MALFUNCTION IN CONNECTION/CONFIGURATION!! ==>> ";

  @Test
  public void testConnection() throws Exception {
    assertTrue(TEST_ENVIRONMENT_SETUP_ERROR + "Required config file not found in "
            + "classpath: " + MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE,
            ClasspathSearcher.fileFoundOnClassPath(MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE));

    try (Connection connection = MConnectionFactory.createConnection();
            Admin admin = connection.getAdmin()) {

      admin.getClusterStatus(); // assure valid connection established (otherwise throws Exception)

      // Assure that every property in the COLUMN_MANAGER_CONFIG_FILE
      //  appears as expected in the connection's configuration.
      Configuration sessionConf = connection.getConfiguration();
      Configuration colManagerConf = new Configuration();
      colManagerConf.addResource(MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE);
      Iterator<Entry<String,String>> parmIterator = colManagerConf.iterator();
      while (parmIterator.hasNext()) {
        Entry<String,String> colManagerParameter = parmIterator.next();
        if (colManagerParameter.getKey().startsWith(Repository.HBASE_CONFIG_PARM_KEY_PREFIX)) {
          String sessionSetting = sessionConf.get(colManagerParameter.getKey());
          assertEquals(CONNECTION_CONFIGURATION_MALFUNCTION + colManagerParameter.getKey() +
                  " parameter not found with expected value in connection configuration.",
                  colManagerParameter.getValue(), sessionSetting);
        }
      }
    }
    catch (Exception e) {
      fail("Exception thrown by MConnectionFactory#createConnection: " + e.getMessage());
    }
    System.out.println("Simple HBase connection test via MConnectionFactory completed successfully.");
  }

  public static void main(String[] args) throws Exception {
    new TestConfirmConnectionConfiguration().testConnection();
  }
}
