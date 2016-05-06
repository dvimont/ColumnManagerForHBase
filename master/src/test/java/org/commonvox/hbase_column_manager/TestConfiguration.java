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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

public class TestConfiguration {

  private static final String CONFIGURATION_FAILURE = "FAILURE IN CONFIGURATION PROCESSING!! ==>> ";
  private static final String STANDARD_HBASE_CONFIG = "hbase-site.xml";
  private static final String VALID_TABLENAME01 = "validNamespace:validTableQualifier";
  private static final String VALID_TABLENAME02 = "validTableInDefaultNamespace";
  private static final String VALID_TABLENAME03 = "anotherValidNamespace:*";
  private static final String INVALID_TABLENAME01
          = "yetAnotherValidNamespace:invalidTableWithWildcard*";
  private static final String INVALID_TABLENAME02
          = "invalidNamespaceWithWildcard*:validTableQualifier";

  @Test
  public void testConfigurationLoading() throws Exception {
    ClasspathSearcher.confirmFileInClasspath("config", MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE);

    try (Connection mConnection = MConnectionFactory.createConnection();
            Admin mAdmin = mConnection.getAdmin()) {

      mAdmin.getClusterStatus(); // assure valid mConnection established (otherwise throws Exception)

      // Assure that every property in the COLUMN_MANAGER_CONFIG_FILE
      //  appears as expected in the mConnection's configuration.
      Configuration sessionConf = mConnection.getConfiguration();
      Configuration colManagerConf = new Configuration();
      colManagerConf.addResource(MConnectionFactory.COLUMN_MANAGER_CONFIG_FILE);

      Iterator<Entry<String, String>> parmIterator = colManagerConf.iterator();
      while (parmIterator.hasNext()) {
        Entry<String, String> colManagerParameter = parmIterator.next();
        if (colManagerParameter.getKey().startsWith(Repository.HBASE_CONFIG_PARM_KEY_PREFIX)) {
          assertEquals(CONFIGURATION_FAILURE + colManagerParameter.getKey()
                  + " parameter not found with expected value in connection configuration.",
                  colManagerParameter.getValue(), sessionConf.get(colManagerParameter.getKey()));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception thrown by MConnectionFactory#createConnection: " + e.getMessage());
    }
  }

  @Test
  public void testValidIncludeConfiguration() throws Exception {

    ClasspathSearcher.confirmFileInClasspath(STANDARD_HBASE_CONFIG, STANDARD_HBASE_CONFIG);

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES,
            VALID_TABLENAME01, VALID_TABLENAME02, VALID_TABLENAME03);
    try (Connection mConnection = MConnectionFactory.createConnection(configuration)) {
    } catch (IllegalArgumentException e) {
      fail("Valid configuration parameters resulted in IllegalArgumentException being thrown.");
    }
  }

  @Test
  public void testValidExcludeConfiguration() throws Exception {

    ClasspathSearcher.confirmFileInClasspath(STANDARD_HBASE_CONFIG, STANDARD_HBASE_CONFIG);

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES,
            VALID_TABLENAME01, VALID_TABLENAME02, VALID_TABLENAME03);
    try (Connection mConnection = MConnectionFactory.createConnection(configuration)) {
    } catch (IllegalArgumentException e) {
      fail("Valid configuration parameters resulted in IllegalArgumentException being thrown.");
    }
  }

  @Test
  public void testInvalidIncludeConfiguration01() throws Exception {

    ClasspathSearcher.confirmFileInClasspath(STANDARD_HBASE_CONFIG, STANDARD_HBASE_CONFIG);

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES,
            VALID_TABLENAME01, VALID_TABLENAME02, VALID_TABLENAME03, INVALID_TABLENAME01);
    try (Connection mConnection = MConnectionFactory.createConnection(configuration)) {
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("Invalid configuration parameter <" + INVALID_TABLENAME01
            + "> failed to result in IllegalArgumentException being thrown.");
  }

  @Test
  public void testInvalidIncludeConfiguration02() throws Exception {

    ClasspathSearcher.confirmFileInClasspath(STANDARD_HBASE_CONFIG, STANDARD_HBASE_CONFIG);

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_INCLUDED_TABLES,
            VALID_TABLENAME01, VALID_TABLENAME02, VALID_TABLENAME03, INVALID_TABLENAME02);
    try (Connection mConnection = MConnectionFactory.createConnection(configuration)) {
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("Invalid configuration parameter <" + INVALID_TABLENAME02
            + "> failed to result in IllegalArgumentException being thrown.");
  }

  @Test
  public void testInvalidExcludeConfiguration01() throws Exception {

    ClasspathSearcher.confirmFileInClasspath(STANDARD_HBASE_CONFIG, STANDARD_HBASE_CONFIG);

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES,
            VALID_TABLENAME01, VALID_TABLENAME02, VALID_TABLENAME03, INVALID_TABLENAME01);
    try (Connection mConnection = MConnectionFactory.createConnection(configuration)) {
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("Invalid configuration parameter <" + INVALID_TABLENAME01
            + "> failed to result in IllegalArgumentException being thrown.");
  }

  @Test
  public void testInvalidExcludeConfiguration02() throws Exception {

    ClasspathSearcher.confirmFileInClasspath(STANDARD_HBASE_CONFIG, STANDARD_HBASE_CONFIG);

    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);
    configuration.setStrings(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_EXCLUDED_TABLES,
            VALID_TABLENAME01, VALID_TABLENAME02, VALID_TABLENAME03, INVALID_TABLENAME02);
    try (Connection mConnection = MConnectionFactory.createConnection(configuration)) {
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("Invalid configuration parameter <" + INVALID_TABLENAME02
            + "> failed to result in IllegalArgumentException being thrown.");
  }

  public static void main(String[] args) throws Exception {
    new TestConfiguration().testConfigurationLoading();
    new TestConfiguration().testValidIncludeConfiguration();
    new TestConfiguration().testValidExcludeConfiguration();
    new TestConfiguration().testInvalidIncludeConfiguration01();
    new TestConfiguration().testInvalidIncludeConfiguration02();
    new TestConfiguration().testInvalidExcludeConfiguration01();
    new TestConfiguration().testInvalidExcludeConfiguration02();
  }
}
