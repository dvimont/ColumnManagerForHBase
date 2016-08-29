/*
 * Copyright (C) 2016 Daniel Vimont
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
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Daniel Vimont
 */
public class TestMConnectionFactory {

  private static final String M_CONNECTION_FACTORY_FAILURE
          = "FAILURE IN " + MConnectionFactory.class.getSimpleName() + " PROCESSING!! ==>> ";

  @Test
  public void testCreateConnection() throws Exception {
    // first, get *standard* HBase connection and admin
    Connection standardConnection = ConnectionFactory.createConnection();
    Admin standardAdmin = standardConnection.getAdmin();

    // do "manual" cleanup to prepare for unit test
    manuallyDropRepositoryStructures(standardConnection, standardAdmin);

    Connection mConnection = MConnectionFactory.createConnection();
    assertTrue(M_CONNECTION_FACTORY_FAILURE
            + "The following Repository NAMESPACE failed to be automatically created upon "
            + "invocation of #createConnection method: "
            + Repository.REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
            namespaceExists(standardAdmin, Repository.REPOSITORY_NAMESPACE_DESCRIPTOR));
    assertTrue(M_CONNECTION_FACTORY_FAILURE
            + "The following Repository TABLE failed to be automatically created upon "
            + "invocation of #createConnection method: "
            + Repository.REPOSITORY_TABLENAME.getNameAsString(),
            standardAdmin.tableExists(Repository.REPOSITORY_TABLENAME));

    manuallyDropRepositoryStructures(standardConnection, standardAdmin);
  }

  static void manuallyDropRepositoryStructures(
          Connection standardConnection, Admin standardAdmin) throws IOException {

    if (standardAdmin.tableExists(Repository.REPOSITORY_TABLENAME)) {
      try (Table existingRepositoryTable
              = standardConnection.getTable(Repository.REPOSITORY_TABLENAME)) {
        standardAdmin.disableTable(Repository.REPOSITORY_TABLENAME);
        standardAdmin.deleteTable(Repository.REPOSITORY_TABLENAME);
      }
      assertTrue(ClasspathSearcher.TEST_ENVIRONMENT_SETUP_ERROR
              + "The following Repository table could not be manually dropped: "
              + Repository.REPOSITORY_TABLENAME.getNameAsString(),
              !standardAdmin.tableExists(Repository.REPOSITORY_TABLENAME));
    }
    if (standardAdmin.tableExists(Repository.ALIAS_DIRECTORY_TABLENAME)) {
      try (Table existingRepositoryTable
              = standardConnection.getTable(Repository.ALIAS_DIRECTORY_TABLENAME)) {
        standardAdmin.disableTable(Repository.ALIAS_DIRECTORY_TABLENAME);
        standardAdmin.deleteTable(Repository.ALIAS_DIRECTORY_TABLENAME);
      }
      assertTrue(ClasspathSearcher.TEST_ENVIRONMENT_SETUP_ERROR
              + "The following Repository table could not be manually dropped: "
              + Repository.ALIAS_DIRECTORY_TABLENAME.getNameAsString(),
              !standardAdmin.tableExists(Repository.ALIAS_DIRECTORY_TABLENAME));
    }
    if (namespaceExists(standardAdmin, Repository.REPOSITORY_NAMESPACE_DESCRIPTOR)) {
      standardAdmin.deleteNamespace(Repository.REPOSITORY_NAMESPACE_DESCRIPTOR.getName());
      assertTrue(ClasspathSearcher.TEST_ENVIRONMENT_SETUP_ERROR
              + "The following Repository namespace could not be manually dropped: "
              + Repository.REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
              !namespaceExists(standardAdmin, Repository.REPOSITORY_NAMESPACE_DESCRIPTOR));
    }
  }

  static boolean namespaceExists(Admin hbaseAdmin, NamespaceDescriptor nd)
          throws IOException {
    try {
      hbaseAdmin.getNamespaceDescriptor(nd.getName());
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    new TestMConnectionFactory().testCreateConnection();
  }
}
