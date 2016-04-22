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

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Daniel Vimont
 */
public class TestRepositoryAdmin {

  private static final String REPOSITORY_ADMIN_FAILURE
          = "FAILURE IN " + RepositoryAdmin.class.getSimpleName() + " PROCESSING!! ==>> ";

  @Test
  public void testStaticMethods() throws Exception {
    // first, get *standard* HBase connection and admin
    Connection standardConnection = ConnectionFactory.createConnection();
    Admin standardAdmin = standardConnection.getAdmin();

    // do "manual" cleanup to prepare for unit test
    TestMConnectionFactory.manuallyDropRepositoryStructures(standardConnection, standardAdmin);

    RepositoryAdmin.installRepositoryStructures(standardAdmin);
    assertTrue(REPOSITORY_ADMIN_FAILURE
            + "The following Repository NAMESPACE failed to be created upon "
            + "invocation of #installRepositoryStructures method: "
            + Repository.REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
            TestMConnectionFactory.namespaceExists(
                    standardAdmin, Repository.REPOSITORY_NAMESPACE_DESCRIPTOR));
    assertTrue(REPOSITORY_ADMIN_FAILURE
            + "The following Repository TABLE failed to be created upon "
            + "invocation of #installRepositoryStructures method: "
            + Repository.REPOSITORY_TABLENAME.getNameAsString(),
            standardAdmin.tableExists(Repository.REPOSITORY_TABLENAME));

    assertEquals(REPOSITORY_ADMIN_FAILURE
            + "Incorrect default value for Repository maxVersions returned by "
            + "#getRepositoryMaxVersions method.",
            Repository.REPOSITORY_DEFAULT_MAX_VERSIONS,
            RepositoryAdmin.getRepositoryMaxVersions(standardAdmin));

    final int NEW_MAX_VERSIONS = 160;
    RepositoryAdmin.setRepositoryMaxVersions(standardAdmin, NEW_MAX_VERSIONS);
    assertEquals(REPOSITORY_ADMIN_FAILURE
            + "Incorrect value for Repository maxVersions returned by "
            + "#getRepositoryMaxVersions method following invocation of #setRepositoryMaxVersions "
            + "method.",
            NEW_MAX_VERSIONS,
            RepositoryAdmin.getRepositoryMaxVersions(standardAdmin));


    RepositoryAdmin.uninstallRepositoryStructures(standardAdmin);
    assertTrue(REPOSITORY_ADMIN_FAILURE
            + "The following Repository NAMESPACE failed to be dropped upon "
            + "invocation of #uninstallRepositoryStructures method: "
            + Repository.REPOSITORY_NAMESPACE_DESCRIPTOR.getName(),
            !TestMConnectionFactory.namespaceExists(
                    standardAdmin, Repository.REPOSITORY_NAMESPACE_DESCRIPTOR));
    assertTrue(REPOSITORY_ADMIN_FAILURE
            + "The following Repository TABLE failed to be dropped upon "
            + "invocation of #uninstallRepositoryStructures method: "
            + Repository.REPOSITORY_TABLENAME.getNameAsString(),
            !standardAdmin.tableExists(Repository.REPOSITORY_TABLENAME));

  }
}
