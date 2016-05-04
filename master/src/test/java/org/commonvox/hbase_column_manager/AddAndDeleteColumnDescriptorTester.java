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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Daniel Vimont
 */
public class AddAndDeleteColumnDescriptorTester {
    static final String MY_NAMESPACE_NAME = "myNamespace";
    static final TableName MY_TABLE_NAME = TableName.valueOf(MY_NAMESPACE_NAME, "myTable");
    static final byte[] MY_COLUMN_FAMILY_NAME = Bytes.toBytes("myColumnFamily");
    static final byte[] MY_COLUMN_FAMILY2_NAME = Bytes.toBytes("myColumnFamily2");
    static final byte[] MY_FIRST_COLUMN_QUALIFIER = Bytes.toBytes("myFirstColumn");
    static final byte[] MY_SECOND_COLUMN_QUALIFIER = Bytes.toBytes("mySecondColumn");
    static byte[] rowId1 = Bytes.toBytes("rowId01");
    static byte[] rowId2 = Bytes.toBytes("rowId02");

    public static void main(String[] args) throws Exception {
        final String TARGET_PATH = "/home/dv/Documents";

        /** HBaseConfiguration#create automatically looks for hbase-site.xml
         * (i.e., the HBase startup parameters) on the system's CLASSPATH, to
         * enable the creation of connections to Zookeeper (i.e., the directory
         * to HBase resources) & HBase. */
        Configuration config = HBaseConfiguration.create();
        try ( Connection connection
                    = MConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin();
                RepositoryAdmin repositoryAdmin = new RepositoryAdmin(connection)
            )
        {
            System.out.println("*** Hello HBase! -- Connection has been established!!\n");
            NamespaceDescriptor nd = NamespaceDescriptor.create(MY_NAMESPACE_NAME).build();
            nd.setConfiguration("WidgetConfig", "WidgetsAplenty");
            if (!repositoryAdmin.namespaceExists(MY_NAMESPACE_NAME)) {
                admin.createNamespace(nd);
            }
            // Create a Table with a Column Family; invoke #setMaxVersions(3) so
            //   that each Column in the family will retain its 2 most recent past
            //   values (i.e., timestamped versions) in addition to its current value.
            HTableDescriptor htd
                    = new HTableDescriptor(MY_TABLE_NAME)
                            .setMemStoreFlushSize(60000000)
                            .setOwnerString("OwnerSmith")
                            .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY_NAME)
                                                    .setMaxVersions(3));
            if (!admin.tableExists(MY_TABLE_NAME)) {
                admin.createTable(htd);
            }

            // THIS IS THE FIRST OBJECT OF THIS TEST -- ADD COLUMN DESCRIPTOR
            admin.addColumn(MY_TABLE_NAME, new HColumnDescriptor(MY_COLUMN_FAMILY2_NAME));

            try (Table table = connection.getTable(MY_TABLE_NAME))
            {
                // <put> (insert) a row into the Table: specify the row's unique ID,
                //   and add two Columns with values "Hello" and " World!".
                table.put(new Put(rowId1).
                                addColumn(MY_COLUMN_FAMILY_NAME, MY_FIRST_COLUMN_QUALIFIER,
                                                            Bytes.toBytes("Hello")).
                                addColumn(MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER,
                                                            Bytes.toBytes(" World!")) );

                // Now 'update' the second Column of the same row with another <put>,
                //   which stores a second 'version' of that column (retaining the first).
                table.put(new Put(rowId1).
                            addColumn(MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER,
                                                        Bytes.toBytes(" BIG DATA World!")) );
            }

//            System.out.println("DUMPING REPOSITORY *BEFORE* delete of COLUMN DESCRIPTOR");
//            mAdmin.dumpRepositoryTable();

            System.out.println("\nPrinting table structures BEFORE colFamily delete\n");
            getAndPrintTableStructuresInHBase(admin, MY_TABLE_NAME);

            // THE SECOND OBJECT OF THIS TEST -- DELETE COLUMN DESCRIPTOR
//            admin.deleteColumn(MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME);

            // Alternate mode of column deletion: use modifyTable
            admin.modifyTable(MY_TABLE_NAME,
                                new HTableDescriptor(MY_TABLE_NAME)
                                        .setMemStoreFlushSize(80000000)
                                        .setOwnerString("OwnerJones")
                                        .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY2_NAME)
                                                                .setMaxVersions(3))
            );

            System.out.println("\nPrinting table structures AFTER colFamily delete\n");
            getAndPrintTableStructuresInHBase(admin, MY_TABLE_NAME);

            System.out.println("DUMPING REPOSITORY *AFTER* delete of COLUMN DESCRIPTOR");
            repositoryAdmin.dumpRepositoryTable();

            // THE THIRD OBJECT OF THIS TEST -- RE-ADD THE COLUMN DESCRIPTOR (without its original attributes)
            admin.addColumn(MY_TABLE_NAME, new HColumnDescriptor(MY_COLUMN_FAMILY_NAME));
            admin.modifyColumn(MY_TABLE_NAME, new HColumnDescriptor(MY_COLUMN_FAMILY2_NAME)
                                                                .setMaxVersions(17));


            System.out.println("DUMPING REPOSITORY *AFTER* re-add of one COLUMN DESCRIPTOR "
                    + "and modification of the other.");
            repositoryAdmin.dumpRepositoryTable();

            deleteUserObjects(admin, repositoryAdmin);
            RepositoryAdmin.uninstallRepositoryStructures(admin);
        }

    }

    static void getAndPrintTableStructuresInHBase (Admin admin, TableName tableName)
            throws TableNotFoundException, IOException {
        HTableDescriptor htd = admin.getTableDescriptor(tableName);
        System.out.println("Table Name: " + htd.getNameAsString());
        System.out.println("  Column Families:");
        for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
            System.out.println("    " + hcd.getNameAsString());
        }
    }

    static void deleteUserObjects(Admin admin, RepositoryAdmin repositoryAdmin) throws IOException {
        if (admin.tableExists(MY_TABLE_NAME)) {
            System.out.println("Disabling/deleting " + MY_TABLE_NAME.getNameAsString());
            admin.disableTable(MY_TABLE_NAME); // Must disable before deleting.
            admin.deleteTable(MY_TABLE_NAME); // Deleting the Table makes this code rerunnable.
        }
        if (repositoryAdmin.namespaceExists(MY_NAMESPACE_NAME)) {
            System.out.println("Deleting " + MY_NAMESPACE_NAME);
            admin.deleteNamespace(MY_NAMESPACE_NAME); // Deleting the Namespace makes this code rerunnable.
        }
    }

}
