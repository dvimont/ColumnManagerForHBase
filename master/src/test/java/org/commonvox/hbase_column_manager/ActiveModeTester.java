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
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author dv
 */
public class ActiveModeTester {
    static final String MY_NAMESPACE_NAME = "myNamespace";
    static final String MY_NAMESPACE2_NAME = "anotherNamespace";
    static final TableName MY_TABLE_NAME = TableName.valueOf(MY_NAMESPACE_NAME, "myTable");
    static final TableName MY_TABLE2_NAME = TableName.valueOf(MY_NAMESPACE_NAME, "myTable2");
    static final TableName MY_TABLE3_NAME = TableName.valueOf(MY_NAMESPACE2_NAME, "anotherTable");
    static final byte[] MY_COLUMN_FAMILY_NAME = Bytes.toBytes("myColumnFamily");
    static final byte[] MY_COLUMN_FAMILY2_NAME = Bytes.toBytes("myColumnFamily2");
    static final byte[] MY_FIRST_COLUMN_QUALIFIER = Bytes.toBytes("myFirstColumn");
    static final byte[] MY_SECOND_COLUMN_QUALIFIER = Bytes.toBytes("mySecondColumn");
    static final byte[] MY_INVALID_COLUMN_QUALIFIER = Bytes.toBytes("myThirdColumn");
    static byte[] rowId1 = Bytes.toBytes("rowId01");
    static byte[] rowId2 = Bytes.toBytes("rowId02");

    static String alternateUserName = "anotherGuy";

    public static void main(String[] args) throws Exception {
        final String TARGET_PATH = "/home/dv/Documents";
        Connection myConnection = ConnectionFactory.createConnection();

        // Creating repository Namespace and Table here as a separate step!!
        try (Admin standardAdmin = ConnectionFactory.createConnection().getAdmin()) {
            RepositoryAdmin.installRepositoryStructures(standardAdmin);
            RepositoryAdmin.setRepositoryMaxVersions(standardAdmin, 88);

        }

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

            // CREATE NAMESPACES
            NamespaceDescriptor nd = NamespaceDescriptor.create(MY_NAMESPACE_NAME).build();
            nd.setConfiguration("WidgetConfig", "WidgetsAplenty");
            if (!repositoryAdmin.namespaceExists(MY_NAMESPACE_NAME)) {
                admin.createNamespace(nd);
            }
            NamespaceDescriptor nd2 = NamespaceDescriptor.create(MY_NAMESPACE2_NAME).build();
            if (!repositoryAdmin.namespaceExists(MY_NAMESPACE2_NAME)) {
                admin.createNamespace(nd2);
            }

            // Create a Table with a Column Family; invoke #setMaxVersions(3) so
            //   that each ColumnAuditor in the family will retain its 2 most recent past
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
            if (!admin.tableExists(MY_TABLE2_NAME)) {
                admin.createTable(new HTableDescriptor(MY_TABLE2_NAME)
                                    .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY2_NAME)));
            }
            if (!admin.tableExists(MY_TABLE3_NAME)) {
                admin.createTable(new HTableDescriptor(MY_TABLE3_NAME)
                                    .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY_NAME)));
            }

//            System.out.println("DUMPING REPOSITORY AFTER initial store of tables");
//            mAdmin.dumpRepositoryTable();

            // ALTER TABLE (under different user.name)
            System.setProperty("user.name", alternateUserName);
            htd.setMemStoreFlushSize(70000000).setDurability(Durability.SKIP_WAL)
                    .setConfiguration("MyGreatConfigParm", "A_good_value");
            admin.modifyTable(MY_TABLE_NAME, htd);

            // ADD COLUMN DEFINITIONS FOR FIRST_COLUMN and SECOND_COLUMN (but NOT for INVALID_COLUMN!)
            ColumnDefinition firstDef
                    = new ColumnDefinition(MY_FIRST_COLUMN_QUALIFIER).setColumnLength(50L);
            repositoryAdmin.addColumnDefinition(MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME, firstDef);
            ColumnDefinition secondDef
                    = new ColumnDefinition(MY_SECOND_COLUMN_QUALIFIER)
                            .setColumnLength(100L).setColumnValidationRegex(".*World.*");
            repositoryAdmin.addColumnDefinition(MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME, secondDef);
            repositoryAdmin.setColumnDefinitionsEnforced(true, MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME);

//            repositoryAdmin.deleteColumnDefinition
//                                (MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER);

            try (Table table = connection.getTable(MY_TABLE_NAME))
            {
                // <put> (insert) a row into the Table: specify the row's unique ID,
                //   and add two Columns with values "Hello" and " World!".
                table.put(new Put(rowId1).
                                addColumn(MY_COLUMN_FAMILY_NAME, MY_FIRST_COLUMN_QUALIFIER,
                                                            Bytes.toBytes("Hello")).
                                addColumn(MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER,
                                                            Bytes.toBytes(" World!")) );
                printColDescriptors(admin.getTableDescriptor(MY_TABLE_NAME));
                getAndPrintColumnValues(table); // Yes, this will print "Hello World!"

                // Now 'update' the second ColumnAuditor of the same row with another <put>,
                //   which stores a second 'version' of that column (retaining the first).
                table.put(new Put(rowId1).
                            addColumn(MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER,
                                                        Bytes.toBytes(" BIG DATA World!")) );
                printColDescriptors(admin.getTableDescriptor(MY_TABLE_NAME));
                getAndPrintColumnValues(table);

                // Now 'update' a third INVALID ColumnAuditor of the same row with another <put>;
                //   this should throw ColumnDefinitionNotFoundException
                try {
                    table.put(new Put(rowId1).
                            addColumn(MY_COLUMN_FAMILY_NAME, MY_INVALID_COLUMN_QUALIFIER,
                                                        Bytes.toBytes(" I declare you invalid!!")) );
                }
                catch (ColumnManagerIOException e) {
                    System.out.println("Caught exception: " + e.toString());
                }
                printColDescriptors(admin.getTableDescriptor(MY_TABLE_NAME));
                getAndPrintColumnValues(table);

                getAndPrintAllCellVersions(table); // prints *both* versions of second ColumnAuditor


                // AUDIT TRAIL TEST
                ChangeEventMonitor auditTrail = repositoryAdmin.getChangeEventMonitor();
                ChangeEventMonitor.exportChangeEventListToCsvFile
                        (auditTrail.getAllChangeEvents(), TARGET_PATH, "changeEvents.csv");


            }


            HTableDescriptor mtd = admin.getTableDescriptor(MY_TABLE_NAME);
            System.out.println("Retrieved enhanced Table Descriptor for Table: "
                                    + MY_TABLE_NAME.getNameAsString());

            System.out.println("Table name: " + mtd.getNameAsString());
            System.out.println("       Table values...");
            for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry : mtd.getValues().entrySet()) {
                System.out.println("          " + Bytes.toString(valueEntry.getKey().copyBytes())
                        + " : " + Bytes.toString(valueEntry.getValue().copyBytes()) );
            }
            System.out.println("       Table configurations...");
            for (Map.Entry<String,String> configEntry : mtd.getConfiguration().entrySet()) {
                System.out.println("          " + configEntry.getKey() + " : " + configEntry.getValue());
            }
           for (HColumnDescriptor mcd : mtd.getFamilies()) {
                System.out.println("  Column Family: " + mcd.getNameAsString());
                for (ColumnAuditor mcq : repositoryAdmin.getColumnAuditors(mtd.getTableName(), mcd.getName())) {
                    System.out.println("    Column Qualifier: " + mcq.getColumnQualifierAsString()
                            + " ; Max value length: " + mcq.getMaxValueLengthFound());
                }
                for (ColumnDefinition mcqd
                        : repositoryAdmin.getColumnDefinitions(mtd.getTableName(), mcd.getName())) {
                    System.out.println("    Column DEFINITION for: " + mcqd.getColumnQualifierAsString());
                    System.out.println("      Column validation regex: " + mcqd.getColumnValidationRegex());
                    System.out.println("      Column defined length: " + mcqd.getColumnLength());
                }
            }
            System.out.println();

           // MSuperAdmin superAdmin = ((MConnection)connection).getSuperAdmin();
            repositoryAdmin.exportRepository
                    (TARGET_PATH, "testExportAll.xml", false);
            repositoryAdmin.exportNamespaceMetadata
                    (MY_NAMESPACE_NAME, TARGET_PATH, "testExportNamespace.xml", false);
            repositoryAdmin.exportTableMetadata
                    (TableName.valueOf(MY_NAMESPACE_NAME,"myTable"), TARGET_PATH, "testExportTable.xml", false);

//            System.out.println
//                (superAdmin.generateHsaFileSummary(TARGET_PATH, "testExportAll.xml"));

//            System.out.println("Testing get of MNamespace descriptor from Repository");
//            MNamespaceDescriptor testMnd
//                    = repositoryAdmin.getMNamespaceDescriptorFromRepository(MY_NAMESPACE_NAME);
//            System.out.println("Retrieved namespace = " + testMnd.getNameAsString());
//            System.out.println("  Namespace configuration values are:");
//            for (Entry<String,String> configEntry : testMnd.getConfiguration().entrySet()) {
//                System.out.println("     Key: " + configEntry.getKey() + "  Value: " + configEntry.getValue());
//            }

            System.out.println("DUMPING REPOSITORY *BEFORE* delete of user tables and namespace");
            repositoryAdmin.dumpRepositoryTable();

            deleteUserObjects(admin, repositoryAdmin);

//            System.out.println("DUMPING REPOSITORY *AFTER* delete of user tables and namespace");
//            mAdmin.dumpRepositoryTable();


            RepositoryAdmin.uninstallRepositoryStructures(admin);
        }

        try ( Connection connection
                    = MConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin();
                RepositoryAdmin repositoryAdmin = new RepositoryAdmin(connection)
                )
        {
            // NOW restore namespace & table from external archive (choose 1 of the 2 below)
//            repositoryAdmin.importMetadata(true, TARGET_PATH, "testExportAll.xml");
            repositoryAdmin.importTableMetadata(true, MY_TABLE_NAME, TARGET_PATH, "testExportAll.xml");


            System.out.println("DUMPING REPOSITORY *AFTER* restore from ARCHIVE of user namespace & table");
            repositoryAdmin.dumpRepositoryTable();

            deleteUserObjects(admin, repositoryAdmin); // delete user namespace and tables again
            RepositoryAdmin.uninstallRepositoryStructures(admin);
        }


    }

    static void deleteUserObjects(Admin admin, RepositoryAdmin repositoryAdmin) throws IOException {
        if (admin.tableExists(MY_TABLE_NAME)) {
            System.out.println("Disabling/deleting " + MY_TABLE_NAME.getNameAsString());
            admin.disableTable(MY_TABLE_NAME); // Must disable before deleting.
            admin.deleteTable(MY_TABLE_NAME); // Deleting the Table makes this code rerunnable.
        }
        if (admin.tableExists(MY_TABLE2_NAME)) {
            System.out.println("Disabling/deleting " + MY_TABLE2_NAME.getNameAsString());
            admin.disableTable(MY_TABLE2_NAME); // Must disable before deleting.
            admin.deleteTable(MY_TABLE2_NAME); // Deleting the Table makes this code rerunnable.
        }
        if (admin.tableExists(MY_TABLE3_NAME)) {
            System.out.println("Disabling/deleting " + MY_TABLE3_NAME.getNameAsString());
            admin.disableTable(MY_TABLE3_NAME); // Must disable before deleting.
            admin.deleteTable(MY_TABLE3_NAME); // Deleting the Table makes this code rerunnable.
        }
        if (repositoryAdmin.namespaceExists(MY_NAMESPACE_NAME)) {
            System.out.println("Deleting " + MY_NAMESPACE_NAME);
            admin.deleteNamespace(MY_NAMESPACE_NAME); // Deleting the Namespace makes this code rerunnable.
        }
        if (repositoryAdmin.namespaceExists(MY_NAMESPACE2_NAME)) {
            System.out.println("Deleting " + MY_NAMESPACE2_NAME);
            admin.deleteNamespace(MY_NAMESPACE2_NAME); // Deleting the Namespace makes this code rerunnable.
        }
    }

    static void printColDescriptors(HTableDescriptor htd) {
        System.out.println("COLUMN DESCRIPTORS FOR TABLE: " + htd.getNameAsString());
        for (HColumnDescriptor hcd : htd.getFamilies()) {
            System.out.println("  Column Descriptor: " + Bytes.toString(hcd.getName()));
        }
    }

    static void getAndPrintColumnValues (Table table) throws IOException {
        Result result = table.get(new Get(rowId1));
        byte[] retrievedValue1
                = result.getValue(MY_COLUMN_FAMILY_NAME, MY_FIRST_COLUMN_QUALIFIER);
        byte[] retrievedValue2
                = result.getValue(MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER);
        byte[] retrievedValue3
                = result.getValue(MY_COLUMN_FAMILY_NAME, MY_INVALID_COLUMN_QUALIFIER);
        System.out.println
                (Bytes.toString(retrievedValue1) + Bytes.toString(retrievedValue2)
                    + Bytes.toString(retrievedValue3));
    }

    static void getAndPrintAllCellVersions (Table table) throws IOException {
        // Invoking the Get#setMaxVersions method assures that ALL versions (i.e.,
        //   all Cells) of each ColumnAuditor will be returned in the Result set.
        Result result = table.get(new Get(rowId1).setMaxVersions());
        System.out.println("\nALL VERSIONS OF ALL CELLS (retrieved using Get#setMaxVersions)");
        if (result.listCells() == null) {
            System.out.println("  **Results NULL");
            return;
        }
        for (Cell cell : result.listCells()) {
            System.out.println("=====");
            System.out.println("  Cell qualifier (name): "
                + Bytes.toStringBinary(ByteBuffer.wrap(cell.getQualifierArray(),
                        cell.getQualifierOffset(), cell.getQualifierLength()).slice()));
            System.out.println("  Cell timestamp: " + cell.getTimestamp());
            System.out.println("  Cell value: "
                + Bytes.toString(Bytes.getBytes(CellUtil.getValueBufferShallowCopy(cell)))
//                + Bytes.toStringBinary(ByteBuffer.wrap(cell.getValueArray(),
//                        cell.getValueOffset(), cell.getValueLength()).slice())
            );
        }
    }
}
