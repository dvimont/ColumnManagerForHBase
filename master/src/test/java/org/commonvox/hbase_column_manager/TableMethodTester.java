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

import org.commonvox.hbase_column_manager.ColumnDefinition;
import org.commonvox.hbase_column_manager.MConnectionFactory;
import org.commonvox.hbase_column_manager.RepositoryAdmin;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author dv
 */
public class TableMethodTester {
    static final String MY_NAMESPACE_NAME = "myNamespace";
    static final String MY_NAMESPACE2_NAME = "anotherNamespace";
    static final TableName MY_TABLE_NAME = TableName.valueOf(MY_NAMESPACE_NAME, "myTable");
    static final TableName MY_TABLE2_NAME = TableName.valueOf(MY_NAMESPACE_NAME, "myTable2");
    static final TableName MY_TABLE3_NAME = TableName.valueOf(MY_NAMESPACE2_NAME, "anotherTable");
    static final byte[] MY_COLUMN_FAMILY_NAME = Bytes.toBytes("myColumnFamily");
    static final byte[] MY_COLUMN_FAMILY2_NAME = Bytes.toBytes("myColumnFamily2");
    static final byte[] MY_FIRST_COLUMN_QUALIFIER = Bytes.toBytes("myFirstColumn");
    static final byte[] MY_SECOND_COLUMN_QUALIFIER = Bytes.toBytes("mySecondColumn");
    static final byte[] MY_THIRD_COLUMN_QUALIFIER = Bytes.toBytes("myThirdColumn");
    static final byte[] MY_APPENDED_COLUMN_QUALIFIER = Bytes.toBytes("APPENDED_column");
    static final byte[] MY_INCREMENT_COLUMN_QUALIFIER = Bytes.toBytes("INCREMENT_column");
    static byte[] rowId1 = Bytes.toBytes("rowId01");
    static byte[] rowId2 = Bytes.toBytes("rowId02");

    public static void main(String[] args) throws Exception {
        final String TARGET_PATH = "/home/dv/Documents";

        /** HBaseConfiguration#create automatically looks for hbase-site.xml
         * (i.e., the HBase startup parameters) on the system's CLASSPATH, to
         * enable the creation of connections to Zookeeper (i.e., the directory
         * to HBase resources) & HBase. */
        Configuration hBaseConfig = HBaseConfiguration.create();
        try ( Connection mConnection
                    = MConnectionFactory.createConnection(hBaseConfig);
                Connection standardConnection
                        = ConnectionFactory.createConnection(hBaseConfig);
                RepositoryAdmin repositoryAdmin = new RepositoryAdmin(mConnection);
                Admin standardAdmin = standardConnection.getAdmin();
                Admin mAdmin = mConnection.getAdmin() )
        {
            System.out.println("*** Hello HBase! -- Connection has been established!!\n");
            NamespaceDescriptor nd = NamespaceDescriptor.create(MY_NAMESPACE_NAME).build();
            nd.setConfiguration("WidgetConfig", "WidgetsAplenty");
            if (!repositoryAdmin.namespaceExists(MY_NAMESPACE_NAME)) {
                standardAdmin.createNamespace(nd);
            }
            NamespaceDescriptor nd2 = NamespaceDescriptor.create(MY_NAMESPACE2_NAME).build();
            if (!repositoryAdmin.namespaceExists(MY_NAMESPACE2_NAME)) {
                standardAdmin.createNamespace(nd2);
            }
            // Create a Table with a Column Family; invoke #setMaxVersions(5) so
            //   that each Column in the family will retain its 4 most recent past
            //   values (i.e., timestamped versions) in addition to its current value.
            HTableDescriptor htd
                    = new HTableDescriptor(MY_TABLE_NAME)
                            .setMemStoreFlushSize(60000000)
                            .setOwnerString("OwnerSmith")
                            .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY_NAME)
                                                    .setMaxVersions(5));
            if (!standardAdmin.tableExists(MY_TABLE_NAME)) {
                standardAdmin.createTable(htd);
            }
            if (!standardAdmin.tableExists(MY_TABLE2_NAME)) {
                standardAdmin.createTable(new HTableDescriptor(MY_TABLE2_NAME)
                                    .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY2_NAME)));
            }
            if (!standardAdmin.tableExists(MY_TABLE3_NAME)) {
                standardAdmin.createTable(new HTableDescriptor(MY_TABLE3_NAME)
                                    .addFamily(new HColumnDescriptor(MY_COLUMN_FAMILY_NAME)));
            }

            htd.setMemStoreFlushSize(70000000).setDurability(Durability.SKIP_WAL)
                    .setConfiguration("MyGreatConfigParm", "A_good_value");
            standardAdmin.modifyTable(MY_TABLE_NAME, htd);

            // ADD COLUMN DEFINITIONS FOR FIRST_COLUMN and SECOND_COLUMN (but only those two!)
            ColumnDefinition firstDef
                    = new ColumnDefinition(MY_FIRST_COLUMN_QUALIFIER).setColumnLength(50L);
            repositoryAdmin.addColumnDefinition(MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME, firstDef);
            ColumnDefinition secondDef
                    = new ColumnDefinition(MY_SECOND_COLUMN_QUALIFIER)
                            .setColumnLength(100L).setColumnValidationRegex(".*World.*");
            repositoryAdmin.addColumnDefinition(MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME, secondDef);
            repositoryAdmin.setColumnDefinitionsEnforced(true, MY_TABLE_NAME, MY_COLUMN_FAMILY_NAME);

            System.out.println("DUMPING REPOSITORY AFTER creation of HBase structures but BEFORE puts");
            repositoryAdmin.dumpRepositoryTable();

            // mConnection returns an MTable object, which will populate MRepository
            try (Table table = mConnection.getTable(MY_TABLE_NAME);
                    BufferedMutator bufferedMutator
                                    = mConnection.getBufferedMutator(MY_TABLE_NAME))
            {
                // <put> (insert) a row into the Table: specify the row's unique ID,
                //   and add two Columns with values "Hello" and " World!".
                Put firstPut = new Put(rowId1);
                firstPut.addColumn(MY_COLUMN_FAMILY_NAME, MY_FIRST_COLUMN_QUALIFIER,
                                                            Bytes.toBytes("Hello")).
                                addColumn(MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER,
                                                            Bytes.toBytes(" World!"));

                // Either (1) do a standard #put
//                table.put(firstPut);

                // or (2) test submittal of a put via #mutateRow
//                RowMutations testRm = new RowMutations(rowId1);
//                testRm.add(firstPut);
//                table.mutateRow(testRm);

                // or (3) submit put via TableMultiplexer (asynchronous mutation processing)
//                HTableMultiplexer htm = repositoryAdmin.createHTableMultiplexer(1);
//                List<Put> putList = new ArrayList<>();
//                putList.add(firstPut);
//                htm.put(MY_TABLE_NAME, putList);

//                htm.put(MY_TABLE_NAME, firstPut, 5);
                try { Thread.sleep(500);  // wait for asynchronous put to complete!
                } catch(InterruptedException ex) { Thread.currentThread().interrupt(); }

                // or (4) use a BufferedMutator to submit puts.
                bufferedMutator.mutate(firstPut);
                bufferedMutator.flush();

                printColDescriptors(standardAdmin.getTableDescriptor(MY_TABLE_NAME));
                getAndPrintColumnValues(table); // Yes, this will print "Hello World!"

                System.out.println("DUMPING REPOSITORY AFTER FIRST Table transaction performed");
                repositoryAdmin.dumpRepositoryTable();


                // VARIATION #1: Put a second and third row separately
                // Now 'update' the second Column of the same row with another <put>,
                //   which stores a second 'version' of that column (retaining the first).
                Put secondPut = new Put(rowId1);
                secondPut.addColumn(MY_COLUMN_FAMILY_NAME, MY_SECOND_COLUMN_QUALIFIER,
                                                        Bytes.toBytes(" BIG DATA World!"));
                //table.put(secondPut);


                // Add a third row
                Put thirdPut = new Put(rowId1);
                thirdPut.addColumn(MY_COLUMN_FAMILY_NAME, MY_THIRD_COLUMN_QUALIFIER,
                                                        Bytes.toBytes("HELLLLLLOOOOOOOO"));
                //table.put(thirdPut);


                // VARIATION #2: Put the second and third rows using a single Table#batch call.
                List<Row> batchActions = new ArrayList<>();
                batchActions.add(secondPut);
                batchActions.add(thirdPut);
                table.batch(batchActions);

                // VARIATION #3: Put the second and third rows using multiplexer
//                putList = new ArrayList<>();
//                putList.add(secondPut);
//                putList.add(thirdPut);
//                htm.put(MY_TABLE_NAME, putList);


                printColDescriptors(standardAdmin.getTableDescriptor(MY_TABLE_NAME));
                getAndPrintColumnValues(table);
                getAndPrintAllCellVersions(table); // prints *both* versions of second Column

                table.append(new Append(rowId1).add(MY_COLUMN_FAMILY_NAME,
                                MY_APPENDED_COLUMN_QUALIFIER, Bytes.toBytes("Appended-first-value-here")));
                table.append(new Append(rowId1).add(MY_COLUMN_FAMILY_NAME,
                                MY_APPENDED_COLUMN_QUALIFIER, Bytes.toBytes("Appended-second-value-here")));
//                table.increment(new Increment(rowId1).addColumn(MY_COLUMN_FAMILY_NAME,
//                                MY_INCREMENT_COLUMN_QUALIFIER, 47));
                table.incrementColumnValue
                        (rowId1, MY_COLUMN_FAMILY_NAME, MY_INCREMENT_COLUMN_QUALIFIER, 2333347);
//                table.delete(new Delete(rowId1).addColumn
//                                                (MY_COLUMN_FAMILY_NAME, MY_APPENDED_COLUMN_QUALIFIER));

                getAndPrintAllCellVersions(table);


            }

            System.out.println("DUMPING REPOSITORY AFTER Table transactions performed");
            repositoryAdmin.dumpRepositoryTable();

//            MTableDescriptor mtd = mAdmin.getMTableDescriptor(MY_TABLE_NAME);
//            System.out.println("Retrieved enhanced Table Descriptor for Table: "
//                                    + MY_TABLE_NAME.getNameAsString());
//
//            System.out.println("Table name: " + mtd.getNameAsString());
//            System.out.println("       Table values...");
//            for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry : mtd.getValues().entrySet()) {
//                System.out.println("          " + Bytes.toString(valueEntry.getKey().copyBytes())
//                        + " : " + Bytes.toString(valueEntry.getValue().copyBytes()) );
//            }
//            System.out.println("       Table configurations...");
//            for (Map.Entry<String,String> configEntry : mtd.getConfiguration().entrySet()) {
//                System.out.println("          " + configEntry.getKey() + " : " + configEntry.getValue());
//            }
//            for (MColumnDescriptor mcd : mtd.getMFamilies()) {
//                System.out.println("  Column Descriptor: " + mcd.getNameAsString());
//                for (MColumnQualifier mcq : mcd.getColumnAuditors()) {
//                    System.out.println("    Column Qualifier: "
//                                            + Bytes.toString(mcq.getName()));
//                    System.out.println("       Column values...");
//                    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry : mcq.getValues().entrySet()) {
//                        System.out.println("          " + Bytes.toString(valueEntry.getKey().copyBytes())
//                                + " : " + Bytes.toString(valueEntry.getValue().copyBytes()) );
//                    }
//                    System.out.println("       Column configurations...");
//                    for (Map.Entry<String,String> configEntry : mcq.getConfiguration().entrySet()) {
//                        System.out.println("          " + configEntry.getKey() + " : " + configEntry.getValue());
//                    }
//                }
//            }
//            System.out.println();

//            repositoryAdmin.exportRepository
//                    (TARGET_PATH, "testExportAll.xml", false);
//            repositoryAdmin.exportNamespaceSchema
//                    (MY_NAMESPACE_NAME, TARGET_PATH, "testExportNamespace.xml", false);
//            repositoryAdmin.exportTableSchema
//                    (TableName.valueOf(MY_NAMESPACE_NAME,"myTable"), TARGET_PATH, "testExportTable.xml", false);

//            System.out.println
//                (repositoryAdmin.generateHsaFileSummary(TARGET_PATH, "testExportAll.xml"));

//            System.out.println("DUMPING REPOSITORY *BEFORE* delete of user tables and namespace");
//            mAdmin.dumpRepositoryTable();

            deleteUserObjects(mAdmin, repositoryAdmin);

//            System.out.println("DUMPING REPOSITORY *AFTER* delete of user tables and namespace");
//            mAdmin.dumpRepositoryTable();

            RepositoryAdmin.uninstallRepositoryStructures(mAdmin);
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
        System.out.println
                (Bytes.toString(retrievedValue1) + Bytes.toString(retrievedValue2));
    }

    static void getAndPrintAllCellVersions (Table table) throws IOException {
        // Invoking the Get#setMaxVersions method assures that ALL versions (i.e.,
        //   all Cells) of each Column will be returned in the Result set.
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
                + Bytes.toStringBinary(ByteBuffer.wrap(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength()).slice()));
        }
    }
}
