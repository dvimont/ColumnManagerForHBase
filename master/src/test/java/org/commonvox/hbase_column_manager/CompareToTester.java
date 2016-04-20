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
import org.commonvox.hbase_column_manager.MColumnDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Daniel Vimont
 */
public class CompareToTester {
    public static void main(String[] args) throws Exception {

        /* testing compareTo in HTableDescriptor */
        HTableDescriptor htd1 = new HTableDescriptor("AAA");
        HTableDescriptor htd2 = new HTableDescriptor("BBB");
        HTableDescriptor htd3 = new HTableDescriptor("AAA");
        htd1.setConfiguration("Somesuch", "whatnot");
        htd3.setConfiguration("Somesuch", "whatnot");

        System.out.println("htd1.compareTo(htd2) = " + htd1.compareTo(htd2));
        System.out.println("htd2.compareTo(htd1) = " + htd2.compareTo(htd1));

        MColumnDescriptor mcd1 = new MColumnDescriptor(Bytes.toBytes("colFam1"));
        MColumnDescriptor mcd2 = new MColumnDescriptor(Bytes.toBytes("colFam1"));
        // HColumnDescriptor hcd1 = mcd2.getHColumnDescriptor();

        mcd1.addColumnDefinition(new ColumnDefinition("colQual1").setConfiguration("colType", "String"));
        mcd2.addColumnDefinition(new ColumnDefinition("colQual1").setConfiguration("colType", "int"));
        System.out.println("mcd1.compareTo(mcd2) = " + mcd1.compareTo(mcd2));

        htd1.addFamily(mcd1);
        htd3.addFamily(mcd2);
        System.out.println("htd3.compareTo(htd1) = " + htd3.compareTo(htd1));
    }
}
