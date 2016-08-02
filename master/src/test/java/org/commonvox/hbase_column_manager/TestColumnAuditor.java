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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Test ColumnAuditor in a standalone (non-persisting) manner. Persistence of ColumnAuditor
 * tested in {@link TestRepositoryAdmin}.
 *
 * @author Daniel Vimont
 */
public class TestColumnAuditor {

  private ColumnAuditor columnAuditor;
  private static final String COLUMN_AUDITOR_FAILURE
          = "FAILURE IN " + ColumnAuditor.class.getSimpleName() + " PROCESSING!! ==>> ";
  private static final String COLUMN_QUALIFIER = "TestColumnQualifier";
  private static final String KEY_STRING = "TestKey";
  private static final String VALUE_STRING = "TestValue";
  private static final ImmutableBytesWritable KEY_IMMUTABLE
          = new ImmutableBytesWritable(Bytes.toBytes(KEY_STRING));
  private static final ImmutableBytesWritable VALUE_IMMUTABLE
          = new ImmutableBytesWritable(Bytes.toBytes(VALUE_STRING));
  private static final long COLUMN_LENGTH = 899999999999993L;

  @Test
  public void testConstructors() {

    columnAuditor = new ColumnAuditor(COLUMN_QUALIFIER);
    assertEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in column qualifier after String constructor invocation",
            COLUMN_QUALIFIER, columnAuditor.getColumnQualifierAsString());

    columnAuditor = new ColumnAuditor(Bytes.toBytes(COLUMN_QUALIFIER));
    assertArrayEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in column qualifier after byte-array constructor invocation",
            Bytes.toBytes(COLUMN_QUALIFIER), columnAuditor.getColumnQualifier());
  }

  @Test
  public void testSetValue() {

    columnAuditor
            = new ColumnAuditor(COLUMN_QUALIFIER).setValue(KEY_STRING, VALUE_STRING);
    assertEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "String #setValue and #getValue invocation",
            VALUE_STRING, columnAuditor.getValue(KEY_STRING));

    columnAuditor
            = new ColumnAuditor(COLUMN_QUALIFIER).setValue(
                    Bytes.toBytes(KEY_STRING), Bytes.toBytes(VALUE_STRING));
    assertArrayEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "byte-array #setValue and #getValue invocation",
            Bytes.toBytes(VALUE_STRING), columnAuditor.getValue(Bytes.toBytes(KEY_STRING)));

    columnAuditor
            = new ColumnAuditor(COLUMN_QUALIFIER).setValue(KEY_IMMUTABLE, VALUE_IMMUTABLE);
    assertArrayEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "ImmutableBytesWritable #setValue and #getValue invocation",
            VALUE_IMMUTABLE.get(), columnAuditor.getValue(KEY_IMMUTABLE.get()));
  }

  @Test
  public void testSetConfiguration() {

    columnAuditor
            = new ColumnAuditor(COLUMN_QUALIFIER).setConfiguration(KEY_STRING, VALUE_STRING);
    assertEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "String #setConfiguration and #getConfigurationValue invocation",
            VALUE_STRING, columnAuditor.getConfigurationValue(KEY_STRING));
  }

  @Test
  public void testSetColumnOccurrencesCount() {
    final long LONG_VALUE = 123987543;
    final String STRING_FROM_LONG = String.valueOf(LONG_VALUE);
    columnAuditor = new ColumnAuditor(COLUMN_QUALIFIER)
            .setValue(ColumnAuditor.COL_COUNTER_KEY, STRING_FROM_LONG);
    assertEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in set and get of long value via #getColumnOccurrencesCount",
            LONG_VALUE, columnAuditor.getColumnOccurrencesCount());
  }

  @Test
  public void testSetCellOccurrencesCount() {
    final long LONG_VALUE = 123987543;
    final String STRING_FROM_LONG = String.valueOf(LONG_VALUE);
    columnAuditor = new ColumnAuditor(COLUMN_QUALIFIER)
            .setValue(ColumnAuditor.CELL_COUNTER_KEY, STRING_FROM_LONG);
    assertEquals(COLUMN_AUDITOR_FAILURE
            + "Inconsistency in set and get of long value via #getCellOccurrencesCount",
            LONG_VALUE, columnAuditor.getCellOccurrencesCount());
  }

  public static void main(String[] args) throws Exception {
    new TestColumnAuditor().testConstructors();
    new TestColumnAuditor().testSetValue();
    new TestColumnAuditor().testSetConfiguration();
    new TestColumnAuditor().testSetColumnOccurrencesCount();
    new TestColumnAuditor().testSetCellOccurrencesCount();
  }
}
