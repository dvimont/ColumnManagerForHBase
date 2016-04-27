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
 * Test ColumnDefinition in a standalone (non-persisting) manner. Persistence of ColumnDefinition
 * tested in {@link TestRepositoryAdmin}.
 *
 * @author Daniel Vimont
 */
public class TestColumnDefinition {

  private ColumnDefinition columnDefinition;
  private static final String COLUMN_DEFINITION_FAILURE
          = "FAILURE IN " + ColumnDefinition.class.getSimpleName() + " PROCESSING!! ==>> ";
  private static final String COLUMN_QUALIFIER = "TestColumnQualifier";
  private static final String KEY_STRING = "TestKey";
  private static final String VALUE_STRING = "TestValue";
  private static final ImmutableBytesWritable KEY_IMMUTABLE
          = new ImmutableBytesWritable(Bytes.toBytes(KEY_STRING));
  private static final ImmutableBytesWritable VALUE_IMMUTABLE
          = new ImmutableBytesWritable(Bytes.toBytes(VALUE_STRING));
  private static final long COLUMN_LENGTH = 899999999999993L;

  @Test
  public void testPublicConstructors() {

    columnDefinition = new ColumnDefinition(COLUMN_QUALIFIER);
    assertEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in column qualifier after String constructor invocation",
            COLUMN_QUALIFIER, columnDefinition.getColumnQualifierAsString());

    columnDefinition = new ColumnDefinition(Bytes.toBytes(COLUMN_QUALIFIER));
    assertArrayEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in column qualifier after byte-array constructor invocation",
            Bytes.toBytes(COLUMN_QUALIFIER), columnDefinition.getColumnQualifier());
  }

  @Test
  public void testSetValue() {

    columnDefinition
            = new ColumnDefinition(COLUMN_QUALIFIER).setValue(KEY_STRING, VALUE_STRING);
    assertEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "String #setValue and #getValue invocation",
            VALUE_STRING, columnDefinition.getValue(KEY_STRING));

    columnDefinition
            = new ColumnDefinition(COLUMN_QUALIFIER).setValue(
                    Bytes.toBytes(KEY_STRING), Bytes.toBytes(VALUE_STRING));
    assertArrayEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "byte-array #setValue and #getValue invocation",
            Bytes.toBytes(VALUE_STRING), columnDefinition.getValue(Bytes.toBytes(KEY_STRING)));

    columnDefinition
            = new ColumnDefinition(COLUMN_QUALIFIER).setValue(KEY_IMMUTABLE, VALUE_IMMUTABLE);
    assertArrayEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "ImmutableBytesWritable #setValue and #getValue invocation",
            VALUE_IMMUTABLE.get(), columnDefinition.getValue(KEY_IMMUTABLE.get()));
  }

  @Test
  public void testSetConfiguration() {

    columnDefinition
            = new ColumnDefinition(COLUMN_QUALIFIER).setConfiguration(KEY_STRING, VALUE_STRING);
    assertEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in key/value pairing involved in "
            + "String #setConfiguration and #getConfigurationValue invocation",
            VALUE_STRING, columnDefinition.getConfigurationValue(KEY_STRING));
  }

  @Test
  public void testSetColumnLength() {
    columnDefinition = new ColumnDefinition(COLUMN_QUALIFIER).setColumnLength(COLUMN_LENGTH);
    assertEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in column length in #setColumnLength and #getColumnLength invocation",
            COLUMN_LENGTH, columnDefinition.getColumnLength());
  }

  @Test
  public void testSetColumnValidationRegex() {
    columnDefinition
            = new ColumnDefinition(COLUMN_QUALIFIER).setColumnValidationRegex(VALUE_STRING);
    assertEquals(COLUMN_DEFINITION_FAILURE
            + "Inconsistency in column-validation-regex "
            + "in #setColumnValidationRegex and #getColumnValidationRegex invocation",
            VALUE_STRING, columnDefinition.getColumnValidationRegex());
  }

  public static void main(String[] args) throws Exception {
    new TestColumnDefinition().testPublicConstructors();
    new TestColumnDefinition().testSetValue();
    new TestColumnDefinition().testSetConfiguration();
    new TestColumnDefinition().testSetColumnLength();
    new TestColumnDefinition().testSetColumnValidationRegex();
  }
}
