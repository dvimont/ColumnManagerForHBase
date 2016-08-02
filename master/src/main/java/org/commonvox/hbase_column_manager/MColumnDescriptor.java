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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Extension of standard HColumnDescriptor class which contains collections of {@link ColumnAuditor}
 * and {@link ColumnDefinition} components.
 *
 * @author Daniel Vimont
 */
class MColumnDescriptor extends HColumnDescriptor {

  private final Map<byte[], ColumnAuditor> columnAuditors
          = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
  private final Map<byte[], ColumnDefinition> columnDefinitions
          = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
  private byte[] foreignKeyValue;
  private static final String COL_DEFINITIONS_ENFORCED_KEY = "_ColDefinitionsEnforced";
  /**
   *
   * @param colFamily Column Family name.
   */
  public MColumnDescriptor(byte[] colFamily) {
    super(colFamily);
  }

  /**
   * Makes a deep copy of submitted HColumnDescriptor object; if submitted object is also an
   * MColumnDescriptor, deep copies of its component {@link ColumnAuditor} and
   * {@link ColumnDefinition} collections are also made.
   *
   * @param desc Column Descriptor to "clone".
   */
  public MColumnDescriptor(HColumnDescriptor desc) {
    super(desc);
    if (MColumnDescriptor.class.isAssignableFrom(desc.getClass())) {
      MColumnDescriptor mcd = (MColumnDescriptor) desc;
      for (ColumnAuditor columnAuditor : mcd.columnAuditors.values()) {
        this.columnAuditors.put(columnAuditor.getName(), new ColumnAuditor(columnAuditor));
      }
      for (ColumnDefinition colDefinition : mcd.columnDefinitions.values()) {
        this.columnDefinitions.put(colDefinition.getName(),
                new ColumnDefinition(colDefinition));
      }
    }
  }

  MColumnDescriptor(HTableDescriptor htd, HColumnDescriptor hcd, Repository repository)
          throws IOException {
    super(hcd);
    for (ColumnAuditor columnAuditor : repository.getColumnAuditors(htd, hcd)) {
      columnAuditors.put(columnAuditor.getName(), columnAuditor);
    }
    for (ColumnDefinition colDefinition
            : repository.getColumnDefinitions(htd, hcd)) {
      columnDefinitions.put(colDefinition.getName(), colDefinition);
    }
  }

  /**
   * This constructor accessed during deserialization process (a row of column family type is read
   * from repository table and deserialized into a SchemaEntity object, which is then passed to
   * this constructor to complete the deserialization into an MColumnDescriptor object).
   *
   * @param entity
   */
  MColumnDescriptor(SchemaEntity entity) {
    super(entity.getName());
    this.setForeignKey(entity.getForeignKey());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry
            : entity.getValues().entrySet()) {
      this.setValue(valueEntry.getKey().get(), valueEntry.getValue().get());
    }
    for (Map.Entry<String, String> configEntry : entity.getConfiguration().entrySet()) {
      this.setConfiguration(configEntry.getKey(), configEntry.getValue());
    }
  }

  /**
   *
   * @param columnAuditor
   * @return this object, to allow method chaining
   */
  MColumnDescriptor addColumnAuditor(ColumnAuditor columnAuditor) {
    columnAuditors.put(columnAuditor.getName(), columnAuditor);
    return this;
  }

  /**
   *
   * @param columnAuditors
   * @return this object, to allow method chaining
   */
  MColumnDescriptor addColumnAuditors(ColumnAuditor[] columnAuditors) {
    for (ColumnAuditor columnAuditor : columnAuditors) {
      this.columnAuditors.put(columnAuditor.getName(), columnAuditor);
    }
    return this;
  }

  /**
   *
   * @param columnAuditors
   * @return this object, to allow method chaining
   */
  MColumnDescriptor addColumnAuditors(Collection<ColumnAuditor> columnAuditors) {
    MColumnDescriptor.this.addColumnAuditors(columnAuditors.toArray(new ColumnAuditor[columnAuditors.size()]));
    return this;
  }

  Set<byte[]> getColumnQualifiers() {
    return columnAuditors.keySet();
  }

  Set<ColumnAuditor> getColumnAuditors() {
    return new TreeSet<>(columnAuditors.values());
  }

  /**
   *
   * @param columnDefinition
   * @return this object, to allow method chaining
   */
  MColumnDescriptor addColumnDefinition(ColumnDefinition columnDefinition) {
    columnDefinitions.put(columnDefinition.getName(), columnDefinition);
    return this;
  }

  /**
   *
   * @param columnDefinitionArray
   * @return this object, to allow method chaining
   */
  MColumnDescriptor addColumnDefinitions(ColumnDefinition[] columnDefinitionArray) {
    for (ColumnDefinition columnDefinition : columnDefinitionArray) {
      columnDefinitions.put(columnDefinition.getName(), columnDefinition);
    }
    return this;
  }

  /**
   *
   * @param columnDefinitions
   * @return this object, to allow method chaining
   */
  MColumnDescriptor addColumnDefinitions(Collection<ColumnDefinition> columnDefinitions) {
    addColumnDefinitions(columnDefinitions.toArray(new ColumnDefinition[columnDefinitions.size()]));
    return this;
  }

  Collection<ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions.values();
  }

  ColumnDefinition getColumnDefinition(byte[] colQualifier) {
    return columnDefinitions.get(colQualifier);
  }

  byte[] getForeignKey() {
    return foreignKeyValue;
  }

  final void setForeignKey(byte[] foreignKeyValue) {
    this.foreignKeyValue = foreignKeyValue;
  }

  void setColumnDefinitionsEnforced(boolean enabled) {
    this.setConfiguration(COL_DEFINITIONS_ENFORCED_KEY, String.valueOf(enabled));
  }

  boolean columnDefinitionsEnforced() {
    String enabledString = this.getConfigurationValue(COL_DEFINITIONS_ENFORCED_KEY);
    return enabledString == null ? false : Boolean.valueOf(enabledString);
  }

  @Override
  public int compareTo(HColumnDescriptor other) {
    if (MColumnDescriptor.class.isAssignableFrom(other.getClass())) {
      MColumnDescriptor mcdOther = (MColumnDescriptor) other;
      int result = Bytes.compareTo(this.getName(), mcdOther.getName());
      if (result == 0 && this.columnAuditors.size()
              != mcdOther.columnAuditors.size()) {
        result = Integer.valueOf(this.columnAuditors.size())
                .compareTo(Integer.valueOf(mcdOther.columnAuditors.size()));
      }
      if (result == 0) {
        for (Iterator<ColumnAuditor> it = this.columnAuditors.values().iterator(),
                it2 = mcdOther.columnAuditors.values().iterator(); it.hasNext();) {
          result = it.next().compareTo(it2.next());
          if (result != 0) {
            break;
          }
        }
      }
      if (result != 0) {
        return result;
      }
    }
    return super.compareTo(other); // compares hash values of configuration and value Maps
  }
}
