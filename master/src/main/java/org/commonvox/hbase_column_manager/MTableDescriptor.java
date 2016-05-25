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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Extension of standard HTableDescriptor class which contains {@link MColumnDescriptor} components,
 * which in turn contain {@link Column} and {@link ColumnDefinition} components.
 *
 * @author Daniel Vimont
 */
class MTableDescriptor extends HTableDescriptor {

  private byte[] foreignKeyValue;

  /**
   * The superclass constructor that is mirrored by this constructor is slated for deprecation in
   * HBase 2.0
   */
  MTableDescriptor() {
    super();
  }

  /**
   * The superclass constructor that is mirrored by this constructor is slated for deprecation in
   * HBase 2.0
   *
   * @param name table name
   */
  MTableDescriptor(byte[] name) {
    super(name);
  }

  /**
   * The superclass constructor that is mirrored by this constructor is slated for deprecation in
   * HBase 2.0
   *
   * @param name Table name.
   */
  MTableDescriptor(String name) {
    super(name);
  }

  /**
   * Makes a deep copy of submitted HTableDescriptor object.
   *
   * @param desc Table descriptor to copy.
   */
  public MTableDescriptor(HTableDescriptor desc) {
    super(desc);
//        for (HColumnDescriptor hcd : desc.getFamilies()) {
//            this.addFamily(new MColumnDescriptor(hcd));
//        }
  }

  /**
   * Construct a table descriptor by specifying a TableName object
   *
   * @param name Table name (in the preferred encapsulated format).
   */
  MTableDescriptor(TableName name) {
    super(name);
  }

  /**
   * This constructor is accessed by {@link RepositoryAdmin} to retrieve an {@link MTableDescriptor}
   * that has its component {@link MColumnDescriptor} objects populated with metadata retrieved from
   * the {@link Repository}.
   *
   * @param htd
   * @param repository
   * @throws IOException
   */
  MTableDescriptor(HTableDescriptor htd, Repository repository)
          throws IOException {
    super(htd);
    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      this.removeFamily(hcd.getName());
      this.addFamily(new MColumnDescriptor(htd, hcd, repository));
    }
  }

  /**
   * This constructor accessed during deserialization process (i.e., building of objects by pulling
   * schema components from Repository or from external archive).
   *
   * @param entity
   */
  MTableDescriptor(SchemaEntity entity) {
    super(entity.getName());
    this.setForeignKey(entity.getForeignKey());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry
            : entity.getValues().entrySet()) {
      this.setValue(valueEntry.getKey(), valueEntry.getValue());
    }
    for (Map.Entry<String, String> configEntry : entity.getConfiguration().entrySet()) {
      this.setConfiguration(configEntry.getKey(), configEntry.getValue());
    }
  }

  public MColumnDescriptor[] getMColumnDescriptorArray() {
    Collection<MColumnDescriptor> mColumnDescriptors = getMColumnDescriptors();
    return mColumnDescriptors.toArray(new MColumnDescriptor[mColumnDescriptors.size()]);
  }

  public Collection<MColumnDescriptor> getMColumnDescriptors() {
    List<MColumnDescriptor> mColumnDescriptors = new ArrayList<>();
    for (HColumnDescriptor hcd : this.getColumnFamilies()) {
      if (MColumnDescriptor.class.isAssignableFrom(hcd.getClass())) {
        mColumnDescriptors.add((MColumnDescriptor) hcd);
      }
    }
    return mColumnDescriptors;
  }

  public MColumnDescriptor getMColumnDescriptor(byte[] colFamily) {
    HColumnDescriptor hcd = this.getFamily(colFamily);
    if (MColumnDescriptor.class.isAssignableFrom(hcd.getClass())) {
      return (MColumnDescriptor) hcd;
    } else {
      return null;
    }
  }

  final void setForeignKey(byte[] foreignKeyValue) {
    this.foreignKeyValue = foreignKeyValue;
  }

  byte[] getForeignKey() {
    return foreignKeyValue;
  }

  boolean hasColumnDefinitions() {
    for (MColumnDescriptor mcd : getMColumnDescriptors()) {
      if (!mcd.getColumnDefinitions().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  boolean hasColDescriptorWithColDefinitionsEnforced() {
    for (MColumnDescriptor mcd : getMColumnDescriptors()) {
      if (mcd.columnDefinitionsEnforced()) {
        return true;
      }
    }
    return false;
  }
}
