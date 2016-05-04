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

import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class serves as a "wrapper" for the standard HBase NamespaceDescriptor class, which cannot
 * be extended since it has only private constructors.
 *
 * @author Daniel Vimont
 */
class MNamespaceDescriptor extends SchemaEntity {

  public MNamespaceDescriptor(byte[] namespaceName) {
    super(SchemaEntityType.NAMESPACE.getRecordType(), namespaceName);
  }

  public MNamespaceDescriptor(String namespaceName) {
    super(SchemaEntityType.NAMESPACE.getRecordType(), namespaceName);
  }

  public MNamespaceDescriptor(NamespaceDescriptor nd) {
    super(SchemaEntityType.NAMESPACE.getRecordType(), nd.getName());
    for (Map.Entry<String, String> configEntry : nd.getConfiguration().entrySet()) {
      this.setConfiguration(configEntry.getKey(), configEntry.getValue());
    }
  }

  /**
   * This constructor accessed during deserialization process (i.e., building of objects by pulling
   * schema components from Repository or from external archive).
   *
   * @param mEntity
   */
  MNamespaceDescriptor(SchemaEntity mEntity) {
    super(SchemaEntityType.NAMESPACE.getRecordType(), mEntity.getName());
    this.setForeignKey(mEntity.getForeignKey());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry
            : mEntity.getValues().entrySet()) {
      this.setValue(valueEntry.getKey(), valueEntry.getValue());
    }
    for (Map.Entry<String, String> configEntry : mEntity.getConfiguration().entrySet()) {
      this.setConfiguration(configEntry.getKey(), configEntry.getValue());
    }
  }

  public NamespaceDescriptor getNamespaceDescriptor() {
    NamespaceDescriptor nd
            = NamespaceDescriptor.create(Bytes.toString(this.getName())).build();
    for (Entry<String, String> configEntry : this.getConfiguration().entrySet()) {
      nd.setConfiguration(configEntry.getKey(), configEntry.getValue());
    }
    return nd;
  }

  @Override
  public MNamespaceDescriptor setValue(String key, String value) {
    super.setValue(key, value);
    return this;
  }

  @Override
  public MNamespaceDescriptor setValue(byte[] key, byte[] value) {
    super.setValue(key, value);
    return this;
  }

  @Override
  public MNamespaceDescriptor setValue(final ImmutableBytesWritable key, final ImmutableBytesWritable value) {
    super.setValue(key, value);
    return this;
  }

  @Override
  public MNamespaceDescriptor setConfiguration(String key, String value) {
    super.setConfiguration(key, value);
    return this;
  }
}
