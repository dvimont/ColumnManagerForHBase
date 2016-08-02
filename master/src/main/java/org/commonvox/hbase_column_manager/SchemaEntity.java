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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author Daniel Vimont
 */
@XmlAccessorType(XmlAccessType.FIELD)
// @XmlRootElement(name = "hBaseSchemaEntity")
@XmlType(name="hBaseSchemaEntity")
class SchemaEntity implements Comparable<SchemaEntity> {

  // all fields are non-final to facilitate straightforward JAXB marshal/unmarshal
  @XmlAttribute
  private SchemaEntityType schemaEntityType;
  @XmlAttribute
  private String name;

  private final Map<String, String> values = new HashMap<>(); // String entries for JAXB!
  private final Map<String, String> configurations = new HashMap<>();
  @XmlElementWrapper(name="childEntities")
  @XmlElement(name="hBaseSchemaEntity")
  private TreeSet<SchemaEntity> children = null; // TreeSet stipulated for ordered unmarshalling!!
  @XmlTransient
  private byte[] foreignKeyValue;
  @XmlTransient
  private SchemaEntityType type;

  public SchemaEntity() {
  }

  SchemaEntity(byte entityType, byte[] name) {
    this.type = SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP.get(entityType);
    this.schemaEntityType = SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP.get(entityType);
    this.name = Bytes.toString(name);
  }

  SchemaEntity(byte entityType, String name) {
    this(entityType, Bytes.toBytes(name));
  }

  SchemaEntity(MNamespaceDescriptor namespaceDescriptor) {
    this(namespaceDescriptor.getEntityRecordType(), namespaceDescriptor.getName());
    shallowClone(namespaceDescriptor);
  }

  SchemaEntity(SchemaEntity schemaEntity) {
    this(schemaEntity.getEntityRecordType(), schemaEntity.getName());
    shallowClone(schemaEntity);
  }

  private void shallowClone(SchemaEntity entity) {
    for (Map.Entry<String, String> valueEntry : entity.values.entrySet()) {
      this.values.put(valueEntry.getKey(), valueEntry.getValue());
    }
    for (Map.Entry<String, String> configEntry : entity.configurations.entrySet()) {
      this.configurations.put(configEntry.getKey(), configEntry.getValue());
    }
  }

  SchemaEntity(MTableDescriptor mtd) {
    this(SchemaEntityType.TABLE.getRecordType(), mtd.getNameAsString());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry
            : mtd.getValues().entrySet()) {
      this.values.put(Bytes.toString(valueEntry.getKey().get()),
              Bytes.toString(valueEntry.getValue().get()));
    }
    for (Map.Entry<String, String> configEntry : mtd.getConfiguration().entrySet()) {
      this.configurations.put(configEntry.getKey(), configEntry.getValue());
    }
  }

  SchemaEntity(MColumnDescriptor mcd) {
    this(SchemaEntityType.COLUMN_FAMILY.getRecordType(), mcd.getNameAsString());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> valueEntry
            : mcd.getValues().entrySet()) {
      this.values.put(Bytes.toString(valueEntry.getKey().get()),
              Bytes.toString(valueEntry.getValue().get()));
    }
    for (Map.Entry<String, String> configEntry : mcd.getConfiguration().entrySet()) {
      this.configurations.put(configEntry.getKey(), configEntry.getValue());
    }
  }

  byte getEntityRecordType() {
    return schemaEntityType.getRecordType();
  }

  SchemaEntityType getSchemaEntityType() {
    if (type == null) {
      type = SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP.get(getEntityRecordType());
    }
    return type;
  }

  /**
   * Get the Name of this entity.
   *
   * @return name as byte-array
   */
  byte[] getName() {
    return name.getBytes();
  }

  /**
   * Get the Name of this entity as a String.
   *
   * @return name as String
   */
  String getNameAsString() {
    return name;
  }

  /**
   * Get this object's values map.
   *
   * @return this object's values map
   */
  Map<ImmutableBytesWritable, ImmutableBytesWritable> getValues() {
    Map<ImmutableBytesWritable, ImmutableBytesWritable> valuesImmutable = new HashMap<>();
    for (Entry<String, String> entry : values.entrySet()) {
        valuesImmutable.put(new ImmutableBytesWritable(entry.getKey().getBytes()),
                new ImmutableBytesWritable(entry.getValue().getBytes()));
    }
    return valuesImmutable;
  }

  Map<String, String> getValuesStringMap() {
    return values;
  }

  /**
   * Get the value for a specific Value entry.
   *
   * @param key the key
   * @return the value
   */
  byte[] getValue(byte[] key) {
    String valueAsString = values.get(Bytes.toString(key));
    if (valueAsString == null) {
      return null;
    }
    if ((key.length > Repository.COUNTER_COLUMN_PREFIX_BYTES.length
              && Bytes.startsWith(key, Repository.COUNTER_COLUMN_PREFIX_BYTES))
            || (key.length > Repository.TIMESTAMP_KEY_PREFIX_BYTES.length
                  && Bytes.startsWith(key, Repository.TIMESTAMP_KEY_PREFIX_BYTES))) {
      return Bytes.toBytes(Long.valueOf(valueAsString)); // counters & timestamps are long
    } else {
      return valueAsString.getBytes();
    }
  }

  /**
   * Get the value for a specific Value entry.
   *
   * @param key the key
   * @return the value as String
   */
  String getValue(String key) {
//    byte[] value = getValue(Bytes.toBytes(key));
//    return (value == null) ? null : Bytes.toString(value);
    return values.get(key);
  }

  /**
   * Remove a value entry from value map.
   *
   * @param key the key of entry to remove
   */
  void remove(final byte[] key) {
    // values.remove(new ImmutableBytesWritable(key));
    values.remove(Bytes.toString(key));
  }

  /**
   * Setter for adding value entry to value map.
   *
   * @param key Value key
   * @param value Value value
   * @return this object to allow method chaining
   */
  SchemaEntity setValue(String key, String value) {
    if (value == null) {
      remove(Bytes.toBytes(key));
    } else {
//      setValue(Bytes.toBytes(key), Bytes.toBytes(value));
      values.put(key, value);
    }
    return this;
  }

  SchemaEntity setValue(byte[] key, byte[] value) {
    String keyString = Bytes.toString(key);
    if ((key.length > Repository.COUNTER_COLUMN_PREFIX_BYTES.length
              && Bytes.startsWith(key, Repository.COUNTER_COLUMN_PREFIX_BYTES))
            || (key.length > Repository.TIMESTAMP_KEY_PREFIX_BYTES.length
                  && Bytes.startsWith(key, Repository.TIMESTAMP_KEY_PREFIX_BYTES))) {
      values.put(keyString, String.valueOf(Bytes.toLong(value))); // counters & timestamps are long
    } else {
      values.put(keyString, Bytes.toString(value));
    }
    return this;
  }

  SchemaEntity setValue(final ImmutableBytesWritable key, final ImmutableBytesWritable value) {
    String keyString = Bytes.toString(key.get());
    if ((key.getLength() > Repository.COUNTER_COLUMN_PREFIX_BYTES.length
              && Bytes.startsWith(key.get(), Repository.COUNTER_COLUMN_PREFIX_BYTES))
            || (key.getLength() > Repository.TIMESTAMP_KEY_PREFIX_BYTES.length
                  && Bytes.startsWith(key.get(), Repository.TIMESTAMP_KEY_PREFIX_BYTES))) {
      values.put(keyString, String.valueOf(Bytes.toLong(value.get()))); // counters/timestamps long
    } else {
      values.put(keyString, Bytes.toString(value.get()));
    }
    return this;
  }

  /**
   * Get this object's configuration map.
   *
   * @return this object's configuration map
   */
  Map<String, String> getConfiguration() {
    return configurations;
  }

  /**
   * Get the value for a specific Configuration entry.
   *
   * @param key the key
   * @return the value
   */
  String getConfigurationValue(String key) {
    return configurations.get(key);
  }

  /**
   * Setter for adding configuration entry to configuration map.
   *
   * @param key Value key
   * @param value Value value
   * @return this object to allow method chaining
   */
  SchemaEntity setConfiguration(String key, String value) {
    if (value == null) {
      removeConfiguration(key);
    } else {
      configurations.put(key, value);
    }
    return this;
  }

  /**
   * Remove a configuration entry from configuration map.
   *
   * @param key the key of entry to remove
   */
  void removeConfiguration(String key) {
    configurations.remove(key);
  }

  byte[] getForeignKey() {
    return foreignKeyValue;
  }

  void setForeignKey(byte[] foreignKeyValue) {
    this.foreignKeyValue = foreignKeyValue;
  }

  void addChild(SchemaEntity child) {
    if (children == null) {
      children = new TreeSet<>();
    }
    children.add(child);
  }

  Set<SchemaEntity> getChildren() {
    if (children == null) {
      return new TreeSet<>();
    } else {
      return children;
    }
  }

  /**
   * Contents of returned Set are MNamespaceDescriptor objects and MTableDescriptor objects; note
   * that MTableDescriptor objects contain MColumnDescriptor objects, which in turn contain
   * ColumnAuditor objects and ColumnDefinition objects.
   */
  static Set<Object> convertToNamespaceAndTableDescriptorSet(SchemaEntity entity,
          String namespaceFilter, TableName tableNameFilter, byte[] colFamilyFilter) {
    Set<Object> convertedObjects = new LinkedHashSet<>();
    if (entity.getEntityRecordType() == SchemaEntityType.NAMESPACE.getRecordType()) {
      if (namespaceFilter != null && !namespaceFilter.equals(entity.getNameAsString())) {
        return convertedObjects; // empty set
      }
      convertedObjects.add(new MNamespaceDescriptor(entity));
      for (SchemaEntity childEntity : entity.getChildren()) {
        convertedObjects.addAll(convertToNamespaceAndTableDescriptorSet(
                childEntity, namespaceFilter, tableNameFilter, colFamilyFilter));
      }
    } else if (entity.getEntityRecordType() == SchemaEntityType.TABLE.getRecordType()) {
      if (tableNameFilter != null
              && !tableNameFilter.getNameAsString().equals(entity.getNameAsString())) {
        return convertedObjects; // empty set
      }
      MTableDescriptor mtd = new MTableDescriptor(entity);
      if (entity.getChildren() != null) {
        for (SchemaEntity childEntity : entity.getChildren()) {
          if (childEntity.getEntityRecordType() != SchemaEntityType.COLUMN_FAMILY.getRecordType()) {
            continue;
          }
          Set<Object> returnedMcdSet
                  = convertToNamespaceAndTableDescriptorSet(
                          childEntity, namespaceFilter, tableNameFilter, colFamilyFilter);
          for (Object returnedMcd : returnedMcdSet) {
            mtd.addFamily((MColumnDescriptor) returnedMcd);
          }
        }
      }
      convertedObjects.add(mtd);
    } else if (entity.getEntityRecordType() == SchemaEntityType.COLUMN_FAMILY.getRecordType()) {
      if (colFamilyFilter != null && !Bytes.toString(colFamilyFilter).equals(entity.getNameAsString())) {
        return convertedObjects; // empty set
      }
      MColumnDescriptor mcd = new MColumnDescriptor(entity);
      if (entity.getChildren() != null) {
        for (SchemaEntity childEntity : entity.getChildren()) {
          if (childEntity.getEntityRecordType()
                  == SchemaEntityType.COLUMN_AUDITOR.getRecordType()) {
            mcd.addColumnAuditor(new ColumnAuditor(childEntity));
          } else if (childEntity.getEntityRecordType()
                  == SchemaEntityType.COLUMN_DEFINITION.getRecordType()) {
            mcd.addColumnDefinition(new ColumnDefinition(childEntity));
          }
        }
      }
      convertedObjects.add(mcd);
    }
    return convertedObjects;
  }


  @Override
  public int compareTo(SchemaEntity other) {
    int result = this.schemaEntityType.compareTo(other.schemaEntityType);
    if (result == 0) {
      result = this.name.compareTo(other.name);
    }
    if (result == 0) {
      int childrenSize = (children == null) ? 0 : children.size();
      int otherChildrenSize = (other.children == null) ? 0 : other.children.size();
      result = childrenSize - otherChildrenSize;
    }
    if (result == 0 && children != null && other.children != null) {
      for (Iterator<SchemaEntity> it = children.iterator(), it2 = other.children.iterator();
              it.hasNext(); ) {
        result = it.next().compareTo(it2.next());
        if (result != 0) {
          break;
        }
      }
    }
    if (result == 0) {
      result = this.values.hashCode() - other.values.hashCode();
      if (result < 0) {
        result = -1;
      } else if (result > 0) {
        result = 1;
      }
    }
    if (result == 0) {
      result = this.configurations.hashCode() - other.configurations.hashCode();
      if (result < 0) {
        result = -1;
      } else if (result > 0) {
        result = 1;
      }
    }
    return result;
  }

  @Override
  public boolean equals (Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof SchemaEntity)) {
      return false;
    }
    return compareTo((SchemaEntity)obj) == 0;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 41 * hash + Objects.hashCode(this.schemaEntityType);
    hash = 41 * hash + Objects.hashCode(this.name);
    hash = 41 * hash + Objects.hashCode(this.values);
    hash = 41 * hash + Objects.hashCode(this.configurations);
    hash = 41 * hash + Objects.hashCode(this.children);
    return hash;
  }

  /**
   * Returns a String in the format "{@link SchemaEntityType}: EntityName"
   * (e.g., "{@code ColumnFamily: myColumnFamily}").
   *
   * @return formatted String
   */
  @Override
  public String toString() {
    return schemaEntityType + ": " + this.name;
  }
}
