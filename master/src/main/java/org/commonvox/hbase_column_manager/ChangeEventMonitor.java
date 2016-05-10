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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.commonvox.collections.KeyComponentProfile;
import org.commonvox.collections.OrderedSet;

/**
 * A <b>ChangeEventMonitor</b> (obtained via a {@code RepositoryAdmin}'s
 * {@link RepositoryAdmin#getChangeEventMonitor() getChangeEventMonitor} method) provides various
 * {@link ChangeEventMonitor#getAllChangeEvents() get methods} by which lists of
 * {@link ChangeEvent}s may be obtained (grouped and ordered in various ways), and the class
 * provides a static  {@link #exportChangeEventListToCsvFile(java.util.List, java.io.File)
 * convenience method} for outputting a list of {@code ChangeEvent}s to a CSV file.
 *
 * @author Daniel Vimont
 */
public class ChangeEventMonitor {
  private final static char COMMA = ',';
  private final KeyComponentProfile<ChangeEvent> timestampComponent
          = new KeyComponentProfile<>(ChangeEvent.class, ChangeEvent.Timestamp.class);
  private final KeyComponentProfile<ChangeEvent> entityComponent
          = new KeyComponentProfile<>(ChangeEvent.class, ChangeEvent.Entity.class);
  private final KeyComponentProfile<ChangeEvent> attributeNameComponent
          = new KeyComponentProfile<>(ChangeEvent.class, ChangeEvent.AttributeName.class);
  private final KeyComponentProfile<ChangeEvent> userNameComponent
          = new KeyComponentProfile<>(ChangeEvent.class, ChangeEvent.UserName.class);
  private final OrderedSet<ChangeEvent> timestampIndex
          = new OrderedSet<>(timestampComponent, entityComponent, attributeNameComponent);
  private final OrderedSet<ChangeEvent> userIndex
          = new OrderedSet<>(userNameComponent, timestampComponent, entityComponent, attributeNameComponent);
  private OrderedSet<ChangeEvent> entityIndex
          = new OrderedSet<>(entityComponent, timestampComponent, attributeNameComponent);
  private final static Charset ENCODING = StandardCharsets.UTF_8;

  ChangeEventMonitor() {
  }

  void add(ChangeEvent changeEvent) {
    entityIndex.add(changeEvent);
  }

  ChangeEventMonitor denormalize() {
    // retrieve complete list of Entities and build entityForeignKeyMap
    Map<byte[], ChangeEvent.Entity> entityForeignKeyMap = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    for (Object entityObject : entityIndex.keyComponentSet(entityComponent)) {
      ChangeEvent.Entity entity = (ChangeEvent.Entity) entityObject;
      entityForeignKeyMap.put(entity.getEntityForeignKey().getBytes(), entity);
    }

    // "denormalize" each ChangeEvent by adding namespace, table, etc. to each one
    OrderedSet<ChangeEvent> denormalizedEntityIndex
            = new OrderedSet<>(entityComponent, timestampComponent, attributeNameComponent);
    ChangeEvent.Entity denormalizedEntity
            = ChangeEvent.createEntity((byte) ' ', null, null);
    for (ChangeEvent event : entityIndex.values()) {
      ChangeEvent.Entity entity = event.getEntity();
      if (entity.compareTo(denormalizedEntity) != 0) {
        byte[] namespaceForeignKey = {(byte) ' '};
        byte[] tableForeignKey = {(byte) ' '};
        byte[] colFamilyForeignKey = {(byte) ' '};
        byte[] colQualifierForeignKey = {(byte) ' '};
        switch (event.getEntityType()) {
          case NAMESPACE:
            namespaceForeignKey = entity.getEntityForeignKey().getBytes();
            break;
          case TABLE:
            tableForeignKey = entity.getEntityForeignKey().getBytes();
            namespaceForeignKey = entity.getParentForeignKey().getBytes();
            break;
          case COLUMN_FAMILY:
            colFamilyForeignKey = entity.getEntityForeignKey().getBytes();
            tableForeignKey = entity.getParentForeignKey().getBytes();
            namespaceForeignKey
                    = entityForeignKeyMap.get(tableForeignKey).getParentForeignKey().getBytes();
            break;
          case COLUMN_AUDITOR:
          case COLUMN_DEFINITION:
            colQualifierForeignKey = entity.getEntityForeignKey().getBytes();
            colFamilyForeignKey = entity.getParentForeignKey().getBytes();
            tableForeignKey
                    = entityForeignKeyMap.get(colFamilyForeignKey).getParentForeignKey().getBytes();
            namespaceForeignKey
                    = entityForeignKeyMap.get(tableForeignKey).getParentForeignKey().getBytes();
            break;
        }
        entity.setNamespaceEntity(entityForeignKeyMap.get(namespaceForeignKey));
        entity.setTableEntity(entityForeignKeyMap.get(tableForeignKey));
        entity.setColumnFamilyEntity(entityForeignKeyMap.get(colFamilyForeignKey));
        entity.setColumnQualifierEntity(entityForeignKeyMap.get(colQualifierForeignKey));

        denormalizedEntity = entity;
      }
      event.setEntity(denormalizedEntity);
      timestampIndex.add(event);
      userIndex.add(event);
      denormalizedEntityIndex.add(event);
    }
    entityIndex = denormalizedEntityIndex;
    return this;
  }

  /**
   * Get a list of all {@link ChangeEvent}s in the ColumnManager repository in the default (timestamp)
   * order.
   *
   * @return complete list of {@link ChangeEvent}s in timestamp order
   */
  public List<ChangeEvent> getAllChangeEvents() {
    return timestampIndex.values();
  }

  /**
   * Get a list of all {@link ChangeEvent}s in the ColumnManager repository, ordered by user name (as
   * designated by the Java "user.name" property in effect within a session as a change was made).
   *
   * @return complete list of {@link ChangeEvent}s in user-name and timestamp order
   */
  public List<ChangeEvent> getAllChangeEventsByUserName() {
    return userIndex.values();
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to a specific user name (as designated by
   * the Java "user.name"
   * property in effect within a session as a change was made), in timestamp order.
   *
   * @param userName value of Java "user.name" property in effect when change was made
   * @return list of {@link ChangeEvent}s pertaining to the user name, in timestamp order
   */
  public List<ChangeEvent> getChangeEventsForUserName(String userName) {
    return userIndex.values(ChangeEvent.createUserName(userName));
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.NamespaceDescriptor Namespace}, in timestamp order.
   *
   * @param namespaceName Namespace name
   * @return list of {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.NamespaceDescriptor Namespace}
   */
  public List<ChangeEvent> getChangeEventsForNamespace(byte[] namespaceName) {
    return getChangeEventsForEntity(SchemaEntityType.NAMESPACE, namespaceName, null, null, null);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}, in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @return list of {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}
   */
  public List<ChangeEvent> getChangeEventsForTable(TableName tableName) {
    return getChangeEventsForEntity(
            SchemaEntityType.TABLE, tableName.getNamespace(), tableName.getName(), null, null);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}, in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param attributeName name of attribute
   * @return list of {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}
   */
  public List<ChangeEvent> getChangeEventsForTableAttribute(
          TableName tableName, String attributeName) {
    List<ChangeEvent> tableEvents = getChangeEventsForTable(tableName);
    List<ChangeEvent> attributeEvents = new ArrayList<>();
    for (ChangeEvent changeEvent : tableEvents) {
      if (attributeName.equals(changeEvent.getAttributeNameAsString())) {
        attributeEvents.add(changeEvent);
      }
    }
    return attributeEvents;
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.HColumnDescriptor Column Family}, in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param columnFamily name of Column Family
   * @return list of {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.HColumnDescriptor Column Family}
   */
  public List<ChangeEvent> getChangeEventsForColumnFamily(
          TableName tableName, byte[] columnFamily) {
    return getChangeEventsForEntity(SchemaEntityType.COLUMN_FAMILY, tableName.getNamespace(),
            tableName.getName(), columnFamily, null);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HColumnDescriptor Column Family}, in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param columnFamily name of Column Family
   * @param attributeName name of attribute
   * @return list of {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HColumnDescriptor Column Family}
   */
  public List<ChangeEvent> getChangeEventsForColumnFamilyAttribute(
          TableName tableName, byte[] columnFamily, String attributeName) {
    List<ChangeEvent> cfEvents = getChangeEventsForColumnFamily(tableName, columnFamily);
    List<ChangeEvent> attributeEvents = new ArrayList<>();
    for (ChangeEvent changeEvent : cfEvents) {
      if (attributeName.equals(changeEvent.getAttributeNameAsString())) {
        attributeEvents.add(changeEvent);
      }
    }
    return attributeEvents;
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified {@link ColumnDefinition},
   * in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param columnFamily name of Column Family
   * @param columnQualifier qualifier that identifies the {@link ColumnDefinition}
   * @return list of {@link ChangeEvent}s pertaining to the specified {@link ColumnDefinition}
   */
  public List<ChangeEvent> getChangeEventsForColumnDefinition(
          TableName tableName, byte[] columnFamily, byte[] columnQualifier) {
    return getChangeEventsForEntity(SchemaEntityType.COLUMN_DEFINITION, tableName.getNamespace(),
            tableName.getName(), columnFamily, columnQualifier);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified {@link ColumnAuditor},
   * in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param columnFamily name of Column Family
   * @param columnQualifier qualifier that identifies the {@link ColumnAuditor}
   * @return list of {@link ChangeEvent}s pertaining to the specified {@link ColumnAuditor}
   */
  public List<ChangeEvent> getChangeEventsForColumnAuditor(
          TableName tableName, byte[] columnFamily, byte[] columnQualifier) {
    return getChangeEventsForEntity(SchemaEntityType.COLUMN_AUDITOR, tableName.getNamespace(),
            tableName.getName(), columnFamily, columnQualifier);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to a specified Entity (e.g., a specific <i>Table</i>),
   * in timestamp order. Note that some parameters will not apply to some {@link SchemaEntityType}s (e.g.
   * columnFamily does not apply when entityType is {@code TABLE}), in which case {@code null}s may
   * be submitted for inapplicable parameters.
   *
   * @param entityType type of Entity for which {@code ChangeEvent} list is to be produced
   * @param namespaceName Name of <i>Namespace</i> which pertains to the Entity
   * @param tableName Name of <i>Table</i> which pertains to the Entity (if applicable)
   * @param columnFamily Name of <i>Column Family</i> which pertains to the Entity (if applicable)
   * @param columnQualifier <i>Column Qualifier</i> which pertains to the Entity (if applicable)
   * @return list of ChangeEvents pertaining to the specified Entity, in timestamp order
   */
  private List<ChangeEvent> getChangeEventsForEntity(SchemaEntityType entityType,
          byte[] namespaceName, byte[] tableName,
          byte[] columnFamily, byte[] columnQualifier) {
    Object[] entityArray = entityIndex.keyComponentSet(entityComponent).toArray();

    int namespaceIndex
            = Arrays.binarySearch(entityArray,
                    ChangeEvent.createEntity(SchemaEntityType.NAMESPACE.getRecordType(),
                            Repository.NAMESPACE_PARENT_FOREIGN_KEY,
                            namespaceName));
    if (namespaceIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.NAMESPACE)) {
      return entityIndex.values(ChangeEvent.createEntity(SchemaEntityType.NAMESPACE.getRecordType(),
              Repository.NAMESPACE_PARENT_FOREIGN_KEY, namespaceName));
    }

    byte[] namespaceForeignKey
            = ((ChangeEvent.Entity) entityArray[namespaceIndex]).getEntityForeignKey().getBytes();
    int tableIndex
            = Arrays.binarySearch(entityArray,
                    ChangeEvent.createEntity(SchemaEntityType.TABLE.getRecordType(),
                            namespaceForeignKey, tableName));
    if (tableIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.TABLE)) {
      return entityIndex.values(ChangeEvent.createEntity(
              SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tableName));
    }

    byte[] tableForeignKey
            = ((ChangeEvent.Entity) entityArray[tableIndex]).getEntityForeignKey().getBytes();
    int colFamilyIndex
            = Arrays.binarySearch(entityArray,
                    ChangeEvent.createEntity(
                            SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey,
                            columnFamily));
    if (colFamilyIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.COLUMN_FAMILY)) {
      return entityIndex.values(ChangeEvent.createEntity(
              SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey, columnFamily));
    }
    byte[] colFamilyForeignKey
            = ((ChangeEvent.Entity)entityArray[colFamilyIndex])
                    .getEntityForeignKey().getBytes();
    int colQualifierIndex
            = Arrays.binarySearch(entityArray,
                    ChangeEvent.createEntity(
                            entityType.getRecordType(), colFamilyForeignKey, columnQualifier));
    if (colQualifierIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.COLUMN_AUDITOR)
            || entityType.equals(SchemaEntityType.COLUMN_DEFINITION)) {
      return entityIndex.values(
              ChangeEvent.createEntity(
                      entityType.getRecordType(), colFamilyForeignKey, columnQualifier));
    }
    return null;
  }

  /**
   * Export the submitted list of ChangeEvent objects to a comma-separated-value (CSV) file, one
   * line per ChangeEvent, with the first line of the file consisting of column headers.
   *
   * @param changeEventList list of ChangeEvent objects returned by one of the "get" methods of the
   * {@code ChangeEventMonitor} class.
   * @param targetFile target file
   * @throws IOException if a remote or network exception occurs
   */
  public static void exportChangeEventListToCsvFile(
          List<ChangeEvent> changeEventList, File targetFile) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(targetFile.toPath(), ENCODING)) {
      if (changeEventList == null || changeEventList.isEmpty()) {
        return;
      }
      writer.write(buildCommaDelimitedString(
              "Timestamp", "Java_Username", "Entity_Type",
              "Namespace", "Table", "Column_Family", "Column_Qualifier",
              "Attribute_Name", "Attribute_Value"));
      writer.newLine();
      for (ChangeEvent ce : changeEventList) {
        writer.write(buildCommaDelimitedString(
                ce.getTimestampAsString(), ce.getUserNameAsString(),
                ce.getEntityType().toString(),
                ce.getNamespaceAsString(), ce.getTableNameAsString(),
                ce.getColumnFamilyAsString(), ce.getColumnQualifierAsString(),
                ce.getAttributeNameAsString(), ce.getAttributeValueAsString()));
        writer.newLine();
      }
    }
  }

  private static String buildCommaDelimitedString(String... strings) {
    StringBuilder stringBuilder = new StringBuilder();
    int stringCount = 0;
    for (String string : strings) {
      stringBuilder.append(string);
      if (++stringCount < strings.length) {
        stringBuilder.append(COMMA);
      }
    }
    return stringBuilder.toString();
  }
}
