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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A <b>ChangeEventMonitor</b> (obtained via a {@code RepositoryAdmin}'s
 * {@link RepositoryAdmin#getChangeEventMonitor() getChangeEventMonitor} method) provides various
 * {@link ChangeEventMonitor#getAllChangeEvents() get methods} by which lists of
 * {@link ChangeEvent}s may be obtained (grouped and ordered in various ways), and the class
 * provides a static {@link #exportChangeEventListToCsvFile(java.util.Collection, java.io.File)
 * convenience method} for outputting a list of {@code ChangeEvent}s to a CSV file.
 *
 * @author Daniel Vimont
 */
public class ChangeEventMonitor {
  private final static char COMMA = ',';
  private Set<ChangeEvent.Entity> entitySet = new TreeSet<>();
  private final Set<ChangeEvent> changeEventsByTimestamp = new TreeSet<>();
  private final Set<ChangeEvent> changeEventsByUser = new TreeSet<>(
          new Comparator<ChangeEvent>() {
            @Override
            public int compare(ChangeEvent ce1, ChangeEvent ce2) {
              int result = ce1.getUserNameObject().compareTo(ce2.getUserNameObject());
              if (result == 0) {
                result = ce1.getTimestampObject().compareTo(ce2.getTimestampObject());
              }
              if (result == 0) {
                result = ce1.getEntity().compareTo(ce2.getEntity());
              }
              if (result == 0) {
                result = ce1.getAttributeNameObject().compareTo(ce2.getAttributeNameObject());
              }
              if (result == 0) {
                result = ce1.getAttributeValueObject().compareTo(ce2.getAttributeValueObject());
              }
              return result;
            }
          }
  );
  private Set<ChangeEvent> changeEventsByEntity = new TreeSet<>(
          new Comparator<ChangeEvent>() {
            @Override
            public int compare(ChangeEvent ce1, ChangeEvent ce2) {
              int result = ce1.getEntity().compareTo(ce2.getEntity());
              if (result == 0) {
                result = ce1.getTimestampObject().compareTo(ce2.getTimestampObject());
              }
              if (result == 0) {
                result = ce1.getAttributeNameObject().compareTo(ce2.getAttributeNameObject());
              }
              if (result == 0) {
                result = ce1.getAttributeValueObject().compareTo(ce2.getAttributeValueObject());
              }
              return result;
            }
          }
  );
  private final static Charset ENCODING = StandardCharsets.UTF_8;

  ChangeEventMonitor(Table repositoryTable) throws IOException {
    try (ResultScanner rows = repositoryTable.getScanner(new Scan().setMaxVersions())) {
      for (Result row : rows) {
        byte[] rowId = row.getRow();
        byte entityType = rowId[0];
        byte[] entityName = Repository.extractNameFromRowId(rowId);
        byte[] parentForeignKey = Repository.extractParentForeignKeyFromRowId(rowId);
        byte[] entityForeignKey = row.getValue(Repository.REPOSITORY_CF, Repository.FOREIGN_KEY_COLUMN);
        Map<Long, byte[]> userNameKeyedByTimestampMap = new HashMap<>();
        for (Cell userNameCell : row.getColumnCells(
                Repository.REPOSITORY_CF, Repository.JAVA_USERNAME_PROPERTY_KEY)) {
          userNameKeyedByTimestampMap.put(userNameCell.getTimestamp(),
                  Bytes.getBytes(CellUtil.getValueBufferShallowCopy(userNameCell)));
        }
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cfEntry : row.getMap().entrySet()) {
          for (Map.Entry<byte[], NavigableMap<Long, byte[]>> colEntry
                  : cfEntry.getValue().entrySet()) {
            byte[] attributeName = colEntry.getKey();
            // bypass ColumnManager-maintained columns
            if (Bytes.equals(attributeName, Repository.FOREIGN_KEY_COLUMN)
                    || Bytes.equals(attributeName, Repository.JAVA_USERNAME_PROPERTY_KEY)
                    || Bytes.equals(attributeName, Repository.MAX_VALUE_COLUMN_NAME)) {
              continue;
            }
            for (Map.Entry<Long, byte[]> cellEntry : colEntry.getValue().entrySet()) {
              long timestamp = cellEntry.getKey();
              byte[] attributeValue = cellEntry.getValue();
              byte[] userName = userNameKeyedByTimestampMap.get(timestamp);
              ChangeEvent changeEvent = new ChangeEvent(entityType, parentForeignKey, entityName,
                      entityForeignKey, attributeName,
                      timestamp, attributeValue, userName);
              changeEventsByEntity.add(changeEvent);
              entitySet.add(changeEvent.getEntity());
            }
          }
        }
      }
    }
    denormalize();
  }

  private void denormalize() {
    // use complete list of Entities to build entityForeignKeyMap for subsequent "look-ups"
    Map<byte[], ChangeEvent.Entity> entityForeignKeyMap = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    for (ChangeEvent.Entity entity : entitySet) {
      entityForeignKeyMap.put(entity.getEntityForeignKey().getBytes(), entity);
    }

    // "denormalize" each ChangeEvent by adding namespace, table, etc. to each one
    Set<ChangeEvent> denormalizedChangeEventsByEntity
            = new TreeSet<>(((TreeSet<ChangeEvent>)changeEventsByEntity).comparator());
    Set<ChangeEvent.Entity> denormalizedEntitySet = new TreeSet<>();
    ChangeEvent.Entity denormalizedEntity = null;
    for (ChangeEvent event : changeEventsByEntity) {
      ChangeEvent.Entity entity = event.getEntity();
      if (denormalizedEntity == null || entity.compareTo(denormalizedEntity) != 0) {
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
        denormalizedEntitySet.add(denormalizedEntity);
      }
      event.setEntity(denormalizedEntity);
      changeEventsByTimestamp.add(event);
      changeEventsByUser.add(event);
      denormalizedChangeEventsByEntity.add(event);
    }
    changeEventsByEntity = denormalizedChangeEventsByEntity;
    entitySet = denormalizedEntitySet;
  }

  /**
   * Get a Set of all {@link ChangeEvent}s in the ColumnManager repository in the default (timestamp)
   * order.
   *
   * @return complete list of {@link ChangeEvent}s in timestamp order
   */
  public Set<ChangeEvent> getAllChangeEvents() {
    return changeEventsByTimestamp;
  }

  /**
   * Get a Set of all {@link ChangeEvent}s in the ColumnManager repository, ordered by user name (as
   * designated by the Java "user.name" property in effect within a session as a change was made).
   *
   * @return complete list of {@link ChangeEvent}s in user-name and timestamp order
   */
  public Set<ChangeEvent> getAllChangeEventsByUserName() {
    return changeEventsByUser;
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to a specific user name (as designated by
   * the Java "user.name"
   * property in effect within a session as a change was made), in timestamp order.
   *
   * @param userName value of Java "user.name" property in effect when change was made
   * @return Set of {@link ChangeEvent}s pertaining to the user name, in timestamp order
   */
  public Set<ChangeEvent> getChangeEventsForUserName(String userName) {
    Set<ChangeEvent> changeEventsForUser = new LinkedHashSet<>();
    boolean userFound = false;
    ChangeEvent.UserName userNameObject = new ChangeEvent.UserName(userName);
    for (ChangeEvent ce : changeEventsByUser) {
      if (ce.getUserNameObject().equals(userNameObject)) {
        userFound = true;
        changeEventsForUser.add(ce);
      } else if (userFound) {
        break;
      }
    }
    return changeEventsForUser;
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.NamespaceDescriptor Namespace}, in timestamp order.
   *
   * @param namespaceName Namespace name
   * @param includeChildEntities if {@code true} {@link ChangeEvent}s for <i>Tables</i> and
   * all table components in the Namespace will be included in returned Set
   * @return Set of {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.NamespaceDescriptor Namespace}
   */
  public Set<ChangeEvent> getChangeEventsForNamespace(
          byte[] namespaceName, boolean includeChildEntities) {
    return getChangeEventsForEntity(
            SchemaEntityType.NAMESPACE, namespaceName, null, null, null, includeChildEntities);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}, in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param includeChildEntities if {@code true} {@link ChangeEvent}s for <i>Column Families</i>,
   * {@link ColumnAuditor}s, and {@link ColumnDefinition}s will be included in returned Set
   * @return Set of {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}
   */
  public Set<ChangeEvent> getChangeEventsForTable(
          TableName tableName, boolean includeChildEntities) {
    return getChangeEventsForEntity(SchemaEntityType.TABLE, tableName.getNamespace(),
            tableName.getName(), null, null, includeChildEntities);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}, in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param attributeName name of attribute
   * @return Set of {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HTableDescriptor Table}
   */
  public Set<ChangeEvent> getChangeEventsForTableAttribute(
          TableName tableName, String attributeName) {
    Set<ChangeEvent> tableEvents = getChangeEventsForTable(tableName, false);
    Set<ChangeEvent> attributeEvents = new LinkedHashSet<>();
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
   * @param includeChildEntities if {@code true} {@link ChangeEvent}s for {@link ColumnAuditor}s
   * and {@link ColumnDefinition}s will be included in returned Set
   * @return Set of {@link ChangeEvent}s pertaining to the specified
   * {@link org.apache.hadoop.hbase.HColumnDescriptor Column Family}
   */
  public Set<ChangeEvent> getChangeEventsForColumnFamily(
          TableName tableName, byte[] columnFamily, boolean includeChildEntities) {
    return getChangeEventsForEntity(SchemaEntityType.COLUMN_FAMILY, tableName.getNamespace(),
            tableName.getName(), columnFamily, null, includeChildEntities);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HColumnDescriptor Column Family}, in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param columnFamily name of Column Family
   * @param attributeName name of attribute
   * @return Set of {@link ChangeEvent}s pertaining to the specified Attribute of the specified
   * {@link org.apache.hadoop.hbase.HColumnDescriptor Column Family}
   */
  public Set<ChangeEvent> getChangeEventsForColumnFamilyAttribute(
          TableName tableName, byte[] columnFamily, String attributeName) {
    Set<ChangeEvent> cfEvents = getChangeEventsForColumnFamily(tableName, columnFamily, false);
    Set<ChangeEvent> attributeEvents = new LinkedHashSet<>();
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
   * @return Set of {@link ChangeEvent}s pertaining to the specified {@link ColumnDefinition}
   */
  public Set<ChangeEvent> getChangeEventsForColumnDefinition(
          TableName tableName, byte[] columnFamily, byte[] columnQualifier) {
    return getChangeEventsForEntity(SchemaEntityType.COLUMN_DEFINITION, tableName.getNamespace(),
            tableName.getName(), columnFamily, columnQualifier, false);
  }

  /**
   * Get the {@link ChangeEvent}s pertaining to the specified {@link ColumnAuditor},
   * in timestamp order.
   *
   * @param tableName {@link org.apache.hadoop.hbase.TableName TableName} object
   * @param columnFamily name of Column Family
   * @param columnQualifier qualifier that identifies the {@link ColumnAuditor}
   * @return Set of {@link ChangeEvent}s pertaining to the specified {@link ColumnAuditor}
   */
  public Set<ChangeEvent> getChangeEventsForColumnAuditor(
          TableName tableName, byte[] columnFamily, byte[] columnQualifier) {
    return getChangeEventsForEntity(SchemaEntityType.COLUMN_AUDITOR, tableName.getNamespace(),
            tableName.getName(), columnFamily, columnQualifier, false);
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
   * @param includeChildEntities
   * @return Set of ChangeEvents pertaining to the specified Entity, in timestamp order
   */
  private Set<ChangeEvent> getChangeEventsForEntity(SchemaEntityType entityType,
          byte[] namespaceName, byte[] tableName,
          byte[] columnFamily, byte[] columnQualifier, boolean includeChildEntities) {

    ChangeEvent.Entity[] entityArray = entitySet.toArray(new ChangeEvent.Entity[entitySet.size()]);

    ChangeEvent.Entity namespaceEntity = new ChangeEvent.Entity(
            SchemaEntityType.NAMESPACE.getRecordType(),
            Repository.NAMESPACE_PARENT_FOREIGN_KEY, namespaceName);
    int namespaceIndex = Arrays.binarySearch(entityArray, namespaceEntity);
    if (namespaceIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.NAMESPACE)) {
      Set<ChangeEvent> returnedChangeEvents = getChangeEventsForEntity(namespaceEntity);
      if (includeChildEntities) {
        for (ChangeEvent.Entity entity : entityArray) {
          if (entity.getEntityRecordType().getByte() == SchemaEntityType.TABLE.getRecordType()
                  && Bytes.equals(entity.getNamespace(), namespaceName)) {
            returnedChangeEvents.addAll(getChangeEventsForEntity(SchemaEntityType.TABLE,
                    namespaceName, entity.getEntityName().getBytes(),
                    null, null, includeChildEntities));
          }
        }
      }
      return returnedChangeEvents;
    }

    byte[] namespaceForeignKey = entityArray[namespaceIndex].getEntityForeignKey().getBytes();
    ChangeEvent.Entity tableEntity = new ChangeEvent.Entity(
            SchemaEntityType.TABLE.getRecordType(), namespaceForeignKey, tableName);
    int tableIndex = Arrays.binarySearch(entityArray, tableEntity);
    if (tableIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.TABLE)) {
      Set<ChangeEvent> returnedChangeEvents = getChangeEventsForEntity(tableEntity);
      if (includeChildEntities) {
        for (ChangeEvent.Entity entity : entityArray) {
          if (entity.getEntityRecordType().getByte()
                  == SchemaEntityType.COLUMN_FAMILY.getRecordType()
                  && Bytes.equals(entity.getNamespace(), namespaceName)
                  && Bytes.equals(entity.getTableName(), tableName)) {
            returnedChangeEvents.addAll(getChangeEventsForEntity(SchemaEntityType.COLUMN_FAMILY,
                    namespaceName, tableName, entity.getEntityName().getBytes(),
                    null, includeChildEntities));
          }
        }
      }
      return returnedChangeEvents;
    }

    byte[] tableForeignKey = entityArray[tableIndex].getEntityForeignKey().getBytes();
    ChangeEvent.Entity colFamilyEntity = new ChangeEvent.Entity(
            SchemaEntityType.COLUMN_FAMILY.getRecordType(), tableForeignKey, columnFamily);
    int colFamilyIndex = Arrays.binarySearch(entityArray, colFamilyEntity);
    if (colFamilyIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.COLUMN_FAMILY)) {
      Set<ChangeEvent> returnedChangeEvents = getChangeEventsForEntity(colFamilyEntity);
      if (includeChildEntities) {
        for (ChangeEvent.Entity entity : entityArray) {
          if (entity.getEntityRecordType().getByte()
                      == SchemaEntityType.COLUMN_AUDITOR.getRecordType()
                  && Bytes.equals(entity.getNamespace(), namespaceName)
                  && Bytes.equals(entity.getTableName(), tableName)
                  && Bytes.equals(entity.getColumnFamily(), columnFamily)) {
            returnedChangeEvents.addAll(getChangeEventsForEntity(SchemaEntityType.COLUMN_AUDITOR,
                    namespaceName, tableName, entity.getEntityName().getBytes(),
                    null, includeChildEntities));
          }
          if (entity.getEntityRecordType().getByte()
                      == SchemaEntityType.COLUMN_DEFINITION.getRecordType()
                  && Bytes.equals(entity.getNamespace(), namespaceName)
                  && Bytes.equals(entity.getTableName(), tableName)
                  && Bytes.equals(entity.getColumnFamily(), columnFamily)) {
            returnedChangeEvents.addAll(getChangeEventsForEntity(SchemaEntityType.COLUMN_DEFINITION,
                    namespaceName, tableName, entity.getEntityName().getBytes(),
                    null, includeChildEntities));
          }
        }
      }
      return returnedChangeEvents;
    }

    byte[] colFamilyForeignKey = entityArray[colFamilyIndex].getEntityForeignKey().getBytes();
    ChangeEvent.Entity colQualifierEntity = new ChangeEvent.Entity(
            entityType.getRecordType(), colFamilyForeignKey, columnQualifier);
    int colQualifierIndex = Arrays.binarySearch(entityArray, colQualifierEntity);
    if (colQualifierIndex < 0) {
      return null;
    }
    if (entityType.equals(SchemaEntityType.COLUMN_AUDITOR)
            || entityType.equals(SchemaEntityType.COLUMN_DEFINITION)) {
      return getChangeEventsForEntity(colQualifierEntity);
    }
    return null;
  }

  private Set<ChangeEvent> getChangeEventsForEntity(ChangeEvent.Entity entity) {
    Set<ChangeEvent> changeEventsForEntity = new LinkedHashSet<>();
    boolean entityFound = false;
    for (ChangeEvent ce : changeEventsByEntity) {
      if (ce.getEntity().equals(entity)) {
        entityFound = true;
        changeEventsForEntity.add(ce);
      } else if (entityFound) {
        break;
      }
    }
    return changeEventsForEntity;
  }

  private enum ReportHeader {
    TIMESTAMP, JAVA_USERNAME, ENTITY_TYPE, NAMESPACE, TABLE, COLUMN_FAMILY, COLUMN_QUALIFIER,
    ATTRIBUTE_NAME, ATTRIBUTE_VALUE }

  /**
   * Export the submitted list of ChangeEvent objects to a comma-separated-value (CSV) file, one
   * line per ChangeEvent, with the first non-comment line of the file consisting of column headers.
   *
   * @param changeEvents list of ChangeEvent objects returned by one of the "get" methods of the
   * {@code ChangeEventMonitor} class.
   * @param targetFile target file
   * @throws IOException if a remote or network exception occurs
   */
  public static void exportChangeEventListToCsvFile(
          Collection<ChangeEvent> changeEvents, File targetFile) throws IOException {
    exportChangeEventListToCsvFile(changeEvents, targetFile, "");
  }

  static void exportChangeEventListToCsvFile(
          Collection<ChangeEvent> changeEvents, File targetFile, String headerDetail)
          throws IOException {
    CSVFormat csvFormat = CSVFormat.DEFAULT.withRecordSeparator("\n").withCommentMarker('#')
            .withHeader(ReportHeader.class);
    try (CSVPrinter csvPrinter = csvFormat.withHeaderComments(
            "List of ChangeEvents in " + Repository.PRODUCT_NAME + " repository " + headerDetail
                    + "-- Exported to CSV by " + Repository.PRODUCT_NAME + ":"
                    + ChangeEventMonitor.class.getSimpleName(),
            new Date()).print(new FileWriter(targetFile))) {
      if (changeEvents == null) {
        return;
      }
      for (ChangeEvent ce : changeEvents) {
        csvPrinter.print(ce.getTimestampAsString());
        csvPrinter.print(ce.getUserNameAsString());
        csvPrinter.print(ce.getEntityType().toString());
        csvPrinter.print(ce.getNamespaceAsString());
        csvPrinter.print(ce.getTableNameAsString());
        csvPrinter.print(ce.getColumnFamilyAsString());
        csvPrinter.print(ce.getColumnQualifierAsString());
        csvPrinter.print(ce.getAttributeNameAsString());
        csvPrinter.print(ce.getAttributeValueAsString());
        csvPrinter.println();
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
