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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A <b>ChangeEvent</b> (obtained via a {@code ChangeEventMonitor}'s various
 * {@link ChangeEventMonitor#getAllChangeEvents() get methods}) contains metadata pertaining to a
 * structural change made to a component of a
 * <a href="package-summary.html#config">ColumnManager-included</a> <i>Namespace</i>
 * or <i>Table</i>; for example, a change to the "durability" setting of a <i>Table</i>
 * or the "maxVersions" setting of a <i>Table</i>'s <i>Column Family</i>
 * is captured in the ColumnManager repository as a {@code ChangeEvent}. When
 * <a href="package-summary.html#activate">ColumnManager is activated</a>, such {@code ChangeEvents}
 * are comprehensively tracked in the ColumnManager repository for all <i>Namespace</i>s and
 * <i>Table</i>s (and their components) that are
 * <a href="package-summary.html#config">included in ColumnManager processing</a>.
 *
 * @author Daniel Vimont
 */
public class ChangeEvent implements Comparable<ChangeEvent> {

  // Row-level metadata: Each row in repository pertains to a user entity
  //   (e.g., a table, or a column family) and tracks all changes to all
  //   of that entity's attributes
  private Entity entity; // corresponds to RowId in repository table

  // Column-level metadata: Each column pertains to one attribute of a user entity
  private AttributeName attributeName; // name of user entity's attribute (e.g., table's "DURABILITY")

  // Cell-level metadata: Each cell pertains to a new value assigned to an attribute of a user entity
  private Timestamp timestamp; // key of cell's key/value pair
  private AttributeValue attributeValue; // new value assigned to attribute; value of cell's key/value pair

  // Username associated with cell's timestamp
  private UserName userName; // pulled from row's "user.name" column/cell with matching timestamp

  ChangeEvent(byte entityType, byte[] parentForeignKey, byte[] entityName,
          byte[] entityForeignKey, byte[] attributeName, long timestamp,
          byte[] attributeValue, byte[] userName) {
    this.setEntity(entityType, parentForeignKey, entityName, entityForeignKey);
    this.setAttributeName(attributeName);
    this.setTimestamp(timestamp);
    this.setAttributeValue(attributeValue);
    this.setUserName(userName);
  }

  private ChangeEvent() {
  }

  Entity getEntity() {
    return entity;
  }

  private void setEntity(byte entityType, byte[] parentForeignKey, byte[] entityName,
          byte[] entityForeignKey) {
    this.entity = new Entity(entityType, parentForeignKey, entityName);
    this.entity.setEntityForeignKey(entityForeignKey);
  }

  void setEntity(Entity entity) {
    this.entity = entity;
  }

  /**
   * Get the {@link SchemaEntityType} of the Entity to which the {@code ChangeEvent} pertains.
   *
   * @return {@link SchemaEntityType} of the Entity to which the {@code ChangeEvent} pertains.
   */
  SchemaEntityType getEntityType() {
    return SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP
            .get(this.entity.getEntityRecordType().getBytes()[0]);
  }

  /**
   * Get the type of the Entity to which the {@code ChangeEvent} pertains (e.g., "Table",
   * "ColumnFamily", etc.).
   *
   * @return type of the Entity to which the {@code ChangeEvent} pertains.
   */
  public String getEntityTypeAsString() {
    return SchemaEntityType.ENTITY_TYPE_BYTE_TO_ENUM_MAP
            .get(this.entity.getEntityRecordType().getBytes()[0]).toString();
  }

  byte[] getParentForeignKey() {
    return this.entity.getParentForeignKey().getBytes();
  }

  ParentForeignKey getParentForeignKeyObject() {
    return this.entity.getParentForeignKey();
  }

  /**
   * Get the name of the Entity to which the {@code ChangeEvent} pertains.
   *
   * @return Name of the Entity to which the {@code ChangeEvent} pertains.
   */
  public byte[] getEntityName() {
    return this.entity.getEntityName().getBytes();
  }

  /**
   * Get the name of the Entity to which the {@code ChangeEvent} pertains.
   *
   * @return Name of the Entity to which the {@code ChangeEvent} pertains.
   */
  public String getEntityNameAsString() {
    return Repository.getPrintableString(this.entity.getEntityName().getBytes());
  }

  EntityName getEntityNameObject() {
    return this.entity.getEntityName();
  }

  /**
   * Get the name of the <i>Namespace</i> associated with the Entity to which the
   * {@code ChangeEvent} pertains.
   *
   * @return Name of the <i>Namespace</i> associated with the Entity to which the
   * {@code ChangeEvent} pertains.
   */
  public String getNamespaceAsString() {
    return this.entity.getNamespaceAsString();
  }

  /**
   * Get the name of the <i>Table</i> associated with the Entity to which the {@code ChangeEvent}
   * pertains (if applicable).
   *
   * @return Name of the <i>Table</i> associated with the Entity to which the {@code ChangeEvent}
   * pertains (if applicable).
   */
  public String getTableNameAsString() {
    return this.entity.getTableNameAsString();
  }

  /**
   * Get the name of the <i>Column Family</i> associated with the Entity to which the
   * {@code ChangeEvent} pertains (if applicable).
   *
   * @return Name of the <i>Column Family</i> associated with the Entity to which the
   * {@code ChangeEvent} pertains (if applicable).
   */
  public String getColumnFamilyAsString() {
    return this.entity.getColumnFamilyAsString();
  }

  /**
   * Get the <i>Column Qualifier</i> associated with the Entity to which the {@code ChangeEvent}
   * pertains (if applicable).
   *
   * @return The <i>Column Qualifier</i> associated with the Entity to which the {@code ChangeEvent}
   * pertains (if applicable).
   */
  public String getColumnQualifierAsString() {
    return this.entity.getColumnQualifierAsString();
  }

  byte[] getEntityForeignKey() {
    return this.entity.getEntityForeignKey().getBytes();
  }

  EntityForeignKey getEntityForeignKeyObject() {
    return this.entity.getEntityForeignKey();
  }

  /**
   * Get the timestamp of the {@code ChangeEvent}.
   *
   * @return The timestamp of the {@code ChangeEvent}.
   */
  public long getTimestamp() {
    return timestamp.getLong();
  }

  /**
   * Get the timestamp of the {@code ChangeEvent}.
   *
   * @return The timestamp of the {@code ChangeEvent}.
   */
  public String getTimestampAsString() {
    return String.valueOf(timestamp.getLong());
  }

  Timestamp getTimestampObject() {
    return timestamp;
  }

  private void setTimestamp(long timestamp) {
    this.timestamp = new Timestamp(timestamp);
  }

  /**
   * Get the user name associated with the {@code ChangeEvent} (as designated by the Java
   * "user.name" property in effect within the session that made the change).
   *
   * @return The user name associated with the {@code ChangeEvent}.
   */
  public byte[] getUserName() {
    return userName.getBytes();
  }

  /**
   * Get the user name associated with the {@code ChangeEvent} (as designated by the Java
   * "user.name" property in effect within the session that made the change).
   *
   * @return The user name associated with the {@code ChangeEvent}.
   */
  public String getUserNameAsString() {
    return Repository.getPrintableString(userName.getBytes());
  }

  UserName getUserNameObject() {
    return userName;
  }

  private void setUserName(byte[] userName) {
    this.userName = new UserName(userName);
  }

  /**
   * Get the name of the attribute to which the {@code ChangeEvent} pertains: for example, a
   * <i>Table</i> attribute name could be "Value__DURABILITY", corresponding to the <i>Table</i>'s
   * durability setting.
   *
   * @return Attribute name.
   */
  public byte[] getAttributeName() {
    return attributeName.getBytes();
  }

  /**
   * Get the name of the attribute to which the {@code ChangeEvent} pertains: for example, a
   * <i>Table</i> attribute name could be "Value__DURABILITY", corresponding to the <i>Table</i>'s
   * durability setting.
   *
   * @return Attribute name.
   */
  public String getAttributeNameAsString() {
    return Repository.getPrintableString(attributeName.getBytes());
  }

  AttributeName getAttributeNameObject() {
    return attributeName;
  }

  private void setAttributeName(byte[] attributeName) {
    this.attributeName = new AttributeName(attributeName);
  }

  /**
   * Get the new value of the attribute to which the {@code ChangeEvent} pertains: for example, a
   * <i>Table</i> attribute named "Value__DURABILITY" might have been set to the value "SKIP_WAL".
   *
   * @return Attribute value.
   */
  public byte[] getAttributeValue() {
    return attributeValue.getBytes();
  }

  /**
   * Get the new value of the attribute to which the {@code ChangeEvent} pertains: for example, a
   * <i>Table</i> attribute named "Value__DURABILITY" might have been set to the value "SKIP_WAL".
   *
   * @return Attribute value.
   */
  public String getAttributeValueAsString() {
    return Repository.getPrintableString(attributeValue.getBytes());
  }

  AttributeValue getAttributeValueObject() {
    return attributeValue;
  }

  private void setAttributeValue(byte[] attributeValue) {
    this.attributeValue = new AttributeValue(attributeValue);
  }

  @Override
  public String toString() {
    return "EntityType:<" + getEntityTypeAsString() + "> EntityName:<" + getEntityNameAsString()
            + "> Timestamp:<" + getTimestamp() + "> AttributeName:<" + getAttributeNameAsString()
            + "> AttributeValue:<" + getAttributeValueAsString() + ">";
  }

  @Override
  public int compareTo(ChangeEvent other) {
    int result = this.timestamp.compareTo(other.timestamp);
    if (result == 0) {
      result = this.userName.compareTo(other.userName);
    }
    if (result == 0) {
      result = this.entity.compareTo(other.entity);
    }
    if (result == 0) {
      result = this.attributeName.compareTo(other.attributeName);
    }
    if (result == 0) {
      result = this.attributeValue.compareTo(other.attributeValue);
    }
    return result;
  }

  static class Entity implements Comparable<Entity> {

    private final EntityType entityType; // part of Entity unique identifier (rowId)
    private final ParentForeignKey parentForeignKey; // part of Entity unique identifier (rowId)
    private final EntityName entityName; // part of Entity unique identifier (rowId)
    private EntityForeignKey entityForeignKey;
    // Entity pointers for denormalization
    private Entity namespace = null;
    private Entity table = null;
    private Entity colFamily = null;
    private Entity column = null;

    Entity(byte entityType, byte[] parentForeignKey, byte[] entityName) {
      this.entityType = new EntityType(entityType);
      this.parentForeignKey = new ParentForeignKey(parentForeignKey);
      this.entityName = new EntityName(entityName);
    }

    EntityType getEntityRecordType() {
      return entityType;
    }

    ParentForeignKey getParentForeignKey() {
      return parentForeignKey;
    }

    EntityName getEntityName() {
      return entityName;
    }

    void setEntityForeignKey(byte[] entityForeignKey) {
      this.entityForeignKey = new EntityForeignKey(entityForeignKey);
    }

    EntityForeignKey getEntityForeignKey() {
      return entityForeignKey;
    }

    void setNamespaceEntity(Entity namespaceEntity) {
      this.namespace = namespaceEntity;
    }

    String getNamespaceAsString() {
      return extractEntityName(this.namespace);
    }

    void setTableEntity(Entity tableEntity) {
      this.table = tableEntity;
    }

    String getTableNameAsString() {
      return extractEntityName(this.table);
    }

    void setColumnFamilyEntity(Entity colFamilyEntity) {
      this.colFamily = colFamilyEntity;
    }

    String getColumnFamilyAsString() {
      return extractEntityName(this.colFamily);
    }

    void setColumnQualifierEntity(Entity columnEntity) {
      this.column = columnEntity;
    }

    String getColumnQualifierAsString() {
      return extractEntityName(this.column);
    }

    private String extractEntityName(Entity entity) {
      return (entity == null) ? "" : entity.getEntityName().toString();
    }

    @Override
    public int compareTo(Entity other) {
      int comparison = this.entityType.compareTo(other.entityType);
      if (comparison != 0) {
        return comparison;
      }
      comparison = this.entityName.compareTo(other.entityName);
      if (comparison != 0) {
        return comparison;
      }
      return this.parentForeignKey.compareTo(other.parentForeignKey);
    }

    @Override
    public boolean equals (Object other) {
      if (!this.getClass().equals(other.getClass())) {
        return false;
      }
      return (compareTo((Entity)other) == 0);
    }

    @Override
    public String toString() {
      return entityType + " EntityName:<" + entityName + ">";
    }
  }

  private static class BytesContainer implements Comparable<BytesContainer> {

    private final byte[] bytes;

    BytesContainer(byte[] bytes) {
      if (bytes == null) {
        this.bytes = new byte[0];
      } else {
        this.bytes = bytes;
      }
    }

    byte[] getBytes() {
      return bytes;
    }

    @Override
    public int compareTo(BytesContainer other) {
      return Bytes.compareTo(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object other) {
      if (!this.getClass().equals(other.getClass())) {
        return false;
      }
      return compareTo((BytesContainer)other) == 0;
    }
  }

  static class EntityType extends BytesContainer {

    EntityType(byte entityRecordType) {
      super(new byte[]{entityRecordType});
    }

    @Override
    public String toString() {
      return "EntityType:<" + Bytes.toString(this.getBytes()) + ">";
    }
  }

  static class ParentForeignKey extends BytesContainer {

    ParentForeignKey(byte[] bytes) {
      super(bytes);
    }
  }

  static class EntityName extends BytesContainer {

    EntityName(byte[] bytes) {
      super(bytes);
    }

    @Override
    public String toString() {
      return Bytes.toString(this.getBytes());
    }
  }

  static class EntityForeignKey extends BytesContainer {

    EntityForeignKey(byte[] bytes) {
      super(bytes);
    }
  }

  static class AttributeName extends BytesContainer {

    AttributeName(byte[] bytes) {
      super(bytes);
    }
  }

  static class Timestamp extends BytesContainer {

    Timestamp(long timestamp) {
      super(Bytes.toBytes(timestamp));
    }

    long getLong() {
      return Bytes.toLong(this.getBytes());
    }
  }

  static class AttributeValue extends BytesContainer {

    AttributeValue(byte[] bytes) {
      super(bytes);
    }
  }

  static class UserName extends BytesContainer {

    UserName(byte[] bytes) {
      super(bytes);
    }

    UserName(String userName) {
      super(Bytes.toBytes(userName));
    }
  }
}
