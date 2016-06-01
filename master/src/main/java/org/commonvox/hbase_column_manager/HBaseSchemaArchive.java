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
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * A container class that provides for straightforward JAXB processing (i.e., XML-formatted
 * serialization and deserialization) of ColumnManager repository schema.
 *
 * @author Daniel Vimont
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "hBaseSchemaArchive")
class HBaseSchemaArchive {

  private static final String BLANKS = "                    ";
  private static final int TAB = 3;

  @XmlAttribute
  private final String fileTimestamp;
  @XmlElement(name = "hBaseSchemaEntity")
  private final Set<SchemaEntity> hBaseSchemaEntities = new LinkedHashSet<>();
//  @XmlTransient
//  private final String ROOT_ELEMENT = "hBaseSchemaArchive";

  HBaseSchemaArchive() {
    this.fileTimestamp = new Timestamp(System.currentTimeMillis()).toString();
  }

  SchemaEntity addSchemaEntity(SchemaEntity entity) {
    if (entity == null) {
      return null;
    }
    hBaseSchemaEntities.add(entity);
    return entity;
  }

  Set<SchemaEntity> getSchemaEntities() {
    return hBaseSchemaEntities;
  }

  String getArchiveFileTimestampString() {
    return fileTimestamp;
  }

  static void exportToXmlFile(HBaseSchemaArchive hsa, File targetFile, boolean formatted)
          throws JAXBException {
    Marshaller marshaller
            = JAXBContext.newInstance(HBaseSchemaArchive.class).createMarshaller();
    if (formatted) {
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
    }
    marshaller.marshal(hsa, targetFile);
  }

  static HBaseSchemaArchive deserializeXmlFile(File sourceHsaFile)
          throws JAXBException {
    return (HBaseSchemaArchive)JAXBContext.newInstance(HBaseSchemaArchive.class)
                    .createUnmarshaller().unmarshal(sourceHsaFile);
  }

  static String getSummaryReport(File sourceHsaFile) throws JAXBException {
    HBaseSchemaArchive schemaArchive = HBaseSchemaArchive.deserializeXmlFile(sourceHsaFile);
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("SUMMARY OF external HBase Schema Archive file*\n")
            .append(BLANKS, 0, TAB).append("SOURCE FILE: ")
            .append(sourceHsaFile.getAbsolutePath()).append("\n")
            .append(BLANKS, 0, TAB).append("FILE TIMESTAMP: ")
            .append(schemaArchive.getArchiveFileTimestampString()).append("\n")
            .append(BLANKS, 0, TAB).append("FILE CONTENTS:\n");
    for (SchemaEntity entity : schemaArchive.getSchemaEntities()) {
      stringBuilder.append(appendSchemaEntityDescription(entity, TAB + TAB));
    }
    stringBuilder.append("\n").append(BLANKS, 0, TAB).append("*To examine the XML-formatted"
            + " HBase Schema Archive file in detail, simply open it in a browser or XML editor.");
    return stringBuilder.toString();
  }

  private static StringBuilder appendSchemaEntityDescription(SchemaEntity entity, int indentSpaces) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(BLANKS, 0, indentSpaces).append(entity).append("\n");
    if (entity.getChildren() != null) {
      for (SchemaEntity childEntity : entity.getChildren()) {
        stringBuilder.append(appendSchemaEntityDescription(childEntity, indentSpaces + TAB));
      }
    }
    return stringBuilder;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!(other instanceof HBaseSchemaArchive)) {
      return false;
    }
    return this.hBaseSchemaEntities.equals(((HBaseSchemaArchive)other).hBaseSchemaEntities);
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + Objects.hashCode(this.hBaseSchemaEntities);
    return hash;
  }
}
