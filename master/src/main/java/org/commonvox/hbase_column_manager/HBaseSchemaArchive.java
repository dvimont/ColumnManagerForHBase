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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

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

  @XmlTransient // was @XmlAttribute (Timestamp attribute now written to comment)
  private final String fileTimestamp;
  @XmlElement(name = "hBaseSchemaEntity")
  private final Set<SchemaEntity> hBaseSchemaEntities = new LinkedHashSet<>();
//  @XmlTransient
//  private final String ROOT_ELEMENT = "hBaseSchemaArchive";
  @XmlTransient
  private String namespace;
  @XmlTransient
  private TableName tableName;

  HBaseSchemaArchive() { // default, no-arg constructor required for JAXB processing
    fileTimestamp = new Timestamp(System.currentTimeMillis()).toString();
  }

  HBaseSchemaArchive(String sourceNamespace, TableName sourceTableName, Repository repository)
          throws IOException {
    this();
    this.namespace = sourceNamespace;
    this.tableName = sourceTableName;
    for (MNamespaceDescriptor mnd : repository.getMNamespaceDescriptors()) {
      if (sourceNamespace != null && !sourceNamespace.equals(Bytes.toString(mnd.getName()))) {
        continue;
      }
      SchemaEntity namespaceEntity = new SchemaEntity(mnd);
      hBaseSchemaEntities.add(namespaceEntity);
      for (MTableDescriptor mtd : repository.getMTableDescriptors(mnd.getForeignKey())) {
        if (sourceTableName != null
                && !sourceTableName.getNameAsString().equals(mtd.getNameAsString())) {
          continue;
        }
        SchemaEntity tableEntity = new SchemaEntity(mtd);
        namespaceEntity.addChild(tableEntity);
        for (MColumnDescriptor mcd : mtd.getMColumnDescriptors()) {
          SchemaEntity colFamilyEntity = new SchemaEntity(mcd);
          tableEntity.addChild(colFamilyEntity);
          for (ColumnAuditor colAuditor : mcd.getColumnAuditors()) {
            colFamilyEntity.addChild(new SchemaEntity(colAuditor));
          }
          for (ColumnDefinition colDef : mcd.getColumnDefinitions()) {
            colFamilyEntity.addChild(new SchemaEntity(colDef));
          }
        }
      }
    }
  }

  Set<SchemaEntity> getSchemaEntities() {
    return hBaseSchemaEntities;
  }

  String getArchiveFileTimestampString() {
    return fileTimestamp;
  }

  static void exportToXmlFile(HBaseSchemaArchive hsa, File targetFile)
          throws JAXBException, XMLStreamException, FileNotFoundException {
    XMLStreamWriter xsw = XMLOutputFactory.newFactory()
            .createXMLStreamWriter(new FileOutputStream(targetFile));
    xsw.writeStartDocument();
    xsw.writeComment(HBaseSchemaArchive.class.getSimpleName() + " file generated for "
            + (hsa.namespace == null && hsa.tableName == null ?
                    "full " + Repository.PRODUCT_NAME + " Repository, " : "")
            + (hsa.namespace == null ? "" : "Namespace:[" + hsa.namespace + "], ")
            + (hsa.tableName == null ? "" : "Table:[" + hsa.tableName.getNameAsString() + "], ")
            + "File generated on [" + hsa.fileTimestamp + "]");
    Marshaller marshaller
            = JAXBContext.newInstance(HBaseSchemaArchive.class).createMarshaller();
    // commented out because XMLStreamWriter does NOT support formatted output!!
    //    if (formatted) {
    //      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
    //    }
    marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
    marshaller.marshal(hsa, xsw);
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
