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
import java.util.LinkedHashSet;
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

  @XmlAttribute
  private final String fileTimestamp;
  @XmlElement(name = "hBaseSchemaEntity")
  private final Set<SchemaEntity> hBaseSchemaEntities = new LinkedHashSet<>();
  @XmlTransient
  private final String ROOT_ELEMENT = "hBaseSchemaArchive";

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

  static void exportToXmlFile(HBaseSchemaArchive archiveSet, File targetFile, boolean formatted)
          throws JAXBException {
    Marshaller marshaller
            = JAXBContext.newInstance(HBaseSchemaArchive.class).createMarshaller();
    if (formatted) {
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
    }
    marshaller.marshal(archiveSet, targetFile);
  }

  static HBaseSchemaArchive deserializeXmlFile(File sourceFile)
          throws JAXBException {
    return (HBaseSchemaArchive)JAXBContext.newInstance(HBaseSchemaArchive.class)
                    .createUnmarshaller().unmarshal(sourceFile);
  }
}
