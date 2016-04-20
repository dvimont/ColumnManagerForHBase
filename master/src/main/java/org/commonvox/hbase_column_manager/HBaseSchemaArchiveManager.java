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

import java.nio.file.Paths;
import java.util.Set;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

/**
 * A container-management class that provides for straightforward JAXB processing (i.e.,
 * XML-formatted serialization and deserialization) of ColumnManager repository metadata.
 * @author Daniel Vimont
 */
class HBaseSchemaArchiveManager {
    private final HBaseSchemaArchive archiveSet;

    HBaseSchemaArchiveManager() {
        archiveSet = new HBaseSchemaArchive();
    }

    private HBaseSchemaArchiveManager(HBaseSchemaArchive archiveSet) {
        this.archiveSet = archiveSet;
    }

    static HBaseSchemaArchiveManager deserializeXmlFile (String sourcePathString, String sourceFileNameString)
            throws JAXBException {
        HBaseSchemaArchive deserializedArchiveSet
                = (HBaseSchemaArchive)
                        JAXBContext.newInstance(HBaseSchemaArchive.class).createUnmarshaller()
                            .unmarshal(Paths.get(sourcePathString, sourceFileNameString).toFile());
        return new HBaseSchemaArchiveManager(deserializedArchiveSet);
    }

    MetadataEntity addMetadataEntity(MetadataEntity mEntity) {
        archiveSet.addMetadataObject(mEntity);
        return mEntity;
    }

    Set<MetadataEntity> getMetadataEntities () {
        return archiveSet.getMetadataObjects();
    }

    String getArchiveFileTimestampString() {
        return archiveSet.getArchiveFileTimestampString();
    }

    void exportToXmlFile (String targetPathString, String targetFileNameString, boolean formatted)
                throws JAXBException {
        Marshaller marshaller
                = JAXBContext.newInstance(HBaseSchemaArchive.class).createMarshaller();
        if (formatted) {
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        }
        marshaller.marshal(archiveSet, Paths.get(targetPathString, targetFileNameString).toFile());
    }

}
