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
import java.io.IOException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

/**
 * Used to generate an XML Schema (xsd) file which stipulates the layout of
 * valid External Metadata Archive XML documents. The xsd file is auto-generated
 * by the JAXBContext#generateSchema method. File is outputted to the
 * project's root directory in the NetBeans environment.
 */
class XmlSchemaGenerator {
    static final String DEFAULT_OUTPUT_FILE_NAME = "HBaseSchemaArchive.xsd.xml";
    static final String DEFAULT_JAVADOCS_OUTPUT_PATH =
            "src/main/java/org/commonvox/hbase_column_manager/doc-files/";
    static File outputDirectory;
    final String outputFile;

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            outputToFile(DEFAULT_JAVADOCS_OUTPUT_PATH);
        } else {
            outputToFile(args[0]);
        }
    }

    static void outputToFile (String outputPath) throws IOException, JAXBException {
      outputDirectory = new File(outputPath);
      if (!outputDirectory.exists()) {
        outputDirectory.mkdirs();
      }
      new XmlSchemaGenerator(outputPath + DEFAULT_OUTPUT_FILE_NAME).outputSchema();
      System.out.println("XML Schema file <" + DEFAULT_OUTPUT_FILE_NAME
              + "> has been generated and outputted to the following directory: "
              + outputDirectory.getCanonicalPath());
    }

    private XmlSchemaGenerator (String outputFile) throws IOException, JAXBException {
        this.outputFile = outputFile;
    }

    private void outputSchema () throws IOException, JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(HBaseSchemaArchive.class);
        SchemaOutputResolver sor = new SchemaOutputResolverClass();
        jaxbContext.generateSchema(sor);
    }

    private class SchemaOutputResolverClass extends SchemaOutputResolver {
        @Override
        public Result createOutput(String namespaceURI, String suggestedFileName) throws IOException {
            File file = new File(outputFile);
            StreamResult result = new StreamResult(file);
            result.setSystemId(file.toURI().toURL().toString());
            return result;
        }
    }
}
