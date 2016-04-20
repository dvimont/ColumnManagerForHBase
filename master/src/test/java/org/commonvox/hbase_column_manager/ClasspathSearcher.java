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
import java.util.StringTokenizer;

public class ClasspathSearcher {

  public static boolean fileFoundOnClassPath(final String fileName) {

    final StringTokenizer classpathTokens
            = new StringTokenizer(System.getProperty("java.class.path"), System.getProperty("path.separator"));
    while (classpathTokens.hasMoreTokens()) {
      final File directoryOrJar = new File(classpathTokens.nextToken());
      final File absoluteDirectoryOrJar = directoryOrJar.getAbsoluteFile();
      if (absoluteDirectoryOrJar.isFile()) {
        if (new File(absoluteDirectoryOrJar.getParent(), fileName).exists()) {
          return true;
        }
      } else {
        if (fileFoundInDirectory(directoryOrJar, fileName)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean fileFoundInDirectory(final File directory, final String fileName) {

    if (new File(directory, fileName).exists()) {
      return true;
    }
    for (File directoryOrFile : directory.listFiles()) {
      if (directoryOrFile.isDirectory()) {
        if (fileFoundInDirectory(directoryOrFile, fileName)) {
          return true;
        }
      }
    }
    return false;
  }
}
