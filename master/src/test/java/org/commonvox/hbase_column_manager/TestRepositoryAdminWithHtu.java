/*
 * Copyright 2016 Daniel Vimont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonvox.hbase_column_manager;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This module should be completed and incorporated in the package in implementations
 * compatible with HBase 2.0 and later. In pre-2.0 implementations, MHBaseTestingUtility cannot
 * function.
 *
 * @author Daniel Vimont
 */
public class TestRepositoryAdminWithHtu {
  // private static final HBaseTestingUtility TEST_UTIL = new MHBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
//    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
//    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStaticMethods() throws IOException {

  }

  /**
   * @param args the command line arguments
   * @throws java.lang.Exception
   */
  public static void main(String[] args) throws Exception {

    beforeClass();

    //new TestRepositoryAdminWithHtu().testStaticMethods();

    afterClass();
  }

}
