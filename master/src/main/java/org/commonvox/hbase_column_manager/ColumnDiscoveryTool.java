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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Daniel Vimont
 */
public class ColumnDiscoveryTool extends Configured implements Tool  {

  private static final Log LOG = LogFactory.getLog(ColumnDiscoveryTool.class);
  private static final String JOB_NAME_CONF_KEY = "mapreduce.job.name";
  private static final String TABLE_NAME_ARG_KEY = "--sourceTable=";
  String sourceTableNameString = null;

  Job createSubmittableJob(final String[] args) throws IOException {
    if (!parseArguments(args)) {
      return null;
    }

    Job job = Job.getInstance(getConf(),
            getConf().get(JOB_NAME_CONF_KEY, "columnDiscovery_for_table_" + sourceTableNameString));
    TableMapReduceUtil.addDependencyJars(job);

    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don't set to true for MapReduce jobs

    return job;
  }

  private boolean parseArguments (final String[] args) {
    if (args.length < 1) {
      printUsage(null);
      return false;
    }
    if (args[0].startsWith(TABLE_NAME_ARG_KEY)) {
      sourceTableNameString = args[0].substring(TABLE_NAME_ARG_KEY.length());
      return true;
    } else {
      printUsage("Invalid argument '" + args[0] + "'");
      return false;
    }
  }

  private void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: " + ColumnDiscoveryTool.class.getSimpleName() + " "
            + TABLE_NAME_ARG_KEY +"<tablename>");
  }

  /**
   * @param args the command line arguments
   * @throws java.lang.Exception if mapreduce job fails
   */
  public static void main(final String[] args) throws Exception {
    int ret = ToolRunner.run(MConfiguration.create(), new ColumnDiscoveryTool(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = createSubmittableJob(args);
    if (job == null) {
      return 1;
    }
    if (!job.waitForCompletion(true)) {
      LOG.info(ColumnDiscoveryTool.class.getSimpleName() + " mapreduce job failed!");
      return 1;
    }
    return 0;
  }
}
