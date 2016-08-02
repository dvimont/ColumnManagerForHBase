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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Daniel Vimont
 */
class ColumnDiscoveryTool extends Configured implements Tool  {

  private static final Log LOG = LogFactory.getLog(ColumnDiscoveryTool.class);
  String sourceTableNameString = null;
  boolean includeAllCells = false;

  Job createSubmittableJob(final String[] args) throws IOException {
    if (!parseArguments(args)) {
      return null;
    }
    getConf().setBoolean(Repository.MAP_SPECULATIVE_CONF_KEY, true); // no redundant processing
    getConf().set(Repository.TABLE_NAME_CONF_KEY, sourceTableNameString);
    Job job = Job.getInstance(
            getConf(), getConf().get(Repository.JOB_NAME_CONF_KEY, sourceTableNameString));
    TableMapReduceUtil.addDependencyJars(job);
    Scan scan = new Scan();
    // note that user can override scan row-caching by setting TableInputFormat.SCAN_CACHEDROWS
    scan.setCaching(getConf().getInt(TableInputFormat.SCAN_CACHEDROWS, 500));
    scan.setCacheBlocks(false);  // should be false for scanning in MapReduce jobs
    scan.setFilter(new KeyOnlyFilter(true));
    if (includeAllCells) {
      scan.setMaxVersions();
    }
    TableMapReduceUtil.initTableMapperJob(
            sourceTableNameString,
            scan,
            ColumnDiscoveryMapper.class,
            null,  // mapper output key is null
            null,  // mapper output value is null
            job);
    job.setOutputFormatClass(NullOutputFormat.class);   // no Mapper output, no Reducer

    return job;
  }

  private boolean parseArguments (final String[] args) {
    if (args.length < 1) {
      return false;
    }
    for (String arg : args) {
      String[] keyValuePair = arg.substring(
              Repository.ARG_KEY_PREFIX.length()).split(Repository.ARG_DELIMITER);
      if (keyValuePair == null || keyValuePair.length != 2) {
        LOG.warn("ERROR in MapReduce " + this.getClass().getSimpleName()
                + " submission: Invalid argument '" + arg + "'");
        return false;
      }
      switch (keyValuePair[0]) {
        case Repository.TABLE_NAME_CONF_KEY:
          sourceTableNameString = keyValuePair[1];
          break;
        case Repository.INCLUDE_ALL_CELLS_CONF_KEY:
          includeAllCells = keyValuePair[1].equalsIgnoreCase(Boolean.TRUE.toString());
          break;
      }
    }
    if (sourceTableNameString == null) {
      System.err.println("ERROR: Required TABLE argument is missing.");
      return false;
    }
    return true;
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
      LOG.info(this.getClass().getSimpleName() + " mapreduce job failed!");
      return 1;
    }
    return 0;
  }

  static class ColumnDiscoveryMapper extends TableMapper<Text, Text> {

    private static final Log LOG = LogFactory.getLog(ColumnDiscoveryMapper.class);
    private Repository repository = null;
    private MTableDescriptor mtd;
    private MConnection columnManagerConnection = null;

    @Override
    protected void setup(Context context) {
      try {
        columnManagerConnection = (MConnection)MConnectionFactory.createConnection();
        repository = columnManagerConnection.getRepository();
        mtd = repository.getMTableDescriptor(TableName.valueOf(
                context.getConfiguration().get(Repository.TABLE_NAME_CONF_KEY)));
      } catch (IOException e) {
        columnManagerConnection = null;
        repository = null;
        LOG.warn(this.getClass().getSimpleName() + "mapper failed to get connection!");
      }
    }
    @Override
    protected void cleanup(Context context) {
      if (columnManagerConnection != null) {
        try {
          columnManagerConnection.close();
        } catch (IOException e) { }
      }
    }

    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context)
            throws InterruptedException, IOException {
      if (columnManagerConnection == null || columnManagerConnection.isClosed()
              || columnManagerConnection.isAborted() || repository == null || mtd == null) {
        return;
      }
      repository.putDiscoveredColumnAuditors(mtd, value, true);
    }
  }
}
