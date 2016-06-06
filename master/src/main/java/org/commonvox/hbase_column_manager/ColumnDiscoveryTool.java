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
  private static final String JOB_NAME_CONF_KEY = "mapreduce.job.name";
  private static final String MAP_SPECULATIVE_CONF_KEY = "mapreduce.map.speculative";
  private static final String TABLE_NAME_ARG_KEY = "--sourceTable=";
  static final String TABLE_NAME_CONF_KEY = "mapreduce.source.table";
  String sourceTableNameString = null;

  Job createSubmittableJob(final String[] args) throws IOException {
    if (!parseArguments(args)) {
      return null;
    }
    getConf().setBoolean(MAP_SPECULATIVE_CONF_KEY, true); // prevent writing data twice
    getConf().set(TABLE_NAME_CONF_KEY, sourceTableNameString);
    Job job = Job.getInstance(getConf(), getConf().get(JOB_NAME_CONF_KEY, sourceTableNameString));
    TableMapReduceUtil.addDependencyJars(job);
    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // should be false for MapReduce jobs
    scan.setFilter(new KeyOnlyFilter(true));
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
    if (args[0].startsWith(TABLE_NAME_ARG_KEY)) {
      sourceTableNameString = args[0].substring(TABLE_NAME_ARG_KEY.length());
      return true;
    } else {
      System.err.println("ERROR: Invalid argument '" + args[0] + "'");
      return false;
    }
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
                context.getConfiguration().get(ColumnDiscoveryTool.TABLE_NAME_CONF_KEY)));
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
      repository.putColumnAuditorSchemaEntities(mtd, value, true);
    }
  }
}
