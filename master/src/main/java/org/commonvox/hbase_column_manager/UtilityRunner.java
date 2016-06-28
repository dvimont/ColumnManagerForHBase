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

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.bind.JAXBException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Handles command-line interface for package:
 * parses arguments and navigates to appropriate methods.
 *
 * @author Daniel Vimont
 */
class UtilityRunner {

  private static final Logger LOG = Logger.getLogger(UtilityRunner.class.getName());
  private static final Option UTILITY_OPTION;
  private static final Option TABLE_OPTION;
  private static final Option TABLE_OPTION_NOT_REQUIRED;
  private static final Option FILE_OPTION;
  private static final Option FILE_OPTION_NOT_REQUIRED;
  private static final Option HELP_OPTION;
  private static final Options OPTIONS_SET_2;
  private static final Options OPTIONS_SET_1;
  private static final HelpFormatter HELP_FORMATTER;
  private static final String BAR = "====================";
  public static final String EXPORT_SCHEMA_UTILITY = "exportSchema";
  public static final String IMPORT_SCHEMA_UTILITY = "importSchema";
  public static final String GET_COLUMN_QUALIFIERS_UTILITY_DIRECT_SCAN = "getColumnQualifiers";
  public static final String GET_COLUMN_QUALIFIERS_UTILITY_MAP_REDUCE
          = "getColumnQualifiersViaMapReduce";
  public static final String GET_CHANGE_EVENTS_UTILITY = "getChangeEventsForTable";
  public static final String UNINSTALL_REPOSITORY = "uninstallRepository";
  private static final Set<String> UTILITY_LIST;
  static {
    HELP_FORMATTER = new HelpFormatter();
    HELP_FORMATTER.setOptionComparator(null); // show options in order added to OPTIONS_SET_2
    UTILITY_LIST = new TreeSet<>();
    UTILITY_LIST.add(EXPORT_SCHEMA_UTILITY);
    UTILITY_LIST.add(IMPORT_SCHEMA_UTILITY);
    UTILITY_LIST.add(GET_CHANGE_EVENTS_UTILITY);
    UTILITY_LIST.add(GET_COLUMN_QUALIFIERS_UTILITY_DIRECT_SCAN);
    UTILITY_LIST.add(GET_COLUMN_QUALIFIERS_UTILITY_MAP_REDUCE);
    UTILITY_LIST.add(UNINSTALL_REPOSITORY);
    StringBuilder validUtilities = new StringBuilder();
    for (String utility : UTILITY_LIST) {
      if (validUtilities.length() > 0) {
        validUtilities.append(",");
      }
      validUtilities.append(" ").append(utility);
    }
    UTILITY_OPTION = Option.builder("u").longOpt("utility").hasArg().required(true)
            .desc("Utility to run. Valid <arg> values are as follows:" + validUtilities)
            .optionalArg(false).build();
    TABLE_OPTION = Option.builder("t").longOpt("table").hasArg().required(true)
            .desc("Fully-qualified table name; or submit '*' in place of table "
                    + "qualifier (e.g., 'myNamespace:*') to process all tables in a given "
                    + "namespace.").optionalArg(false).build();
    TABLE_OPTION_NOT_REQUIRED = (Option)TABLE_OPTION.clone();
    TABLE_OPTION_NOT_REQUIRED.setRequired(false);
    FILE_OPTION = Option.builder("f").longOpt("file").hasArg().required(true)
            .desc("Source/target file.").optionalArg(false).build();
    FILE_OPTION_NOT_REQUIRED = (Option)FILE_OPTION.clone();
    FILE_OPTION_NOT_REQUIRED.setRequired(false);
    HELP_OPTION = Option.builder("h").longOpt("help").hasArg(false).required(false)
            .desc("Display this help message.").build();
    OPTIONS_SET_1 = new Options();
    OPTIONS_SET_1.addOption(UTILITY_OPTION);
    OPTIONS_SET_1.addOption(TABLE_OPTION_NOT_REQUIRED);
    OPTIONS_SET_1.addOption(FILE_OPTION_NOT_REQUIRED);
    OPTIONS_SET_1.addOption(HELP_OPTION);
    OPTIONS_SET_2 = new Options();
    OPTIONS_SET_2.addOption(UTILITY_OPTION);
    OPTIONS_SET_2.addOption(TABLE_OPTION);
    OPTIONS_SET_2.addOption(FILE_OPTION);
    OPTIONS_SET_2.addOption(HELP_OPTION);
  }

  UtilityRunner(String[] args) throws Exception, ParseException, IOException, JAXBException {
    if ( args != null && args.length == 1
            && (args[0].equals("-" + HELP_OPTION.getOpt())
            || args[0].equals("--" + HELP_OPTION.getLongOpt())) ) {
      printHelp();
      return;
    }
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = parser.parse(OPTIONS_SET_1, args);
    } catch (ParseException pe) {
      LOG.error(pe.getClass().getSimpleName() + " encountered: " + pe.getMessage());
      printHelp();
      throw pe;
    }
    if (commandLine.hasOption(HELP_OPTION.getLongOpt())) {
      printHelp();
    }
    String selectedUtility = commandLine.getOptionValue(UTILITY_OPTION.getOpt());
    if (!UTILITY_LIST.contains(selectedUtility)) {
      throw new ParseException("Invalid utility argument submitted: <" + selectedUtility + ">");
    }

    if (selectedUtility.equals(UNINSTALL_REPOSITORY)) {
      logParmInfo(commandLine);
      LOG.info(this.getClass().getSimpleName()
              + " is invoking the following utility: <" + selectedUtility + ">");
      try (Admin standardAdmin = ConnectionFactory.createConnection().getAdmin()) {
        RepositoryAdmin.uninstallRepositoryStructures(standardAdmin);
      }
      return;
    }

    try {
      parser = new DefaultParser();
      commandLine = parser.parse(OPTIONS_SET_2, args);
    } catch (ParseException pe) {
      LOG.error(pe.getClass().getSimpleName() + " encountered: " + pe.getMessage());
      printHelp();
      throw pe;
    }
    logParmInfo(commandLine);

    String selectedTableString = commandLine.getOptionValue(TABLE_OPTION.getOpt());
    String selectedNamespaceString = "";
    if (selectedTableString.endsWith(Repository.ALL_TABLES_WILDCARD_INDICATOR)) {
      selectedNamespaceString
                = selectedTableString.substring(0, selectedTableString.length() - 2);
      if (selectedNamespaceString.isEmpty()) {
        selectedNamespaceString = Bytes.toString(Repository.HBASE_DEFAULT_NAMESPACE);
      }
    }
    String selectedFileString = commandLine.getOptionValue(FILE_OPTION.getOpt());

    Configuration mConf = MConfiguration.create();
    // invoking UtilityRunner overrides possible inactive state of ColumnManager
    mConf.setBoolean(Repository.HBASE_CONFIG_PARM_KEY_COLMANAGER_ACTIVATED, true);

    try (Connection mConnection = MConnectionFactory.createConnection(mConf)) {
      RepositoryAdmin repositoryAdmin = new RepositoryAdmin(mConnection);
      // Note that selectedUtility will validate selectedTableString and selectedFileString
      File selectedFile = new File(selectedFileString);
      LOG.info(this.getClass().getSimpleName()
              + " is invoking the following utility: <" + selectedUtility
              + (selectedNamespaceString.isEmpty() ?
                      "> on the following table: <" + selectedTableString
                      : "> on the following namespace: <" + selectedNamespaceString)
              + "> using the following "
              + "source/target file <" + selectedFileString + ">");
      switch (selectedUtility) {
        case EXPORT_SCHEMA_UTILITY:
          if (selectedNamespaceString.isEmpty()) {
            repositoryAdmin.exportSchema(selectedFile, TableName.valueOf(selectedTableString));
          } else {
            repositoryAdmin.exportSchema(selectedFile, selectedNamespaceString);
          }
          break;
        case IMPORT_SCHEMA_UTILITY:
          if (selectedNamespaceString.isEmpty()) {
            repositoryAdmin.importSchema(selectedFile, TableName.valueOf(selectedTableString), false);
          } else {
            repositoryAdmin.importSchema(selectedFile, selectedNamespaceString, false);
          }
          break;
        case GET_CHANGE_EVENTS_UTILITY:
          StringBuilder headerDetail = new StringBuilder("-- file generated for ")
                  .append(selectedNamespaceString.isEmpty() && selectedTableString.isEmpty() ?
                          "full " + Repository.PRODUCT_NAME + " Repository, " : "")
                  .append(selectedNamespaceString.isEmpty() ? ""
                          : "Namespace:[" + selectedNamespaceString + "], ")
                  .append(selectedTableString.isEmpty() ? ""
                          : "Table:[" + selectedTableString + "], ");
          if (selectedNamespaceString.isEmpty()) {
            ChangeEventMonitor.exportChangeEventListToCsvFile(
                    repositoryAdmin.getChangeEventMonitor().getChangeEventsForTable(
                            TableName.valueOf(selectedTableString), true),
                    selectedFile, headerDetail.toString());
          } else {
            ChangeEventMonitor.exportChangeEventListToCsvFile(
                    repositoryAdmin.getChangeEventMonitor().getChangeEventsForNamespace(
                            Bytes.toBytes(selectedNamespaceString), true),
                    selectedFile, headerDetail.toString());
          }
          break;
        case GET_COLUMN_QUALIFIERS_UTILITY_DIRECT_SCAN:
          if (selectedNamespaceString.isEmpty()) {
            repositoryAdmin.discoverColumnMetadata(
                    TableName.valueOf(selectedTableString), true, false);
            repositoryAdmin.outputReportOnColumnQualifiers(
                    selectedFile, TableName.valueOf(selectedTableString));
          } else {
            repositoryAdmin.discoverColumnMetadata(selectedNamespaceString, true, false);
            repositoryAdmin.outputReportOnColumnQualifiers(selectedFile, selectedNamespaceString);
          }
          break;
        case GET_COLUMN_QUALIFIERS_UTILITY_MAP_REDUCE:
          if (selectedNamespaceString.isEmpty()) {
            repositoryAdmin.discoverColumnMetadata(
                    TableName.valueOf(selectedTableString), true, true);
            repositoryAdmin.outputReportOnColumnQualifiers(
                    selectedFile, TableName.valueOf(selectedTableString));
          } else {
            repositoryAdmin.discoverColumnMetadata(selectedNamespaceString, true, true);
            repositoryAdmin.outputReportOnColumnQualifiers(selectedFile, selectedNamespaceString);
          }
          break;
      }
    }
  }

  private void logParmInfo(CommandLine commandLine) {
    StringBuilder parmInfo = new StringBuilder(this.getClass().getSimpleName()
            + " has been invoked with the following <option=argument> combinations:");
    for (Option option : commandLine.getOptions()) {
      parmInfo.append(" <").append(option.getLongOpt())
              .append(option.getValue() == null ? "" : ("=" + option.getValue())).append(">");
    }
    LOG.info(parmInfo);
  }

  private void printHelp() {
    System.out.println(BAR);
    String commandLineSyntax
            = "java [-options] -cp <hbase-classpath-entries> " + this.getClass().getName();
    String header = "\n    *** Note that <hbase-classpath-entries> must include\n"
            + "    ***   $HBASE_HOME/lib/*:$HBASE_HOME/conf, where $HBASE_HOME\n"
            + "    ***   is the path to the local HBase installation."
            + "\n\nArguments for " + Repository.PRODUCT_NAME + " "+ this.getClass().getSimpleName()
            + ":\n" + BAR;
    String footer = BAR + "\n\nFOR EXAMPLE, the exportSchema function might be invoked as "
            + "follows from within the directory containing the *utility.jar:\n\n"
            + "    java -cp *:$HBASE_HOME/lib/*:$HBASE_HOME/conf\n"
            + "        org.commonvox.hbase_column_manager.UtilityRunner\n"
            + "        -u exportSchema -t myNamespace:myTable -f myOutputFile.xml";
    HELP_FORMATTER.printHelp(commandLineSyntax, header, OPTIONS_SET_2, footer, true);
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args)
          throws Exception, ParseException, IOException, JAXBException {
    new UtilityRunner(args);
  }
}
