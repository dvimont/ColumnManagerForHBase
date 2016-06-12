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

import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
  private static final Option HELP_OPTION;
  private static final Options OPTIONS;
  private static final HelpFormatter HELP_FORMATTER;
  private static final String BAR = "====================";
  private static final String EXPORT_SCHEMA_UTILITY = "exportSchema";
  private static final String IMPORT_SCHEMA_UTILITY = "importSchema";
  private static final String GET_COLUMN_AUDITORS_UTILITY = "getColumnAuditors";
  private static final String DISCOVER_COLUMN_METADATA_UTILITY = "discoverColumnMetadata";
  private static final String GET_ALL_CHANGE_EVENTS_UTILITY = "getAllChangeEvents";
  private static final String ADD_COLUMN_DEFINITIONS_UTILITY = "addColumnDefinitions";
  private static final Set<String> UTILITY_LIST;
  static {
    HELP_FORMATTER = new HelpFormatter();
    HELP_FORMATTER.setOptionComparator(null); // show options in order they were added to OPTIONS
    UTILITY_LIST = new TreeSet<>();
    UTILITY_LIST.add(EXPORT_SCHEMA_UTILITY);
    UTILITY_LIST.add(IMPORT_SCHEMA_UTILITY);
    UTILITY_LIST.add(GET_COLUMN_AUDITORS_UTILITY);
    UTILITY_LIST.add(DISCOVER_COLUMN_METADATA_UTILITY);
    UTILITY_LIST.add(GET_ALL_CHANGE_EVENTS_UTILITY);
    UTILITY_LIST.add(ADD_COLUMN_DEFINITIONS_UTILITY);
    StringBuilder validUtilities = new StringBuilder();
    for (String utility : UTILITY_LIST) {
      if (validUtilities.length() > 0) {
        validUtilities.append(",");
      }
      validUtilities.append(" ").append(utility);
    }
    UTILITY_OPTION = Option.builder("u").longOpt("utility").hasArg().optionalArg(false)
            .required(true).desc("Utility to run. Valid <arg> values are as follows:"
                    + validUtilities).build();
    TABLE_OPTION = Option.builder("t").longOpt("table").hasArg().optionalArg(false)
            .required(true).desc("Fully-qualified table name.").build();
    HELP_OPTION = Option.builder("h").longOpt("help").hasArg(false)
            .required(false).desc("Display help message.").build();
    OPTIONS = new Options();
    OPTIONS.addOption(UTILITY_OPTION);
    OPTIONS.addOption(TABLE_OPTION);
    OPTIONS.addOption(HELP_OPTION);
  }

  UtilityRunner(String[] args) throws ParseException {
    if (args != null && args.length == 1
            && (args[0].equals("-" + HELP_OPTION.getOpt())
            || args[0].equals("--" + HELP_OPTION.getLongOpt()))) {
      printHelp();
      return;
    }
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = parser.parse(OPTIONS, args);
    } catch (ParseException pe) {
      LOG.error(pe.getClass().getSimpleName() + " encountered: " + pe.getMessage());
      printHelp();
      throw pe;
    }
    if (commandLine.hasOption(HELP_OPTION.getLongOpt())) {
      printHelp();
    }
    StringBuilder parmInfo = new StringBuilder(this.getClass().getSimpleName()
            + " has been invoked with the following <option=argument> combinations:");
    for (Option option : commandLine.getOptions()) {
      parmInfo.append(" <").append(option.getLongOpt())
              .append(option.getValue() == null ? "" : ("=" + option.getValue())).append(">");
    }
    LOG.info(parmInfo);

    String selectedUtility = commandLine.getOptionValue(UTILITY_OPTION.getOpt());
    if (!UTILITY_LIST.contains(selectedUtility)) {
      throw new ParseException("Invalid utility argument submitted: " + selectedUtility);
    }
    LOG.info(this.getClass().getSimpleName()
            + " is invoking the following utility: " + selectedUtility);
    switch (selectedUtility) {
      case EXPORT_SCHEMA_UTILITY:
        break;
      case IMPORT_SCHEMA_UTILITY:
        break;
      case GET_COLUMN_AUDITORS_UTILITY:
        break;
      case DISCOVER_COLUMN_METADATA_UTILITY:
        break;
      case GET_ALL_CHANGE_EVENTS_UTILITY:
        break;
      case ADD_COLUMN_DEFINITIONS_UTILITY:
        break;
    }
  }

  private void printHelp() {
    System.out.println(BAR);
    HELP_FORMATTER.printHelp("java [-options] -cp <hbase-classpath-entries> " + this.getClass().getName(),
            "\n(Note that <hbase-classpath-entries> must include "
                    + "$HBASE_HOME/lib/*:$HBASE_HOME/conf, where $HBASE_HOME is the path to the "
                    + "local HBase installation.)"
                    + "\n\nArguments for " + Repository.PRODUCT_NAME + " "
                    + this.getClass().getSimpleName() + ":\n" + BAR,
            OPTIONS, BAR + "\n\n", true);
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws ParseException {
//    if (args == null || args.length == 0) {
//      args = new String[]{"-u", "oregano", "-h"};
//    }
    new UtilityRunner(args);
  }

}
