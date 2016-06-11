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
  /*
   -cp <class search path of directories and zip/jar files>
    -classpath <class search path of directories and zip/jar files>
  */
//  private static final Option JAVA_COMMAND = new Option("")
  private static final Option UTILITY_OPTION = new Option("u", "utility", true,
          "specify " + Repository.PRODUCT_NAME + " utility to run");
  private static final Option HELP_OPTION = new Option("h", "help", false,
          "view this help message");
  private static final Options OPTIONS = new Options();
  static {
     OPTIONS.addOption(UTILITY_OPTION);
     OPTIONS.addOption(HELP_OPTION);
  }
  private static final HelpFormatter HELP_FORMATTER = new HelpFormatter();
  static {
    HELP_FORMATTER.setOptionComparator(null); // show options in order they were added to OPTIONS
  }
  private static final String BAR = "====================";

  UtilityRunner(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = parser.parse(OPTIONS, args);
    } catch (ParseException pe) {
      LOG.error(ParseException.class.getSimpleName() + " encountered.", pe);
      printHelp();
      return;
    }
    if (commandLine.hasOption(HELP_OPTION.getLongOpt())) {
      printHelp();
    }
    if (commandLine.hasOption(UTILITY_OPTION.getLongOpt())) {
      LOG.info("Utility selected: " + commandLine.getOptionValue(UTILITY_OPTION.getLongOpt()));
    }
  }

  private void printHelp() {
    HELP_FORMATTER.printHelp("java [-options] -cp <hbase-classpath-entries> " + this.getClass().getName(),
            "\nNote that <hbase-classpath-entries> must include "
                    + "$HBASE_HOME/lib/*:$HBASE_HOME/conf, where $HBASE_HOME is the path to the "
                    + "local HBase installation."
                    + "\n\nArguments for " + Repository.PRODUCT_NAME + " "
                    + this.getClass().getSimpleName() + ":\n" + BAR,
            OPTIONS, BAR + "\n\n", true);
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) {
//    if (args == null || args.length == 0) {
//      args = new String[]{"-u", "oregano", "-h"};
//    }
    new UtilityRunner(args);
  }

}
