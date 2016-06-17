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
/**
 * <hr><b>ColumnManagerAPI for <a href="http://hbase.apache.org/" target="_blank">HBase™</a></b>
 * provides an extended <i>METADATA REPOSITORY SYSTEM for HBase 1.x</i>
 * with options for:<br><br>
 * <BLOCKQUOTE>
 * &nbsp;&nbsp;&nbsp;&nbsp;(1) <b>COLUMN AUDITING/DISCOVERY</b> -- captures Column metadata
 * (qualifier-name and max-length for each unique column-qualifier)
 * -- either via <a href="#column-auditing">real-time auditing</a>
 * as <i>Tables</i> are updated, or via a <a href="#discovery">discovery facility</a>
 * (direct-scan or mapreduce) for previously-existing <i>Tables</i>;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;(2) <b>COLUMN-DEFINITION FACILITIES</b> --
 * administratively-managed <a href="ColumnDefinition.html">ColumnDefinitions</a>
 * (stipulating valid qualifier-name, column length, and/or value) may be created and
 * (a) optionally <a href="#enforcement">activated for column validation and enforcement</a>
 * as Tables are updated, and/or (b) used in the
 * <a href="#invalid-column-reporting">generation of various "Invalid Column" CSV-formatted reports</a>
 * (reporting on any column qualifiers, lengths, or values which do not adhere to
 * ColumnDefinitions);<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;(3) <b>SCHEMA CHANGE MONITORING</b> -- tracks and provides an
 * <a href="#audit-trail">audit trail</a> for structural modifications made to
 * <i>Namespaces</i>, <i>Tables</i>, and <i>Column Families</i>;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;(4) <b>SCHEMA EXPORT/IMPORT</b> -- provides
 * <a href="#export-import">schema (metadata) export and import facilities</a>
 * for HBase <i>Namespace</i>, <i>Table</i>, and all table-component structures.<br><br>
 *
 * Once it is installed and configured, standard usage of the ColumnManagerAPI in Java programs is
 * accomplished by simply substituting any reference to the standard HBase {@code ConnectionFactory}
 * class with a reference to the ColumnManager
 * <a href="MConnectionFactory.html">MConnectionFactory</a> class (as shown in the
 * <a href="#usage">USAGE IN APPLICATION DEVELOPMENT section</a> below).<br>
 * All other interactions with the HBase API are then to be coded as usual; ColumnManager will work
 * behind the scenes to capture HBase metadata as stipulated by an administrator/developer in
 * <a href="#config">the ColumnManager configuration</a>.<br><br>
 * Any application coded with the ColumnManager API can be made to revert to standard HBase API
 * functionality simply by either (a) setting the value of the {@code column_manager.activated}
 * property to {@code <false>} in all {@code hbase-*.xml} configuration files, or (b) removing
 * that property from {@code hbase-*.xml} configuration files altogether.<br>
 * Thus, a ColumnManager-coded application can be used with ColumnManager <i>activated</i> in a
 * development and/or staging environment, but <i>deactivated</i> in production (where
 * ColumnManager's extra overhead might be undesirable).
 * </BLOCKQUOTE>
 * <i>HBase™ is a trademark of the <a href="http://www.apache.org/" target="_blank">
 * Apache Software Foundation</a>.</i><br><br>
 * <hr><b>FUTURE ENHANCEMENTS MAY INCLUDE:</b>
 * <ul>
 * <li><b>GUI interface:</b>
 * A JavaFX-based GUI interface could be built atop the ColumnManagerAPI, for administrative use on
 * Mac, Linux, and Windows desktops.
 * </li>
 * <li><b>Command-line invocation for some administrative functions:</b>
 * For the convenience of HBase administrators, particularly those who are not Java coders, it
 * may be advisable to make certain <a href="RepositoryAdmin.html">RepositoryAdmin</a> functions
 * available via command-line invocation (i.e. through an executable UtilityRunner).
 * </li>
 * </ul>
 * <hr>
 * This package transparently complements the standard HBase API provided by the Apache Software
 * Foundation in the packages {@code org.apache.hadoop.hbase} and
 * {@code org.apache.hadoop.hbase.client}.
 * <br>
 * <br>
 * <br>
 * <hr style="height:3px;color:black;background-color:black">
 * <h3>Table of Contents</h3>
 * <ol type="I"><li><a href="#prereq">PREREQUISITES</a></li>
 * <li><a href="#install">INSTALLATION</a></li>
 * <li><a href="#uninstall">UNINSTALLATION</a></li>
 * <li><a href="#config">CONFIGURATION</a></li>
 * <li><a href="#usage">USAGE IN APPLICATION DEVELOPMENT</a></li>
 * <li><a href="#column-auditing">COLUMN AUDITING IN REAL-TIME</a></li>
 * <li><a href="#query">QUERYING THE REPOSITORY</a></li>
 * <li><a href="#admin">ADMINISTRATIVE TOOLS</a>
 * </ol>
 *
 * <a name="prereq"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>I. <u>PREREQUISITES</u></b>
 * <BLOCKQUOTE>
 * <b>HBase 1.x or later</b> -- HBase must be installed as per the installation instructions given in the
 * official
 * <a href="https://hbase.apache.org/book.html" target="_blank">Apache HBase Reference Guide</a>
 * (either in stand-alone, pseudo-distributed, or fully-distributed mode).
 * <br><br>
 * <b>HBase Java API</b> -- An IDE environment must be set up for HBase-oriented development using
 * the HBase Java API such that, at a minimum, an
 * <a href="https://gist.github.com/dvimont/a7791f61c4ba788fd827" target="_blank">HBase "Hello
 * World" application</a>
 * can be successfully compiled and run in it.
 * <br><br>
 * <b>JDK 7</b> -- HBase 1.x or later (upon which this package is dependent) requires JDK 7
 * or later.
 * </BLOCKQUOTE>
 * <a name="install"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>II. <u>INSTALLATION</u></b>
 * <BLOCKQUOTE>
 * <b>Step 1: Get the required JAR files via download or by setting Maven project dependencies</b>
 * <br>
 * The most recently released versions of
 * <b><a href="https://github.com/dvimont/ColumnManager/releases" target="_blank">
 * the JAR files for ColumnManager</a></b>
 * may be downloaded from GitHub and included in the IDE environment's compile and run-time
 * classpath configurations.
 * <br><br>
 * In the context of a Maven project, a dependency may be set as follows:
 * <br>
 * <pre>{@code      [MAVEN DEPENDENCY EXAMPLE TO BE INSERTED HERE .]}</pre>
 * <br>
 * <br>
 * <a name="activate"></a>
 * <b>Step 2: Activate ColumnManager</b>
 * <br>
 * Add the following property element to either
 * <a href="https://hbase.apache.org/book.html#_configuration_files" target="_blank">the
 * &#60;hbase-site.xml&#62; file</a>
 * or an optional separate configuration file named <b>{@code <hbase-column-manager.xml>}</b>.
 * <pre>{@code      <property>
 *         <name>column_manager.activated</name>
 *         <value>true</value>
 *      </property>}</pre>
 * <i>NOTE</i> that the default for "{@code column_manager.activated}" is "{@code false}", so when
 * the property above is not present in {@code <hbase-site.xml>}
 * or in {@code <hbase-column-manager.xml>}, the ColumnManager API will
 * function exactly like the standard HBase API. Thus, a single body of code can operate <i>with</i>
 * ColumnManager functionality in one environment (typically, a development or testing
 * environment) and can completely <i>bypass</i>
 * ColumnManager functionality in another environment (potentially staging or production),
 * with the only difference between the environments being the presence or absence of the
 * "{@code column_manager.activated}" property in each environment's {@code hbase-*.xml}
 * configuration files.
 * <br>
 * <br>
 * <b>Step 3: Confirm installation (and create ColumnManager Repository <i>Namespace</i> and
 * <i>Table</i>)</b>
 * <br>
 * The following code may be run to confirm successful installation of ColumnManager:
 * <pre>{@code      import org.apache.hadoop.hbase.client.Connection;
 *      import org.commonvox.hbase_column_manager.MConnectionFactory;
 *
 *      public class ConfirmColumnManagerInstall {
 *          public static void main(String[] args) throws Exception {
 *              try (Connection connection = MConnectionFactory.createConnection()) {}
 *          }
 *      } }</pre> Note that the first invocation of <a href="MConnectionFactory.html#createConnection--">
 * MConnectionFactory.createConnection()</a> (as in the above code) will result in the automatic
 * creation of the ColumnManager Repository
 * <i>Namespace</i> ("{@code __column_manager_repository_namespace}") and <i>Table</i>
 * ("{@code column_manager_repository_table}").<br>
 * If the code above runs successfully, its log output will include a number of lines of Zookeeper
 * INFO output, as well as several lines of ColumnManager INFO output.
 * <br>
 * <br>
 * <b>Step 4: [OPTIONAL] Explicitly create Repository structures</b>
 * <br>
 * As an alternative to the automatic creation of the ColumnManager Repository
 * <i>Namespace</i> and <i>Table</i> in the preceding step, these structures may be explicitly
 * created through invocation of the static method
 * <a href="RepositoryAdmin.html#installRepositoryStructures-org.apache.hadoop.hbase.client.Admin-">
 * RepositoryAdmin#installRepositoryStructures</a>. Successful creation of these structures will
 * result in messages
 * such as the following appearing in the session's log output:
 * <pre>{@code      2015-10-09 11:03:30,184 INFO  [main] commonvox.hbase_column_manager: ColumnManager Repository Namespace has been created ...
 *      2015-10-09 11:03:31,498 INFO  [main] commonvox.hbase_column_manager: ColumnManager Repository Table has been created ...}</pre>
 * </BLOCKQUOTE>
 *
 * <a name="uninstall"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>III. <u>UNINSTALLATION</u></b>
 * <BLOCKQUOTE>
 * <b>Step 1: Deactivate ColumnManager</b>
 * <br>
 * Either remove the {@code column_manager.activated} property element from the environment's
 * {@code hbase-*.xml} configuration files or else set the property element's value to
 * {@code false}.
 * <br>
 * <br>
 * <b>Step 2: Invoke the {@code uninstall} method</b>
 * <br>
 * Invoke the static {@code RepositoryAdmin} method
 * <a href="RepositoryAdmin.html#uninstallRepositoryStructures-org.apache.hadoop.hbase.client.Admin-">
 * uninstallRepositoryStructures</a> to disable and delete the Repository table and to drop the
 * Repository namespace.
 * </BLOCKQUOTE>
 *
 * <a name="config"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>IV. <u>CONFIGURATION OPTIONS</u></b>
 * <BLOCKQUOTE>
 * <b>A. INCLUDE/EXCLUDE TABLES FOR ColumnManager PROCESSING</b>
 * <br>
 * <i><b>By default, when ColumnManager is installed and <a href="#activate">activated</a>,
 * all user Tables are included in ColumnManager processing.</b></i>
 * However, the following options are available
 * to limit ColumnManager processing to a specific subset of user Tables.
 * <BLOCKQUOTE>
 * <b>Option 1: Explicitly INCLUDE Tables for ColumnManager processing</b><br>
 * Specific <i>Tables</i> may optionally be explicitly
 * <b>included</b> in ColumnManager processing (with all others not specified being automatically
 * excluded).
 * This is done by adding the {@code [column_manager.includedTables]} property to either the
 * <a href="https://hbase.apache.org/book.html#_configuration_files" target="_blank">
 * &#60;hbase-site.xml&#62; file</a>
 * or in an optional, separate <b>{@code <hbase-column-manager.xml>}</b> file. Values are expressed
 * as fully-qualified <i>Table</i> names (for those <i>Tables</i> not in the default namespace,
 * the fully-qualified name is the <i>Namespace</i> name followed by the <i>Table</i> name,
 * delimited by a colon). Multiple values are delimited by commas, as in the following example:
 * <pre>{@code      <property>
 *         <name>column_manager.includedTables</name>
 *         <value>default:*,goodNamespace:myTable,betterNamespace:yetAnotherTable</value>
 *      </property>}</pre>
 * Note that all <i>Tables</i> in a given Namespace may be included by using an
 * asterisk {@code [*]} symbol in the place of a specific <i>Table</i> qualifier,
 * as in the example above which includes all Tables in the
 * "default" namespace via the specification, [{@code default:*}].<br><br>
 *
 * <b>Option 2: Explicitly EXCLUDE Tables from ColumnManager processing</b><br>
 *
 * Alternatively, specific <i>Tables</i> may optionally be explicitly
 * <b>excluded</b> from ColumnManager processing (with all others not specified being automatically
 * included).
 * This is done by adding the {@code [column_manager.excludedTables]} property to either the
 * <a href="https://hbase.apache.org/book.html#_configuration_files" target="_blank">
 * &#60;hbase-site.xml&#62; file</a>
 * or in an optional, separate <b>{@code <hbase-column-manager.xml>}</b> file. Values are expressed
 * as fully-qualified <i>Table</i> names (for those <i>Tables</i> not in the default namespace,
 * the fully-qualified name is the <i>Namespace</i> name followed by the <i>Table</i> name,
 * delimited by a colon). Multiple values are delimited by commas, as in the following example:
 * <pre>{@code      <property>
 *         <name>column_manager.excludedTables</name>
 *         <value>myNamespace:*,goodNamespace:myExcludedTable,betterNamespace:yetAnotherExcludedTable</value>
 *      </property>}</pre>
 * Note that all <i>Tables</i> in a given Namespace may be excluded by using an
 * asterisk {@code [*]} symbol in the place of a specific <i>Table</i> qualifier,
 * as in the example above which excludes all Tables in the
 * "myNamespace" namespace via the specification, [{@code myNamespace:*}].<br><br>
 * <i>Note also that if a {@code [column_manager.includedTables]} property is found in the
 * {@code <hbase-*.xml>}
 * files, then any {@code [column_manager.excludedTables]} property will be ignored.</i>
 * </BLOCKQUOTE>
 *
 * <a name="enforcement"></a>
 * <b>B. MANAGE <i>COLUMN DEFINITIONS</i> AND ENABLE ENFORCEMENT</b>
 * <BLOCKQUOTE>
 * <hr>
 * A <a href="ColumnDefinition.html">Column Definition</a> pertains to a specific
 * <i>Column Qualifier</i> within a <i>Column Family</i> of a
 * <a href="#config">ColumnManager-included</a> <i>Table</i>, and permits optional stipulation of
 * <ul>
 * <li><u>Column Length</u>: valid maximum length of a value stored in HBase for the column,
 * and/or</li>
 * <li><u>Column Validation Regular Expression</u>: a regular expression that any value submitted
 * for storage in the column must match.</li>
 * </ul>
 * <hr><br>
 * <b>Manage ColumnDefinitions</b>: The <a href="ColumnDefinition.html">ColumnDefinitions</a>
 * of a <i>Column Family</i> are managed via a number of RepositoryAdmin
 * <a href="RepositoryAdmin.html#addColumnDefinition-org.apache.hadoop.hbase.TableName-byte:A-org.commonvox.hbase_column_manager.ColumnDefinition-">
 * add</a>,
 * <a href="RepositoryAdmin.html#getColumnDefinitions-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * get</a>, and
 * <a href="RepositoryAdmin.html#deleteColumnDefinition-org.apache.hadoop.hbase.TableName-byte:A-byte:A-">
 * delete</a> methods.<br><br>
 * <b>Enable enforcement of ColumnDefinitions</b>: Enforcement of the
 * <a href="ColumnDefinition.html">ColumnDefinitions</a> of a given <i>Column Family</i> does
 * not occur until explicitly enabled via the method
 * <a href="RepositoryAdmin.html#setColumnDefinitionsEnforced-boolean-org.apache.hadoop.hbase.TableName-byte:A-">
 * RepositoryAdmin#setColumnDefinitionsEnforced</a>. This same method may be invoked to toggle
 * enforcement {@code off} again for the <i>Column Family</i>.<br><br>
 * When enforcement is enabled, then (a) any <i>Column Qualifier</i> submitted in a {@code put}
 * (i.e., insert/update) to the <i>Table:Column-Family</i> must correspond to an existing
 * {@code ColumnDefinition} of the <i>Column Family</i>, and (b) the corresponding <i>Column
 * value</i>
 * submitted must pass all validations (if any) stipulated by the {@code ColumnDefinition}. Any
 * {@code ColumnDefinition}-related enforcement-violation encountered during processing of a
 * {@code put} transaction will result in a
 * <a href="ColumnManagerIOException.html">ColumnManagerIOException</a>
 * (a subclass of the standard {@code IOException} class) being thrown: specifically, either a
 * <a href="ColumnDefinitionNotFoundException.html">ColumnDefinitionNotFoundException</a> or a
 * <a href="ColumnValueInvalidException.html">ColumnValueInvalidException</a>.
 * </BLOCKQUOTE>
 * </BLOCKQUOTE>
 *
 * <a name="usage"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>V. <u>USAGE IN APPLICATION DEVELOPMENT</u></b>
 * <BLOCKQUOTE>
 * <b>A. ALWAYS USE {@code MConnectionFactory} INSTEAD OF {@code ConnectionFactory}</b>
 * <BLOCKQUOTE>
 * To use ColumnManager in an HBase development environment, simply replace any reference to the
 * standard HBase API {@code ConnectionFactory} with a reference to ColumnManager's
 * <b><a href="MConnectionFactory.html">MConnectionFactory</a></b>
 * as follows:<br><br>
 * <u><i>Instead of</i></u><br>
 * <pre>{@code      import org.apache.hadoop.hbase.client.ConnectionFactory;
 *      ...
 *      Connection myConnection = ConnectionFactory.createConnection();
 *      ...}</pre>
 * <u><i>Use</i></u><br>
 * <pre>{@code      import org.commonvox.hbase_column_manager.MConnectionFactory;
 *      ...
 *      Connection myConnection = MConnectionFactory.createConnection();
 *      ...}</pre> Note that all Connection objects created in this manner generate special
 * {@code Admin}, {@code Table}, and {@code BufferedMutator} objects which (in addition to providing
 * all standard HBase API functionality) transparently interface with the ColumnManager Repository
 * for tracking and persisting of
 * <i>Namespace</i>, <i>Table</i>, <i>Column Family</i>, and
 * <i><a href="ColumnAuditor.html">ColumnAuditor</a></i> metadata. In addition,
 * ColumnManager-enabled {@code HTableMultiplexer} instances may be obtained via the method
 * <a href="RepositoryAdmin.html#createHTableMultiplexer-int-">RepositoryAdmin#createHTableMultiplexer</a>.
 * </BLOCKQUOTE>
 * <b>B. OPTIONALLY CATCH {@code ColumnManagerIOException} OCCURRENCES</b>
 * <BLOCKQUOTE>
 * In the context of some applications it may be necessary to perform special processing when a
 * <a href="ColumnManagerIOException.html">ColumnManagerIOException</a> is thrown, which may
 * signify rejection of a specific <i>Column</i> entry submitted in a {@code put}
 * (i.e., insert/update) to
 * an <a href="#enforcement">enforcement-enabled</a> <i>Table/Column-Family</i>.
 * In such cases, exceptions of this abstract
 * type (or its concrete subclasses) may be caught, and appropriate processing performed.
 * </BLOCKQUOTE>
 * </BLOCKQUOTE>
 *
 * <a name="column-auditing"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>VI. <u>COLUMN AUDITING IN REAL-TIME</u></b>
 * <BLOCKQUOTE>
 * When <a href="#activate">ColumnManager is activated</a> and <a href="#usage">usage has been
 * properly configured</a>,
 * <a href="ColumnAuditor.html">ColumnAuditor</a> metadata is gathered and persisted in the
 * Repository at runtime as Mutations (i.e. puts, appends, increments) are submitted via the
 * API to any <a href="#config">ColumnManager-included</a> <i>Table</i>.
 * All such metadata is then retrievable via the
 * <a href="RepositoryAdmin.html#getColumnAuditors-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * RepositoryAdmin#getColumnAuditors</a> and
 * <a href="RepositoryAdmin.html#getColumnQualifiers-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * RepositoryAdmin#getColumnQualifiers</a> methods.
 * <br><br>Note that <a href="ColumnAuditor.html">ColumnAuditor</a> metadata may also be
 * gathered for previously-existing <i>Column</i>s via the
 * <a href="#discovery">RepositoryAdmin discovery methods</a>.
 * </BLOCKQUOTE>
 *
 * <a name="query"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>VII. <u>QUERYING THE COLUMN-MANAGER REPOSITORY</u></b>
 * <ul>
 * <li>Get <i>Column Qualifier</i> names and additional column metadata:
 * <BLOCKQUOTE>
 * Subsequent to either the <a href="#column-auditing">capture of column metadata in real-time</a>
 * or its discovery via the <a href="#discovery">RepositoryAdmin discovery methods</a>,
 * a list of the <i>Column Qualifier</i>s belonging to a <i>Column Family</i>
 * of a <i>Table</i> may be obtained via the
 * <a href="RepositoryAdmin.html#getColumnQualifiers-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * RepositoryAdmin#getColumnQualifiers</a> method.
 * Alternatively, a list of <a href="ColumnAuditor.html">ColumnAuditor</a> objects (containing
 * column qualifiers and additional column metadata) is obtained via the
 * <a href="RepositoryAdmin.html#getColumnAuditors-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * RepositoryAdmin#getColumnAuditors</a> method.
 * </BLOCKQUOTE>
 * </li>
 * <li><a name="invalid-column-reporting"></a>Get Invalid Column reports which cite
 * discrepancies from <a href="ColumnDefinition.html">ColumnDefinitions</a>:<br>
 * <BLOCKQUOTE>
 * Subsequent to creation of <a href="ColumnDefinition.html">ColumnDefinitions</a> for a
 * <i>Table/ColumnFamily</i>, a CSV-formatted report listing columns which deviate from
 * those ColumnDefinitions (either in terms of qualifier-name, length, or value) may be
 * generated via the various
 * <a href="RepositoryAdmin.html#outputReportOnInvalidColumnQualifiers-java.io.File-org.apache.hadoop.hbase.TableName-boolean-boolean-">
 * RepositoryAdmin#outputReportOnInvalidColumn*</a> methods. If a method is run in
 * <i>verbose</i> mode, the outputted CSV file will include an entry (identified by the
 * fully-qualified column name and rowId) for each explicit invalid column that is found;
 * otherwise the report will contain a summary, giving a count of the invalidities associated
 * with a specific column-qualifier name. Note that invalid column report processing may optionally
 * be done via direct-scan or via mapreduce.
 * </BLOCKQUOTE>
 * </li>
 * <li><a name="audit-trail"></a>Get audit trail metadata:<br>
 * <BLOCKQUOTE>
 * A <a href="ChangeEventMonitor.html">ChangeEventMonitor</a> object (obtained via the method
 * <a href="RepositoryAdmin.html#getChangeEventMonitor--">RepositoryAdmin#getChangeEventMonitor</a>)
 * outputs lists of <a href="ChangeEvent.html">ChangeEvents</a>
 * (pertaining to structural changes made to user <i>Namespaces</i>, <i>Tables</i>,
 * <i>Column Families</i>, <i>ColumnAuditors</i>, and <i>ColumnDefinitions</i>) tracked by the
 * ColumnManager Repository.<br>
 * The ChangeEventMonitor's "get" methods allow for retrieving {@link ChangeEvent}s grouped and
 * ordered in various ways, and a static convenience method,
 * <a href="ChangeEventMonitor.html#exportChangeEventListToCsvFile-java.util.Collection-java.io.File-">
 * ChangeEventMonitor#exportChangeEventListToCsvFile</a>, is provided for outputting a list of
 * {@code ChangeEvent}s to a CSV file.
 * </BLOCKQUOTE>
 * </li>
 * </ul>
 *
 * <a name="admin"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>VIII. <u>ADMINISTRATIVE TOOLS</u></b>
 * <ul>
 * <li><a name="discovery"></a>HBase column-metadata discovery tools
 * <BLOCKQUOTE>
 * When ColumnManager is installed into an already-populated HBase environment, the
 * <a href="RepositoryAdmin.html#discoverColumnMetadata-boolean-">
 * RepositoryAdmin#discoverColumnMetadata</a> method
 * may be invoked to perform discovery of column-metadata
 * for all <a href="#config">ColumnManager-included</a> <i>Table</i>s.
 * Column metadata (for each unique column-qualifier value found) is persisted in the
 * ColumnManager Repository in the form of <a href="ColumnAuditor.html">ColumnAuditor</a> objects;
 * all such metadata is then retrievable via the
 * <a href="RepositoryAdmin.html#getColumnAuditors-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * RepositoryAdmin#getColumnAuditors</a> and
 * <a href="RepositoryAdmin.html#getColumnQualifiers-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * RepositoryAdmin#getColumnQualifiers</a> methods. Column discovery involves a full Table scan
 * (with KeyOnlyFilter), using either a direct-scan option or a mapreduce option.
 * </BLOCKQUOTE>
 * </li>
 * <li><a name="export-import"></a>HBase schema export/import tools
 * <BLOCKQUOTE>
 * The {@code RepositoryAdmin}
 * <a href="RepositoryAdmin.html#exportSchema-java.io.File-boolean-">
 * export methods</a> provide for creation of an external HBaseSchemaArchive (HSA) file (in XML
 * format*) containing the complete metadata contents (i.e., all <i>Namespace</i>, <i>Table</i>,
 * <i>Column Family</i>, <i>ColumnAuditor</i>, and  <i>ColumnDefinition</i> metadata) of either the
 * entire Repository or the user-specified <i>Namespace</i> or <i>Table</i>. Conversely, the
 * {@code RepositoryAdmin}
 * <a href="RepositoryAdmin.html#importSchema-java.io.File-boolean-">
 * import methods</a> provide for deserialization of a designated HSA file and importation of its
 * components into HBase (creating any Namespaces or Tables not already found in HBase).<br><br>
 * *An HSA file adheres to the XML Schema layout in
 * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
 * </BLOCKQUOTE>
 * </li>
 * <li>Set "maxVersions" for ColumnManager Repository
 * <BLOCKQUOTE>
 * By default, the Audit Trail subsystem (as outlined in the subsection
 * <a href="#audit-trail">"Get audit trail metadata"</a> above) is configured to track and report on
 * only the most recent 50 {@code ChangeEvent}s of each entity-attribute that it tracks (for
 * example, the most recent 50 changes to the "durability" setting of a given
 * <i>Table</i>). This limitation relates directly to the default "maxVersions" setting of the
 * <i>Column Family</i> of the Repository <i>Table</i>. This setting may be changed through
 * invocation of the static method
 * <a href="RepositoryAdmin.html#setRepositoryMaxVersions-org.apache.hadoop.hbase.client.Admin-int-">
 * RepositoryAdmin#setRepositoryMaxVersions</a>.
 * </BLOCKQUOTE>
 * </li>
 * </ul>
 */
package org.commonvox.hbase_column_manager;
