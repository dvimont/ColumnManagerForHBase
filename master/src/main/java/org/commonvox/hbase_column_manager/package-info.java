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
 * is an extended <i>METADATA REPOSITORY SYSTEM for HBase 1.x</i>
 * with options for:<br><br>
 * <BLOCKQUOTE>
 * &nbsp;&nbsp;&nbsp;&nbsp;(1) <b>COLUMN AUDITING/DISCOVERY</b> -- captures
 * <a href="#query">Column metadata</a> (qualifier and max-length) as Tables are updated, or via a
 * <a href="#discovery">discovery facility</a> for previously-existing Tables;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;(2) <b>COLUMN-DEFINITION ENFORCEMENT</b> -- optionally
 * <a href="#enforcement">enforces administratively-managed Column definitions</a>
 * (stipulating valid name, length, and/or value) as Tables are updated (bringing HBase's
 * "on-the-fly" column-qualifier creation under centralized control);<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;(3) <b>SCHEMA CHANGE MONITORING</b> -- tracks and provides an
 * <a href="#auditing">audit trail</a> for structural modifications made to
 * <i>Namespaces</i>, <i>Tables</i>, and <i>Column Families</i>;<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;(4) <b>SCHEMA EXPORT/IMPORT</b> -- provides
 * <a href="#export-import">schema (metadata) export and import facilities</a>
 * for HBase <i>Namespace</i>, <i>Table</i>, and all table-component structures.<br><br>
 *
 * Once it is installed and configured, standard usage of the ColumnManagerAPI in Java programs is
 * accomplished by simply substituting any reference to the standard HBase {@code ConnectionFactory}
 * class with a reference to the ColumnManager
 * <a href="MConnectionFactory.html">MConnectionFactory</a> class (as shown in the
 * <a href="#usage">USAGE section</a> below).<br>
 * All other interactions with the HBase API are then to be coded as usual; ColumnManager will work
 * behind the scenes to capture HBase metadata as stipulated by an administrator/developer in
 * <a href="#config">the ColumnManager configuration</a>.<br><br>
 * Any application coded with the ColumnManager API can be made to revert to standard HBase API
 * functionality simply by either (a) removing the {@code column_manager.activated} property from
 * all {@code hbase-*.xml} configuration files, or (b) by setting the value of that property to
 * {@code <false>}.<br>
 * Thus, a ColumnManager-coded application can be used with ColumnManager activated in a development
 * and/or staging environment, but deactivated in production (where ColumnManager's extra overhead
 * might be undesirable).
 * </BLOCKQUOTE>
 * <i>HBase™ is a trademark of the <a href="http://www.apache.org/" target="_blank">
 * Apache Software Foundation</a>.</i><br><br>
 * <hr><b>UPCOMING ENHANCEMENTS MAY INCLUDE:</b>
 * <ul>
 * <li><b>GUI interface:</b>
 * A JavaFX-based GUI interface may be built atop the ColumnManagerAPI, for administrative use on
 * Mac, Linux, and Windows desktops.
 * </li>
 * <li><b>Metadata discovery via MapReduce:</b>
 * A MapReduce-based infrastructure will provide for much more efficient Column metadata discovery.
 * (The current discovery facility performs full <i>Table</i> scans [with KeyOnlyFilter] via the
 * HBase API).
 * </li>
 * <li><b>Capture/discovery of additional schema-oriented metadata:</b>
 * Users may identify additional schema-oriented metadata that would be worthwhile to capture or
 * discover using ColumnManager mechanisms.
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
 * <li><a href="#usage">USAGE</a></li>
 * <li><a href="#query">QUERIES</a></li>
 * <li><a href="#admin">ADMINISTRATIVE TOOLS</a>
 * </ol>
 *
 * <a name="prereq"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>I. <u>PREREQUISITES</u></b>
 * <BLOCKQUOTE>
 * <b>HBase 1.x</b> -- HBase must be installed as per the installation instructions given in the
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
 * <b>JDK 7</b> -- HBase 1.x (upon which this package is dependent) requires JDK 7 or later.
 * </BLOCKQUOTE>
 * <a name="install"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>II. <u>INSTALLATION</u></b>
 * <BLOCKQUOTE>
 * <b>Step 1: Get the required JAR files</b>
 * <br>
 * The most recently released version of
 * <b><a href="https://github.com/dvimont/ColumnManager/releases" target="_blank">
 * the JAR file for ColumnManager</a></b>
 * may be downloaded from GitHub and included in the IDE environment's compile and run-time
 * classpath configurations (just as the HBase API libraries are already included).
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
 * the property above is not present in {@code <hbase-site.xml>}, the ColumnManager API will
 * function exactly like the standard HBase API. Thus, a single body of code can operate <i>with</i>
 * ColumnManager functionality in one environment (typically, a development environment) and can
 * completely <i>bypass</i>
 * ColumnManager functionality in another environment (potentially testing, staging, production),
 * with the only difference between the environments being the presence or absence of the
 * "{@code column_manager.activated}" property in each environment's {@code hbase-*.xml}
 * configuration files.
 * <br>
 * <br>
 * <b>Step 3: Confirm installation (and create ColumnManager repository <i>Namespace</i> and
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
 * creation of the ColumnManager repository
 * <i>Namespace</i> ("{@code column_manager_repository_namespace}") and <i>Table</i>
 * ("{@code column_manager_repository_table}").<br>
 * If the code above runs successfully, its log output will include a number of lines of Zookeeper
 * INFO output, as well as several lines of ColumnManager INFO output.
 * <br>
 * <br>
 * <b>Step 4: [OPTIONAL] Explicitly create repository structures</b>
 * <br>
 * As an alternative to the automatic creation of the ColumnManager repository
 * <i>Namespace</i> and <i>Table</i> in the preceding step, these structures may be explicitly
 * created through invocation of the static {@code RepositoryAdmin} method
 * <a href="RepositoryAdmin.html#installRepositoryStructures-org.apache.hadoop.hbase.client.Admin-">
 * installRepositoryStructures</a>. Successful creation of these structures will result in messages
 * such as the following appearing in the session's log output:
 * <pre>{@code      2015-10-09 11:03:30,184 INFO  [main] commonvox.column_manager: ColumnManager Repository Namespace has been created ...
 *      2015-10-09 11:03:31,498 INFO  [main] commonvox.column_manager: ColumnManager Repository Table has been created ...}</pre>
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
 * uninstallRepositoryStructures</a> to disable and delete the repository table and to drop the
 * repository namespace.
 * </BLOCKQUOTE>
 *
 * <a name="config"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>IV. <u>CONFIGURATION OPTIONS</u></b>
 * <BLOCKQUOTE>
 * <b>A. INCLUDE/EXCLUDE TABLES FOR ColumnManager PROCESSING</b>
 * <br>
 * By default, when ColumnManager is installed and <a href="#activate">activated</a>, all user
 * Tables are included in ColumnManager processing. However, the following options are available
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
 *         <value>myNamespace:*,goodNamespace:myTable,betterNamespace:yetAnotherTable</value>
 *      </property>}</pre>
 * Note that all <i>Tables</i> in a given Namespace may be excluded by using an
 * asterisk {@code [*]} symbol in the place of a specific <i>Table</i> qualifier,
 * as in the example above which excludes all Tables in the
 * "myNamespace" namespace via the specification, [{@code myNamespace:*}].<br><br>
 * Note also that if a {@code [column_manager.includedTables]} property is found in the
 * {@code <hbase-*.xml>}
 * files, then any {@code [column_manager.excludedTables]} property will be ignored.
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
 * <b>Manage <a href="ColumnDefinition.html">ColumnDefinitions</a></b>: The
 * {@code ColumnDefinition}s of a <i>Column Family</i> are managed via a number of RepositoryAdmin
 * <a href="RepositoryAdmin.html#addColumnDefinition-org.apache.hadoop.hbase.TableName-byte:A-org.commonvox.hbase_column_manager.ColumnDefinition-">
 * add</a>,
 * <a href="RepositoryAdmin.html#getColumnDefinitions-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * get</a>, and
 * <a href="RepositoryAdmin.html#deleteColumnDefinition-org.apache.hadoop.hbase.TableName-byte:A-byte:A-">
 * delete</a> methods.<br><br>
 * <b>Enable enforcement of <a href="ColumnDefinition.html">ColumnDefinitions</a></b>: Enforcement
 * of the {@code ColumnDefinition}s of a given <i>Column Family</i> does not occur until explicitly
 * enabled via the RepositoryAdmin method
 * <a href="RepositoryAdmin.html#setColumnDefinitionsEnforced-boolean-org.apache.hadoop.hbase.TableName-byte:A-">
 * setColumnDefinitionsEnforced</a>. This same method may be invoked to toggle enforcement
 * {@code off} again for the <i>Column Family</i>.<br><br>
 * When enforcement is enabled, then (a) any <i>Column Qualifier</i> submitted in a {@code put}
 * (i.e., insert/update) to the <i>Table:Column-Family</i> must correspond to an existing
 * {@code ColumnDefinition} of the <i>Column Family</i>, and (b) the corresponding <i>Column
 * value</i>
 * submitted must pass all validations (if any) stipulated by the {@code ColumnDefinition}. Any
 * {@code ColumnDefinition}-related enforcement-violation encountered during processing of a
 * {@code put} transaction will result in a
 * <a href="ColumnManagerIOException.html">ColumnManagerIOException</a>
 * (a subclass of the standard {@code IOException} class) being thrown: specifically, either a
 * <a href="ColumnDefinitionNotFoundException.html">ColumnDefinitionNotFoundException</a> or an
 * <a href="InvalidColumnValueException.html">InvalidColumnValueException</a>.
 * </BLOCKQUOTE>
 * </BLOCKQUOTE>
 *
 * <a name="usage"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>V. <u>USAGE</u></b>
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
 * all standard HBase API functionality) transparently interface with the ColumnManager repository
 * for tracking and persisting of
 * <i>Namespace</i>, <i>Table</i>, <i>Column Family</i>, and
 * <i><a href="ColumnAuditor.html">ColumnAuditor</a></i> metadata. In addition,
 * ColumnManager-enabled {@code HTableMultiplexer} instances may be obtained via the
 * {@code RepositoryAdmin} method
 * <a href="RepositoryAdmin.html#createHTableMultiplexer-int-">createHTableMultiplexer</a>.
 * </BLOCKQUOTE>
 * <b>B. OPTIONALLY CATCH {@code ColumnManagerIOException} OCCURRENCES</b>
 * <BLOCKQUOTE>
 * In the context of certain applications, it may be necessary to perform special processing when a
 * <a href="ColumnManagerIOException.html">ColumnManagerIOException</a> is thrown, signifying
 * rejection of a specific <i>Column</i> entry submitted in a {@code put} (i.e., insert/update) to
 * an
 * <a href="RepositoryAdmin.html#setColumnDefinitionsEnforced-boolean-org.apache.hadoop.hbase.TableName-byte:A-">
 * enforcement-enabled</a> <i>Table/Column-Family</i>. In such cases, exceptions of this abstract
 * type (or its concrete subclasses) may be caught, and appropriate processing performed.
 * </BLOCKQUOTE>
 * </BLOCKQUOTE>
 *
 * <a name="query"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>VI. <u>QUERYING THE COLUMN-MANAGER REPOSITORY</u></b>
 * <ul>
 * <li>Get <i>Column Qualifier</i> names and additional column metadata:
 * <BLOCKQUOTE>
 * A list of the <i>Column Qualifier</i>s belonging to a <i>Column Family</i>
 * of a <i>Table</i> is obtained via the {@code RepositoryAdmin} method
 * <a href="RepositoryAdmin.html#getColumnQualifiers-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * getColumnQualifiers</a>.<br>
 * Alternatively, a list of <a href="ColumnAuditor.html">ColumnAuditor</a> objects (containing
 * column qualifiers and additional column metadata) is obtained via the {@code RepositoryAdmin}
 * method
 * <a href="RepositoryAdmin.html#getColumnAuditors-org.apache.hadoop.hbase.HTableDescriptor-org.apache.hadoop.hbase.HColumnDescriptor-">
 * getColumnAuditors</a>.
 * </BLOCKQUOTE>
 * </li>
 * <li><a name="auditing"></a>Get audit trail metadata:<br>
 * <BLOCKQUOTE>
 * A <a href="ChangeEventMonitor.html">ChangeEventMonitor</a> object (obtained via the
 * {@code RepositoryAdmin} method
 * <a href="RepositoryAdmin.html#getChangeEventMonitor--">getChangeEventMonitor</a>) outputs lists
 * of <a href="ChangeEvent.html">ChangeEvents</a>
 * (pertaining to structural changes made to user <i>Namespaces</i>, <i>Tables</i>,
 * <i>Column Families</i>, <i>ColumnAuditors</i>, and <i>ColumnDefinitions</i>) tracked by the
 * ColumnManager repository.<br>
 * The ChangeEventMonitor's "get" methods allow for retrieving {@link ChangeEvent}s grouped and
 * ordered in various ways, and a static convenience method,
 * <a href="ChangeEventMonitor.html#exportChangeEventListToCsvFile-java.util.List-java.lang.String-java.lang.String-">
 * exportChangeEventListToCsvFile</a>, is provided for outputting a list of {@code ChangeEvent}s to
 * a CSV file.
 * </BLOCKQUOTE>
 * </li>
 * </ul>
 *
 * <a name="admin"></a>
 * <hr style="height:3px;color:black;background-color:black">
 * <b>VII. <u>ADMINISTRATIVE TOOLS</u></b>
 * <ul>
 * <li><a name="discovery"></a>HBase metadata discovery tools
 * <BLOCKQUOTE>
 * When ColumnManager is installed into an already-populated HBase environment, the
 * {@code RepositoryAdmin} method
 * <a href="RepositoryAdmin.html#discoverMetadata--">discoverMetadata</a>
 * may be invoked to perform discovery of all ColumnManager-included user <i>Namespaces</i> and
 * <i>Tables</i> and store pertinent metadata in the repository. The method includes discovery of
 * <a href="ColumnAuditor.html">ColumnAuditor</a> metadata (via a full scan of the Tables [with
 * KeyOnlyFilter]).
 * </BLOCKQUOTE>
 * </li>
 * <li><a name="export-import"></a>HBase schema export/import tools
 * <BLOCKQUOTE>
 * The {@code RepositoryAdmin}
 * <a href="RepositoryAdmin.html#exportRepository-java.io.File-boolean-">
 * export methods</a> provide for creation of an external HBaseSchemaArchive (HSA) file (in XML
 * format*) containing the complete metadata contents (i.e., all <i>Namespace</i>, <i>Table</i>,
 * <i>Column Family</i>, <i>ColumnAuditor</i>, and  <i>ColumnDefinition</i> metadata) of either the
 * entire repository or the user-specified <i>Namespace</i> or <i>Table</i>. Conversely, the
 * {@code RepositoryAdmin}
 * <a href="RepositoryAdmin.html#importSchema-boolean-java.io.File-">
 * import methods</a> provide for deserialization of a designated HSA file and importation of its
 * components into HBase (creating any Namespaces or Tables not already found in HBase).<br><br>
 * *An HSA file adheres to the XML Schema layout in
 * <a href="doc-files/HBaseSchemaArchive.xsd.xml" target="_blank">HBaseSchemaArchive.xsd.xml</a>.
 * </BLOCKQUOTE>
 * </li>
 * <li>Set "maxVersions" for ColumnManager repository
 * <BLOCKQUOTE>
 * By default, the Audit Trail subsystem (as outlined in the section
 * <a href="#auditing">"Get audit trail metadata"</a> above) is configured to track and report on
 * only the most recent 50 {@code ChangeEvent}s of each entity-attribute that it tracks (for
 * example, the most recent 50 changes to the "durability" setting of a given
 * <i>Table</i>). This limitation relates directly to the default "maxVersions" setting of the
 * <i>Column Family</i> of the repository <i>Table</i>. This setting may be changed through
 * invocation of the static {@code RepositoryAdmin} method
 * <a href="RepositoryAdmin.html#setRepositoryMaxVersions-org.apache.hadoop.hbase.client.Admin-int-">
 * setRepositoryMaxVersions</a>.
 * </BLOCKQUOTE>
 * </li>
 * </ul>
 */
package org.commonvox.hbase_column_manager;
