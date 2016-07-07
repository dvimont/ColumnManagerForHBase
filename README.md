# ColumnManagerAPI for HBase™
*ColumnManagerAPI for <a href="http://hbase.apache.org/" target="_blank">HBase™</a>* is an extended **METADATA REPOSITORY SYSTEM for HBase** with options for:

1. **COLUMN AUDITING** -- captures Column metadata (qualifier and max-length) as Tables are updated (or via a discovery facility for previously-existing Tables).
2. **COLUMN-DEFINITION FACILITIES** -- optionally enforces administratively-managed Column definitions (stipulating valid name, length, and/or value) as Tables are updated, optionally bringing HBase's "on-the-fly" column-qualifier creation under centralized control.
3. **SCHEMA CHANGE MONITORING** -- tracks and provides an audit trail for structural modifications made to *Namespaces*, *Tables*, and *Column Families*.
4. **SCHEMA EXPORT/IMPORT** -- provides schema (metadata) export and import facilities for HBase *Namespace*, *Table*, and all table-component structures.

A basic COMMAND-LINE INTERFACE is also provided for direct invocation of a number of the above-listed functions without any need for Java coding.

### Detailed Javadocs documentation for this project may be viewed here: http://dvimont.github.io/ColumnManagerForHBase/

**NOTE that this project is currently in final stages of initial development, with core design and coding completed. Currently, JARs compatible with HBase 1.0.x are available on GitHub, JARs compatible with HBase 1.1.x and 1.2.x are being prepared, and all artifacts will soon be published to the Maven Central Repository.**