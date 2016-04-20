# ColumnManagerAPI for HBase™
*ColumnManagerAPI for <a href="http://hbase.apache.org/" target="_blank">HBase™</a>* is a **METADATA REPOSITORY SYSTEM for HBase 1.x** with options for:

1. **COLUMN AUDITING** -- captures Column metadata (qualifier and max-length) as Tables are updated (or via a discovery facility for previously-existing Tables).
2. **COLUMN-DEFINITION ENFORCEMENT** -- optionally enforces administratively-managed Column definitions (stipulating valid name, length, and/or value) as Tables are updated, optionally bringing HBase's "on-the-fly" column-qualifier creation under centralized control.
3. **SCHEMA CHANGE MONITORING** -- tracks and provides an audit trail for structural modifications made to *Namespaces*, *Tables*, and *Column Families*.
4. **SCHEMA EXPORT/IMPORT** -- provides schema (metadata) export and import facilities for HBase *Namespace*, *Table*, and all table-component structures.

**NOTE that this project is currently in final stages of initial development, with core design and coding completed. Currently, extensive test code is being added to the project. Upon final release, jars will be available at GitHub, and all artifacts will be made available from the Maven Central Repository.**