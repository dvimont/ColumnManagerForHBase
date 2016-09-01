# ColumnManagerAPI for HBase™ ![tesseract](http://dvimont.github.io/ColumnManagerForHBase/org/commonvox/hbase_column_manager/doc-files/Tesseract_64pixels.jpg "tesseract")

*ColumnManagerAPI for <a href="http://hbase.apache.org/" target="_blank">HBase™</a>* is an extended **METADATA REPOSITORY SYSTEM for HBase** with options for:

1. **COLUMN AUDITING** -- captures Column metadata (qualifier, max-length, column-occurrences count, and cell-occurrences count) as Tables are updated (or via a discovery facility for previously-existing Tables).
2. **COLUMN ALIASING** -- a 4-byte (positive integer) column-alias is stored in each cell in place of the (often much longer) full-length column-qualifier. This works invisibly to the application developer, who continues only working with the standard hbase-client API, reading and writing full-length column-qualifiers.
3. **COLUMN-DEFINITION FACILITIES** -- optionally enforces administratively-managed Column definitions (stipulating valid name, length, and/or value) as Tables are updated, optionally bringing HBase's "on-the-fly" column-qualifier creation under centralized control.
4. **SCHEMA EXPORT/IMPORT** -- provides schema (metadata) export and import facilities for HBase *Namespace*, *Table*, and all table-component structures.
5. **SCHEMA CHANGE MONITORING** -- tracks and provides an audit trail for structural modifications made to *Namespaces*, *Tables*, and *Column Families*.

A basic COMMAND-LINE INTERFACE is also provided for direct invocation of a number of the above-listed functions without any need for Java coding.

### This project is hosted on GitHub: https://github.com/dvimont/ColumnManagerForHBase

### Detailed Javadocs documentation for this project may be viewed here: http://dvimont.github.io/ColumnManagerForHBase/

### All versions/distributions of ColumnManager for HBase (compatible with all HBase 1.x releases) are available via the Maven Central Repository: http://bit.ly/ColumnManagerMaven

![ColumnManager era comparison](http://dvimont.github.io/ColumnManagerForHBase/org/commonvox/hbase_column_manager/doc-files/ColumnManager_era_comparison.jpg "This Era is Reminiscent of Another")
