.. _howto_extract:

***************
Extract
***************

This feature allows you to extract files from any JDBC compliant database to comma delimited values files.

To extract a table of view content, we need to :

* Write a YAML file that describe the table schema
* a mustache template that describe how the table data should be extracted as files. A generic mustache template is provided below
* Run `comet extract` to apply the templated script to your database

The YAML File
################

The extract and load process are both based on the same YAML description file.
Please check first how a schema is described in See :ref:`howto_load`

The only difference is that the YAML file for the extraction process describe a table schema instead of a formatted file schema.
In that case, the following YAML fields have a special meaning:

* Domain Name : Database schema
* Schema Name : Table name


The Mustache Template
########################

Write a mustache template that run a SQL export request to the target file.
The following parameters are available :

* full_export : Boolean. If true means that we are requesting a full export
* export_file : filename of the exported data
* table_name : table name in uppercase
* delimiter : delimiter to use in the export file
* columns : Column map with the single name attribute

.. code-block:: none

    -- How the data should be exported
    SET ECHO OFF
    SET VERIFY OFF
    SET TRIMSPOOL ON
    SET TRIMOUT ON
    SET LINESIZE 9999
    SET PAGESIZE 0
    -- We decide to export without any header.
    -- Do not forget to value the withHeader property to false in the YAML file
    SET HEADING OFF
    SET FEEDBACK OFF
    SET TIMING OFF
    SET TIME OFF
    SET LONG 10000
    -- The separator here should be the one used in the YAML file
    SET COLSEP ';'
    SET HEADSEP off

    -- Stop in case of failure
    WHENEVER SQLERROR exit 1;

    -- Data time pattern we want to use
    ALTER SESSION SET NLS_DATE_FORMAT = 'yyyymmddhh24miss';

    -- The output file directory
    DEFINE OUTPUT_DIR = &1;

    -- Get current date/time.
    COLUMN DT_VAL NEW_VALUE CURRENT_EXPORT_DATE_CHAR;
    SELECT TO_CHAR(SYSDATE) DT_VAL FROM DUAL;

    -- We store in a dedicated table, the last export date/time.
    -- Useful for incremental exports
    COLUMN LED NEW_VALUE LAST_EXPORT_DATE;
    SELECT
        COALESCE(
            (
                SELECT
                    MAX(DA_LAST_EXPORT_DATE)
                FROM
                    MY_SCHEMA.COMET_EXPORT_STATUS
                WHERE
                    LI_SCHEMA_NAME = 'MY_SCHEMA' AND
                    LI_TABLE_NAME = '{{table_name}}'
            ),
            TO_DATE('19700101','yyyymmdd') -- If table has never been exported
        ) LED
    FROM
        DUAL;

    -- Start export
    PROMPT EXPORTING {{table_name}} TO &OUTPUT_DIR/{{export_file}}_{{#full_export}}FULL{{/full_export}}{{^full_export}}DELTA{{/full_export}}_&CURRENT_EXPORT_DATE_CHAR\.csv;
    SPOOL &OUTPUT_DIR/{{export_file}}_{{#full_export}}FULL{{/full_export}}{{^full_export}}DELTA{{/full_export}}_&CURRENT_EXPORT_DATE_CHAR\.csv REPLACE

    ALTER SESSION SET NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss';

    -- SQL to execute if an incremental export is requested
    {{^full_export}}
    SELECT
        {{#columns}}
        TO_CHAR({{name}}) || ';' ||
        {{/columns}}
        ''
    FROM
        MY_SCHEMA.{{table_name}}
    WHERE
        {{delta_column}} >= '&LAST_EXPORT_DATE' AND {{delta_column}} IS NOT NULL;
    {{/full_export}}

    -- SQL to execute if a full export is requested
    {{#full_export}}
    SELECT
        {{#columns}}
        TO_CHAR({{name}}) || ';' ||
        {{/columns}}
        ''
    FROM
        MY_SCHEMA.{{table_name}};
    {{/full_export}}

    -- Export finished successfully
    SPOOL OFF

    -- Update reporot table containing the last expoort date
    -- This is useful for audit purpose and for incremental export since we store the last export date here.
    BEGIN
        INSERT INTO
            MY_SCHEMA.COMET_EXPORT_STATUS (LI_SCHEMA_NAME, LI_TABLE_NAME, DA_LAST_EXPORT_DATE, TYPE_LAST_EXPORT, NB_ROWS_LAST_EXPORT)
        VALUES
            (
                'MY_SCHEMA',
                '{{table_name}}',
                TO_DATE(&CURRENT_EXPORT_DATE_CHAR),
                {{#full_export}}
                'FULL',
                (
                    SELECT
                        COUNT(*)
                    FROM
                        MY_SCHEMA.{{table_name}}
                )
                {{/full_export}}
                {{^full_export}}
                'DELTA',
                (
                    SELECT
                        COUNT(*)
                    FROM
                        MY_SCHEMA.{{table_name}}
                    WHERE
                        {{delta_column}} >= '&LAST_EXPORT_DATE' AND {{delta_column}} IS NOT NULL
                )
                {{/full_export}}
            );
    END;
    /

    EXIT SUCCESS

    sqlplus sys/Ora_db1 as SYSDBA @ EXTRACT_{{table_name}}.sql /opt/oracle/user-scripts/scripts/



The comet extract command
#########################

See the :ref:`cli_extract` CLI


