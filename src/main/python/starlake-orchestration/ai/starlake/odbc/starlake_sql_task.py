from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Tuple

from ai.starlake.common import MissingEnvironmentVariable
from ai.starlake.job import StarlakeOptions, TaskType

from ai.starlake.odbc import Session, SessionProvider

from enum import Enum

class SQLTask(ABC, StarlakeOptions):

    def __init__(self, sink: str, caller_globals: dict = dict(), arguments: list = [], options: dict = dict(), task_type: Optional[TaskType]=None, **kwargs):
        """Initialize the SQL task.
        Args:
            sink (str): The sink.
            caller_globals (dict, optional): The caller globals. Defaults to dict().
            arguments (list, optional): The arguments of the task. Defaults to [].
            options (dict, optional): The options of the task. Defaults to dict().
            task_type (Optional[TaskType], optional): The task type. Defaults to None.
        """
        super().__init__(**kwargs)
        self.sink = sink
        domainAndTable = sink.split('.')
        self.domain = domainAndTable[0]
        self.table = domainAndTable[-1]

        self.caller_globals = caller_globals

        self.statements: dict = self.caller_globals.get('statements', dict()).get(sink, None)
        self.audit: dict = self.caller_globals.get('audit', dict())
        self.expectations: dict = self.caller_globals.get('expectations', dict())
        self.expectation_items: dict = self.caller_globals.get('expectation_items', dict()).get(sink, None)

        self.options = options
        self.sl_env_vars = __class__.get_sl_env_vars(self.options)
        self.sl_root = __class__.get_sl_root(self.options)
        self.sl_datasets = __class__.get_sl_datasets(self.options)

        self.arguments = arguments
        task_options = self.sl_env_vars.copy()
        for index, arg in enumerate(arguments):
            if arg == "--options" and arguments.__len__() > index + 1:
                opts = arguments[index+1]
                if opts.strip().__len__() > 0:
                    self.task_options.update({
                        key: value
                        for opt in opts.split(",")
                        if "=" in opt  # Only process valid key=value pairs
                        for key, value in [opt.split("=")]
                    })
                break
        self.safe_params = defaultdict(lambda: 'NULL', task_options)
        self.__task_type = task_type

    @property
    def task_type(self) -> TaskType:
        """Get the task type.
        Returns:
            TaskType: The task type.
        """
        return self.__task_type

    def bindParams(self, stmt: str) -> str:
        """Bind parameters to the SQL statement.
        Args:
            stmt (str): The SQL statement.
        Returns:
            str: The SQL statement with the parameters bound
        """
        return stmt.format_map(self.safe_params)

    def str_to_bool(self, value: str) -> bool:
        """Convert a string to a boolean.
        Args:
            value (str): The string to convert.
        Returns:
            bool: The boolean value.
        """
        truthy = {'yes', 'y', 'true', '1'}
        falsy = {'no', 'n', 'false', '0'}

        value = value.strip().lower()
        if value in truthy:
            return True
        elif value in falsy:
            return False
        raise ValueError(f"Valeur invalide : {value}")

    def execute_sql(self, session: Session, sql: Optional[str], message: Optional[str] = None, dry_run: bool = False) -> List[Any]:
        """Execute the SQL.
        Args:
            session (Session): The Starlake session.
            sql (str): The SQL query to execute.
            message (Optional[str], optional): The optional message. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            List[Any]: The rows.
        """
        if sql:
            stmt: str = self.bindParams(sql)
            check_stmt = stmt.strip().upper()
            if check_stmt.startswith("USE SCHEMA ") and session.provider() != SessionProvider.SNOWFLAKE:
                schema = stmt.strip().split()[-1]
                if session.provider() in [SessionProvider.REDSHIFT, SessionProvider.POSTGRES]:
                    stmt = f"SET search_path TO {schema}"
                else:
                    stmt = f"USE `{session.database}.{schema}`"
            elif session.provider() == SessionProvider.BIGQUERY and (check_stmt.startswith("BEGIN") or check_stmt.startswith("COMMIT") or check_stmt.startswith("ROLLBACK")):
                stmt = f"-- {stmt} not supported with BigQuery provider"
            elif dry_run and message:
                print(f"-- {message}")
            if dry_run:
                print(f"{stmt};")
                return []
            else:
                try:
                    rows = session.sql(stmt)
                    return rows
                except Exception as e:
                    raise Exception(f"Error executing SQL {stmt}: {str(e)}")
        else:
            return []

    def execute_sqls(self, session: Session, sqls: List[str], message: Optional[str] = None, dry_run: bool = False) -> None:
        """Execute the SQLs.
        Args:
            session (Session): The Starlake session.
            sqls (List[str]): The SQLs.
            message (Optional[str], optional): The optional message. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if sqls:
            if dry_run and message:
                print(f"-- {message}")
            for sql in sqls:
                self.execute_sql(session, sql, None, dry_run)

    def check_if_table_exists(self, session: Session, domain: str, table: str) -> bool:
        """Check if the table exists.
        Args:
            session (Session): The Starlake session.
            domain (str): The domain.
            table (str): The table.
            Returns:
            bool: True if the table exists, False otherwise.
        """
        dataset = (domain + '.') if session.provider() == SessionProvider.BIGQUERY else ''
        query=f"SELECT * FROM {dataset}INFORMATION_SCHEMA.TABLES WHERE LOWER(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)) LIKE '{domain}.{table}'"
        return self.execute_sql(session, query, f"Check if table {domain}.{table} exists:", False).__len__() > 0

    def check_if_audit_table_exists(self, session: Session, dry_run: bool = False) -> bool:
        """Check if the audit table exists.
        Args:
            session (Session): The Starlake session.
            Returns:
            bool: True if the audit table exists, False otherwise.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if self.audit:
            try:
                # create SQL domain
                domain = self.audit.get('domain', ['audit'])[0]
                self.create_domain_if_not_exists(session, domain, dry_run)
                # execute SQL preActions
                self.execute_sqls(session, self.audit.get('preActions', []), "Execute audit pre action:", dry_run)
                # check if the audit table exists
                if not self.check_if_table_exists(session, domain, 'audit'):
                    # execute SQL createSchemaSql
                    sqls: List[str] = self.audit.get('createSchemaSql', [])
                    if sqls:
                        self.execute_sqls(session, sqls, "Create audit table", dry_run)
                    return True
                else:
                    return True
            except Exception as e:
                print(f"-- Error creating audit table: {str(e)}")
                return False
        else:
            return False

    def check_if_expectations_table_exists(self, session: Session, dry_run: bool = False) -> bool:
        """Check if the expectations table exists.
        Args:
            session (Session): The Starlake session.
            Returns:
            bool: True if the expectations table exists, False otherwise.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if self.expectations:
            try:
                # create SQL domain
                domain = self.expectations.get('domain', ['audit'])[0]
                self.create_domain_if_not_exists(session, domain, dry_run)
                # check if the expectations table exists
                if not self.check_if_table_exists(session, domain, 'expectations'):
                    # execute SQL createSchemaSql
                    self.execute_sqls(session, self.expectations.get('createSchemaSql', []), "Create expectations table", dry_run)
                    return True
                else:
                    return True
            except Exception as e:
                print(f"-- Error creating expectations table: {str(e)}")
                return False
        else:
            return False

    def log_audit(self, session: Session, domain: str, table: str, paths: Optional[str], count: int, countAccepted: int, countRejected: int, success: bool, duration: int, message: str, ts: datetime, jobid: Optional[str] = None, step: Optional[str] = None, dry_run: bool = False) -> bool :
        """Log the audit record.
        Args:
            session (Session): The Starlake session.
            domain (str): The domain.
            table (str): The table.
            paths (Optional[str], optional): The optional paths. Defaults to None.
            count (int): The count.
            countAccepted (int): The count accepted.
            countRejected (int): The count rejected.
            success (bool): The success.
            duration (int): The duration.
            message (str): The message.
            ts (datetime): The timestamp.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            step (Optional[str], optional): The optional step. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            bool: True if the audit record was logged, False otherwise.
        """
        if self.audit and self.check_if_audit_table_exists(session, dry_run):
            audit_domain = self.audit.get('domain', ['audit'])[0]
            audit_sqls = self.audit.get('mainSqlIfExists', None)
            if audit_sqls:
                try:
                    audit_sql = audit_sqls[0]
                    formatted_sql = audit_sql.format(
                        jobid = jobid or f'{domain}.{table}',
                        paths = paths or table,
                        domain = domain,
                        schema = table,
                        success = str(success),
                        count = str(count),
                        countAccepted = str(countAccepted),
                        countRejected = str(countRejected),
                        timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                        duration = str(duration),
                        message = message,
                        step = step or "TRANSFORM",
                        database = "",
                        tenant = ""
                    )
                    insert_sql = f"INSERT INTO {audit_domain}.audit {formatted_sql}"
                    self.execute_sql(session, insert_sql, "Insert audit record:", dry_run)
                    return True
                except Exception as e:
                    print(f"-- Error inserting audit record: {str(e)}")
                    return False
            else:
                return False
        else:
            return False

    def log_expectation(self, session: Session, domain:str, table: str, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime, jobid: Optional[str] = None, dry_run: bool = False) -> bool :
        """Log the expectation record.
        Args:
            session (Session): The Starlake session.
            success (bool): whether the expectation has been successfully checked or not.
            name (str): The name of the expectation.
            params (str): The params for the expectation.
            sql (str): The SQL.
            count (int): The count.
            exception (str): The exception.
            ts (datetime): The timestamp.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            bool: True if the expectation record was logged, False otherwise.
        """
        if self.expectations and self.check_if_expectations_table_exists(session, dry_run):
            expectation_domain = self.expectations.get('domain', ['audit'])[0]
            expectation_sqls = self.expectations.get('mainSqlIfExists', None)
            if expectation_sqls:
                try:
                    expectation_sql = expectation_sqls[0]
                    formatted_sql = expectation_sql.format(
                        jobid = jobid or f'{domain}.{table}',
                        database = "",
                        domain = domain,
                        schema = table,
                        count = count,
                        exception = exception,
                        timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                        success = str(success),
                        name = name,
                        params = params,
                        sql = sql
                    )
                    insert_sql = f"INSERT INTO {expectation_domain}.expectations {formatted_sql}"
                    self.execute_sql(session, insert_sql, "Insert expectations record:", dry_run)
                    return True
                except Exception as e:
                    print(f"-- Error inserting expectations record: {str(e)}")
                    return False
            else:
                return False
        else:
            return False

    def run_expectation(self, session: Session, name: str, params: str, query: str, failOnError: bool = False, jobid: Optional[str] = None, dry_run: bool = False) -> None:
        """Run the expectation.
        Args:
            session (Session): The Starlake session.
            name (str): The name of the expectation.
            params (str): The params for the expectation.
            query (str): The query.
            failOnError (bool, optional): Whether to fail on error. Defaults to False.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        count = 0
        try:
            if query:
                rows = self.execute_sql(session, query, f"Run expectation {name}:", dry_run)
                if rows.__len__() != 1:
                    if not dry_run:
                        raise Exception(f'Expectation failed for {self.sink}: {query}. Expected 1 row but got {rows.__len__()}')
                else:
                    count = rows[0][0]
                #  log expectations as audit in expectation table here
                if count != 0:
                    raise Exception(f'Expectation failed for {self.sink}: {query}. Expected count to be equal to 0 but got {count}')
                self.log_expectation(session=session, domain=self.domain, table=self.table,success=True, name=name, params=params, sql=query, count=count, exception="", ts=datetime.now(), jobid=jobid, dry_run=dry_run)
            else:
                raise Exception(f'Expectation failed for {self.sink}: {name}. Query not found')
        except Exception as e:
            print(f"-- Error running expectation {name}: {str(e)}")
            self.log_expectation(session=session, domain=self.domain, table=self.table,success=False, name=name, params=params, sql=query, count=count, exception=str(e), ts=datetime.now(), jobid=jobid, dry_run=dry_run)
            if failOnError and not dry_run:
                raise e

    def run_expectations(self, session: Session, jobid: Optional[str] = None, dry_run: bool = False) -> None:
        """Run the expectations.
        Args:
            session (Session): The Starlake session.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if self.expectation_items and self.check_if_expectations_table_exists(session, dry_run):
            for expectation in self.expectation_items:
                self.run_expectation(session, expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), self.str_to_bool(expectation.get('failOnError', 'no')), jobid, dry_run)

    def begin_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Begin the transaction.
        Args:
            session (Session): The Starlake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "BEGIN", "BEGIN transaction:", dry_run)

    def create_domain_if_not_exists(self, session: Session, domain: str, dry_run: bool = False) -> None:
        """Create the schema if it does not exist.
        Args:
            session (Session): The Starlake session.
            domain (str): The domain.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, f"CREATE SCHEMA IF NOT EXISTS {domain}", f"Create schema {domain} if not exists:", dry_run)

    def enable_change_tracking(self, session: Session, sink: str, dry_run: bool = False) -> None:
        """Enable change tracking.
        Args:
            session (Session): The Starlake session.
            sink (str): The sink.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, f"ALTER TABLE {self.sink} SET CHANGE_TRACKING = TRUE", "Enable change tracking:", dry_run)

    def commit_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Commit the transaction.
        Args:
            session (Session): The Starlake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "COMMIT", "COMMIT transaction:", dry_run)

    def rollback_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Rollback the transaction.
        Args:
            session (Session): The Starlake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "ROLLBACK", "ROLLBACK transaction:", dry_run)

    @abstractmethod
    def execute(self, session: Session, jobid: Optional[str] = None, config: dict = dict(), dry_run: bool = False) -> None:
        """Execute the task.
        Args:
            session (Session): The Starlake session.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            config (dict, optional): The config. Defaults to dict().
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        ...

class SQLEmptyTask(SQLTask):
    def __init__(self, sink: str, caller_globals: dict = dict(), arguments: list = [], options: dict = dict(), task_type: Optional[TaskType]=TaskType.EMPTY, **kwargs):
        """Initialize the SQL empty task.
        Args:
            sink (str): The sink.
            caller_globals (dict, optional): The caller globals. Defaults to dict().
            arguments (list, optional): The arguments of the task. Defaults to [].
            options (dict, optional): The options. Defaults to dict().
            task_type (Optional[TaskType], optional): The task type. Defaults to TaskType.EMPTY.
        """
        super().__init__(sink, caller_globals, arguments, options, task_type or TaskType.EMPTY, **kwargs)

    def execute(self, session: Session, jobid: Optional[str] = None, config: dict = dict(), dry_run: bool = False) -> None:
        """Execute the empty task.
        Args:
            session (Session): The Starlake session.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            config (dict, optional): The config. Defaults to dict().
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, f"SELECT '{self.sink}'", f"Execute {self.task_type} task:", dry_run)

class SQLLoadTask(SQLTask, StarlakeOptions):

    def __init__(self, sink: str, caller_globals: dict = dict(), arguments: list = [], options: dict = dict(), **kwargs):
        """Initialize the SQL load task.
        Args:
            sink (str): The sink.
            caller_globals (dict, optional): The caller globals. Defaults to dict().
            arguments (list, optional): The arguments of the task. Defaults to [].
            options (dict, optional): The options. Defaults to dict().
        """
        super().__init__(sink, caller_globals, arguments, options, TaskType.LOAD, **kwargs)
        json_context = self.caller_globals.get('json_context', '{}')
        import json
        context = json.loads(json_context).get(sink, None)
        if not context:
            raise ValueError("load context not found")

        try:
            self._sl_incoming_file_stage = kwargs.get('sl_incoming_file_stage', __class__.get_context_var(var_name='sl_incoming_file_stage', options=self.options))
        except MissingEnvironmentVariable:
            self._sl_incoming_file_stage = None
        temp_stage = self.sl_incoming_file_stage or context.get('tempStage', None)
        if not temp_stage:
            raise ValueError(f"Temp stage for {self.sink} not found")
        else:
            self.temp_stage = temp_stage

        self.context_schema: dict = context.get('schema', dict())

        pattern: str = self.context_schema.get('pattern', None)
        if not pattern:
            raise ValueError(f"Pattern for {self.sink} not found")
        else:
            self.pattern = pattern

        self.metadata: dict = self.context_schema.get('metadata', dict())

        format: str = self.metadata.get('format', None)
        if not format:
            raise ValueError(f"Format for {self.sink} not found")
        else:
            self.format = format.upper()

        self.metadata_options: dict = self.metadata.get("options", dict())

        self.compression = self.is_true(self.get_option("compression", None), True)
        if self.compression:
            self.compression_format = "COMPRESSION = GZIP" 
        else:
            self.compression_format = "COMPRESSION = NONE"

        null_if = self.get_option('NULL_IF', None)
        if not null_if and self.is_true(self.metadata.get('emptyIsNull', 'false'), False):
            self.null_if = "NULL_IF = ('')"
        elif null_if:
            self.null_if = f"NULL_IF = {null_if}"
        else:
            self.null_if = ""

        purge = self.get_option("PURGE", None)
        if not purge:
            self.purge = "FALSE"
        else:
            self.purge = purge.upper()

    def get_option(self, key: str, metadata_key: Optional[str]) -> Optional[str]:
        if self.metadata_options and key.lower() in self.metadata_options:
            return self.metadata_options.get(key.lower(), None)
        elif metadata_key and self.metadata.get(metadata_key, None):
            return self.metadata[metadata_key].replace('\\', '\\\\')
        return None

    def is_true(self, value: str, default: bool) -> bool:
        if value is None:
            return default
        return value.lower() == "true"

    def get_audit_info(self, rows: List[Tuple]) -> Tuple[str, str, str, int, int, int]:
        if rows.__len__() == 0:
            return '', '', '', -1, -1, -1
        else:
            files = []
            first_error_lines = []
            first_error_column_names = []
            rows_parsed = 0
            rows_loaded = 0
            errors_seen = 0
            for row in rows:
                row_dict = {k: v for k, v in row}
                file = row_dict.get('file', None)
                if file:
                    files.append(file)
                first_error_line=row_dict.get('first_error_line', None)
                if first_error_line:
                    first_error_lines.append(first_error_line)
                first_error_column_name=row_dict.get('first_error_column_name', None)
                if first_error_column_name:
                    first_error_column_names.append(first_error_column_name)
                rows_parsed += row_dict.get('rows_parsed', 0)
                rows_loaded += row_dict.get('rows_loaded', 0)
                errors_seen += row_dict.get('errors_seen', 0)
            return ','.join(files), ','.join(first_error_lines), ','.join(first_error_column_names), rows_parsed, rows_loaded, errors_seen

    def copy_extra_options(self, common_options: list[str]):
        extra_options = ""
        if self.metadata_options:
            for k, v in self.metadata_options.items():
                if not k in common_options:
                    extra_options += f"{k} = {v}\n"
        return extra_options

    def schema_as_dict(self, schema_string: str) -> dict:
        tableNativeSchema = map(lambda x: (x.split()[0].strip(), x.split()[1].strip()), schema_string.replace("\"", "").split(","))
        tableSchemaDict = dict(map(lambda x: (x[0].lower(), x[1].lower()), tableNativeSchema))
        return tableSchemaDict

    def add_columns_from_dict(self, dictionary: dict):
        return [f"ALTER TABLE IF EXISTS {self.domain}.{self.table} ADD COLUMN IF NOT EXISTS {k} {v};" for k, v in dictionary.items()]

    def drop_columns_from_dict(dictionary: dict):
        # In the current version, we do not drop any existing columns for backward compatibility
        # return [f"ALTER TABLE IF EXISTS {self.domain}.{self.table} DROP COLUMN IF EXISTS {k};" for k, v in dictionary.items()]
        return []    

    def update_table_schema(self, session: Session, dry_run: bool) -> bool:
        existing_schema_sql = f"select column_name, data_type from information_schema.columns where table_schema ilike '{self.domain}' and table_name ilike '{self.table}';"
        rows = self.execute_sql(session, existing_schema_sql, f"Retrieve existing schema for {self.domain}.{self.table}", False)
        existing_columns = []
        for row in rows:
            existing_columns.append((str(row[0]).lower(), str(row[1]).lower()))
        existing_schema = dict(existing_columns)
        if dry_run:
            print(f"-- Existing schema for {self.domain}.{self.table}: {existing_schema}")
        schema_string = self.statements.get("schemaString", "") 
        if schema_string.strip() == "":
            return False
        new_schema = self.schema_as_dict(schema_string)
        new_columns = set(new_schema.keys()) - set(existing_schema.keys())
        old_columns = set(existing_schema.keys()) - set(new_schema.keys())
        nb_new_columns = new_columns.__len__()
        nb_old_columns = old_columns.__len__()
        update_required = nb_new_columns + nb_old_columns > 0
        if not update_required:
            if dry_run:
                print(f"-- No schema update required for {self.domain}.{self.table}")
            return False
        new_columns_dict = {key: new_schema[key] for key in new_columns}
        old_columns_dict = {key: existing_schema[key] for key in old_columns}
        alter_columns = self.add_columns_from_dict(new_columns_dict)
        self.execute_sqls(session, alter_columns, "Add columns", dry_run)

        old_columns_dict = {key: existing_schema[key] for key in old_columns}
        drop_columns = self.drop_columns_from_dict(old_columns_dict)
        self.execute_sqls(session, drop_columns, "Drop columns", dry_run)

        return True

    def build_copy_csv(self, provider: SessionProvider) -> str:
        skipCount = self.get_option('SKIP_HEADER', None)

        if not skipCount and self.is_true(self.metadata.get('withHeader', 'false'), False):
            skipCount = '1'

        common_options = [
            'SKIP_HEADER', 
            'NULL_IF', 
            'FIELD_OPTIONALLY_ENCLOSED_BY', 
            'FIELD_DELIMITER',
            'ESCAPE_UNENCLOSED_FIELD', 
            'ENCODING'
        ]
        extra_options = self.copy_extra_options(common_options)
        if self.compression:
            extension = ".gz"
        else:
            extension = ""
        sql = f'''
            COPY INTO {self.sink} 
            FROM @{self.temp_stage}/{self.domain}/
            PATTERN = "{self.pattern}{extension}"
            PURGE = {self.purge}
            FILE_FORMAT = (
                TYPE = CSV
                ERROR_ON_COLUMN_COUNT_MISMATCH = false
                SKIP_HEADER = {skipCount} 
                FIELD_OPTIONALLY_ENCLOSED_BY = '{self.get_option('FIELD_OPTIONALLY_ENCLOSED_BY', 'quote')}' 
                FIELD_DELIMITER = '{self.get_option('FIELD_DELIMITER', 'separator')}' 
                ESCAPE_UNENCLOSED_FIELD = '{self.get_option('ESCAPE_UNENCLOSED_FIELD', 'escape')}' 
                ENCODING = '{self.get_option('ENCODING', 'encoding')}'
                {self.null_if}
                {extra_options}
                {self.compression_format}
            )
        '''
        return sql

    def build_copy_json(self, provider: SessionProvider) -> str:
        strip_outer_array = self.get_option("STRIP_OUTER_ARRAY", 'array')
        common_options = [
            'STRIP_OUTER_ARRAY', 
            'NULL_IF'
        ]
        extra_options = self.copy_extra_options(common_options)
        sql = f'''
            COPY INTO {self.sink} 
            FROM @{self.temp_stage}/{self.domain}
            PATTERN = "{self.pattern}"
            PURGE = {self.purge}
            FILE_FORMAT = (
                TYPE = JSON
                STRIP_OUTER_ARRAY = {strip_outer_array}
                {self.null_if}
                {extra_options}
                {self.compression_format}
            )
        '''
        return sql
        
    def build_copy_other(self, format: str, provider: SessionProvider) -> str:
        common_options = [
            'NULL_IF'
        ]
        extra_options = self.copy_extra_options(common_options)
        sql = f'''
            COPY INTO {self.sink} 
            FROM @{self.temp_stage}/{self.domain} 
            PATTERN = "{self.pattern}"
            PURGE = {self.purge}
            FILE_FORMAT = (
                TYPE = {self.format}
                {self.null_if}
                {extra_options}
                {self.compression_format}
            )
        '''
        return sql

    def build_copy(self, provider: SessionProvider) -> str:
        if self.format == 'DSV':
            return self.build_copy_csv(provider=provider)
        elif self.format == 'JSON':
            return self.build_copy_json(provider=provider)
        elif self.format == 'PARQUET':
            return self.build_copy_other(provider=provider)
        elif self.format == 'XML':
            return self.build_copy_other(provider=provider)
        else:
            raise ValueError(f"Unsupported format {format}")

    def execute(self, session: Session, jobid: Optional[str] = None, config: dict = dict(), dry_run: bool = False) -> None:
        """Load the data.
        Args:
            session (Session): The Starlake session.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            config (dict, optional): The config. Defaults to dict().
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if dry_run:
            print(f"-- Loading {self.sink} in dry run mode")

        if not jobid:
            jobid = self.sink

        snowflake: bool = session.provider() == SessionProvider.SNOWFLAKE

        start = datetime.now()

        try:
            # BEGIN transaction
            self.begin_transaction(session, dry_run)

            nbSteps = int(self.statements.get('steps', '1'))
            write_strategy = self.statements.get('writeStrategy', None)
            if nbSteps == 1:
                # execute schema presql
                self.execute_sqls(session, self.context_schema.get('presql', []), "Pre sqls", dry_run)
                # create table
                self.execute_sqls(session, self.statements.get('createTable', []), "Create table", dry_run)
                exists = self.check_if_table_exists(session, self.domain, self.table)
                if exists:
                    # enable change tracking
                    if snowflake:
                        self.enable_change_tracking(session, self.sink, dry_run)
                    # update table schema
                    self.update_table_schema(session, dry_run)
                if write_strategy == 'WRITE_TRUNCATE':
                    # truncate table
                    self.execute_sql(session, f"TRUNCATE TABLE {self.sink}", "Truncate table", dry_run)
                # create stage if not exists
                if snowflake:
                    self.execute_sql(session, f"CREATE STAGE IF NOT EXISTS {self.temp_stage}", "Create stage", dry_run)
                # copy data
                copy_results = self.execute_sql(session, self.build_copy(provider=session.provider()), "Copy data", dry_run)
                if not exists and snowflake:
                    # enable change tracking
                    self.enable_change_tracking(session, self.sink, dry_run)
            elif nbSteps == 2:
                # execute first step
                self.execute_sqls(session, self.statements.get('firstStep', []), "Execute first step", dry_run)
                if self.write_strategy == 'WRITE_TRUNCATE':
                    # truncate table
                    self.execute_sql(session, f"TRUNCATE TABLE {self.sink}", "Truncate table", dry_run)
                # create stage if not exists
                self.execute_sql(session, f"CREATE STAGE IF NOT EXISTS {self.temp_stage}", "Create stage", dry_run)
                # copy data
                copy_results = self.execute_sql(session, self.build_copy(provider=session.provider()), "Copy data", dry_run)
                second_step: dict = self.statements.get('secondStep', dict())
                # execute preActions
                self.execute_sqls(session, second_step.get('preActions', []), "Pre actions", dry_run)
                # execute schema presql
                self.execute_sqls(session, self.context_schema.get('presql', []), "Pre sqls", dry_run)
                if self.check_if_table_exists(session, self.domain, self.table):
                    # enable change tracking
                    if snowflake:
                        self.enable_change_tracking(session, self.sink, dry_run)
                    # execute addSCD2ColumnsSqls
                    self.execute_sqls(session, second_step.get('addSCD2ColumnsSqls', []), "Add SCD2 columns", dry_run)
                    # update schema
                    self.update_table_schema(session, dry_run)
                    # execute mainSqlIfExists
                    self.execute_sqls(session, second_step.get('mainSqlIfExists', []), "Main sql if exists", dry_run)
                else:
                    # execute mainSqlIfNotExists
                    self.execute_sqls(session, second_step.get('mainSqlIfNotExists', []), "Main sql if not exists", dry_run)
                    # enable change tracking
                    if snowflake:
                        self.enable_change_tracking(session, self.sink, dry_run)
                # execute dropFirstStep
                self.execute_sql(session, self.statements.get('dropFirstStep', None), "Drop first step", dry_run)
            else:
                raise ValueError(f"Invalid number of steps: {nbSteps}")

            # execute schema postsql
            self.execute_sqls(session, self.context_schema.get('postsql', []), "Post sqls", dry_run)

            # run expectations
            self.run_expectations(session, jobid, dry_run)

            # COMMIT transaction
            self.commit_transaction(session, dry_run)
            end = datetime.now()
            duration = (end - start).total_seconds()
            print(f"-- Duration in seconds: {duration}")
            files, first_error_line, first_error_column_name, rows_parsed, rows_loaded, errors_seen = self.get_audit_info(copy_results)
            message = first_error_line + '\n' + first_error_column_name
            success = errors_seen == 0
            self.log_audit(session=session, domain=self.domain, table=self.table, paths=files, count=rows_parsed, countAccepted=rows_loaded, countRejected=errors_seen, success=success, duration=duration, message=message, ts=end, jobid=jobid, step="LOAD", dry_run=dry_run)
            
        except Exception as e:
            # ROLLBACK transaction
            error_message = str(e)
            print(f"-- Error executing load for {self.sink}: {error_message}")
            self.rollback_transaction(session, dry_run)
            end = datetime.now()
            duration = (end - start).total_seconds()
            print(f"-- Duration in seconds: {duration}")
            self.log_audit(session=session, domain=self.domain, table=self.table, paths=None, count=-1, countAccepted=-1, countRejected=-1, success=False, duration=duration, message=error_message, ts=end, jobid=jobid, step="LOAD", dry_run=dry_run)
            raise e

class SQLTransformTask(SQLTask):
    
    def __init__(self, sink: str, caller_globals: dict = dict(), arguments: list = [], options: dict = dict(), **kwargs):
        """Initialize the SQL transform task.
        Args:
            sink (str): The sink.
            caller_globals (dict, optional): The caller globals. Defaults to dict().
            arguments (list, optional): The arguments of the task. Defaults to [].
            options (dict, optional): The options. Defaults to dict().
        """
        super().__init__(sink, caller_globals, arguments, options, TaskType.TRANSFORM, **kwargs)

    def execute(self, session: Session, jobid: Optional[str] = None, config: dict = dict(), dry_run: bool = False) -> None:
        """Transform the data.
        Args:
            session (Session): The Starlake session.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            config (dict, optional): The configuration. Defaults to dict().
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if dry_run:
            print(f"-- Executing transform for {self.sink} in dry run mode")

        if not jobid:
            jobid = self.sink

        cron_expr = config.get('cron_expr', None)

        if cron_expr:
            from croniter import croniter
            from croniter.croniter import CroniterBadCronError
            original_schedule = config.get('logical_date', None)
            if original_schedule:
                if isinstance(original_schedule, str):
                    from dateutil import parser
                    start_time = parser.parse(original_schedule)
                else:
                    start_time = original_schedule
            else:
                start_time = datetime.fromtimestamp(datetime.now().timestamp())
            try:
                croniter(cron_expr)
                iter = croniter(cron_expr, start_time)
                curr = iter.get_current(datetime)
                previous = iter.get_prev(datetime)
                next = croniter(cron_expr, previous).get_next(datetime)
                if curr == next :
                    sl_end_date = curr
                else:
                    sl_end_date = previous
                sl_start_date = croniter(cron_expr, sl_end_date).get_prev(datetime)
                format = '%Y-%m-%d %H:%M:%S%z'
                self.safe_params.update({'sl_start_date': sl_start_date.strftime(format), 'sl_end_date': sl_end_date.strftime(format)})
            except CroniterBadCronError:
                raise ValueError(f"Invalid cron expression: {self.cron_expr}")

        snowflake: bool = session.provider() == SessionProvider.SNOWFLAKE

        start = datetime.now()

        try:
            # BEGIN transaction
            self.begin_transaction(session, dry_run)

            # create SQL domain
            self.create_domain_if_not_exists(session, self.domain, dry_run)

            # execute preActions
            self.execute_sqls(session, self.statements.get('preActions', []), "Pre actions", dry_run)

            # execute preSqls
            self.execute_sqls(session, self.statements.get('preSqls', []), "Pre sqls", dry_run)

            if self.check_if_table_exists(session, self.domain, self.table):
                # enable change tracking
                if snowflake:
                    self.enable_change_tracking(session, self.sink, dry_run)
                # execute addSCD2ColumnsSqls
                self.execute_sqls(session, self.statements.get('addSCD2ColumnsSqls', []), "Add SCD2 columns", dry_run)
                # execute mainSqlIfExists
                self.execute_sqls(session, self.statements.get('mainSqlIfExists', []), "Main sql if exists", dry_run)
            else:
                # execute mainSqlIfNotExists
                self.execute_sqls(session, self.statements.get('mainSqlIfNotExists', []), "Main sql if not exists", dry_run)
                # enable change tracking
                if snowflake:
                    self.enable_change_tracking(session, self.sink, dry_run)

            # execute postSqls
            self.execute_sqls(session, self.statements.get('postSqls', []) , "Post sqls", dry_run)

            # run expectations
            self.run_expectations(session, jobid, dry_run)

            # COMMIT transaction
            self.commit_transaction(session, dry_run)
            end = datetime.now()
            duration = (end - start).total_seconds()
            print(f"-- Duration in seconds: {duration}")
            self.log_audit(session=session, domain=self.domain, table=self.table, paths=None, count=-1, countAccepted=-1, countRejected=-1, success=True, duration=duration, message='Success', ts=end, jobid=jobid, step="TRANSFORM", dry_run=dry_run)
            
        except Exception as e:
            # ROLLBACK transaction
            error_message = str(e)
            print(f"-- Error executing transform for {self.sink}: {error_message}")
            self.rollback_transaction(session, dry_run)
            end = datetime.now()
            duration = (end - start).total_seconds()
            print(f"-- Duration in seconds: {duration}")
            self.log_audit(session=session, domain=self.domain, table=self.table, paths=None, count=-1, countAccepted=-1, countRejected=-1, success=False, duration=duration, message=error_message, ts=end, jobid=jobid, step="TRANSFORM", dry_run=dry_run)
            raise e

class SQLTaskFactory:

    def __init__(self):
        """Initialize the SQL task factory."""
        ...

    @classmethod
    def task(cls, caller_globals: dict = dict(), sink: Optional[str] = None, arguments: list = [], options: dict = dict(), task_type: Optional[TaskType] = None, **kwargs) -> SQLTask:
        """Create a task.
        Args:
            cls: The task class.
            caller_globals (dict, optional): The caller globals. Defaults to dict().
            sink (Optional[str], optional): The sink. Defaults to None.
            arguments (list): The required arguments of the starlake command to run.
            options (dict): The options.
            task_type (Optional[TaskType], optional): The task type. Defaults to None.
        Returns:
            SQLTask: The SQL task.
        """
        if not sink:
            raise ValueError("Sink not found")
        if not task_type and len(arguments) > 0:
            task_type = TaskType.from_str(arguments[0])
        else:
            task_type = TaskType.EMPTY
        if task_type == TaskType.LOAD:
            return SQLLoadTask(sink, caller_globals, arguments, options, **kwargs)
        elif task_type == TaskType.TRANSFORM:
            return SQLTransformTask(sink, caller_globals, arguments, options, **kwargs)
        elif task_type == TaskType.EMPTY:
            return SQLEmptyTask(sink, caller_globals, arguments, options, **kwargs)
        else:
            raise ValueError(f"Unsupported task type: {task_type}")
