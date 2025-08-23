from snowflake.snowpark import Row, Session
from snowflake.snowpark.dataframe import DataFrame

from typing import List, Optional, Tuple, Union

from types import ModuleType

from croniter import croniter
from croniter.croniter import CroniterBadCronError

from datetime import datetime, timedelta

import pytz

import logging

import site
import os
import zipfile
import tempfile

def zip_selected_packages(package_root: str = site.getsitepackages()[0], package_name: str = "ai", include: list[str] = ["ai", "ai.starlake", "ai.starlake.common", "ai.starlake.helper"], exclude: list[str] = ["ai.starlake.airflow", "ai.starlake.aws", "ai.starlake.dagster", "ai.starlake.dataset", "ai.starlake.gcp", "ai.starlake.job", "ai.starlake.odbc", "ai.starlake.orchestration", "ai.starlake.snowflake", "ai.starlake.sql"]) -> str:
    """
    Create a ZIP containing only selected subpackages of a package.
    Args:
        package_root (str): The root directory where the package is located. Defaults to the first entry in site.getsitepackages().
        package_name (str): The name of the package to zip. Defaults to "ai".
        include (list[str]): List of subpackage prefixes to include.
        exclude (list[str]): List of subpackage prefixes to exclude.
    """
    package_path = os.path.join(package_root, package_name)
    if not os.path.isdir(package_path):
        raise FileNotFoundError(f"Package '{package_name}' not found in {package_root}")

    import uuid
    zip_path = os.path.join(tempfile.gettempdir(), f"{package_name}_selected_{uuid.uuid4()}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(package_path):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, package_root)
                module_path = rel_path.replace(os.sep, ".").replace(".py", "")

                if any(module_path.startswith(inc) for inc in include) and not any(module_path.startswith(exc) for exc in exclude):
                    zipf.write(file_path, rel_path)
    return zip_path

datetime_format = '%Y-%m-%d %H:%M:%S %z'

class SnowflakeHelper:
    def __init__(self, name: str, timezone: Optional[str] = None) -> None:
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        from collections import defaultdict
        self.safe_params = defaultdict(lambda: 'NULL', {})
        self.name = name
        self.timezone = timezone if timezone else 'UTC'

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

    def is_true(self, value: str, default: bool) -> bool:
        if value is None:
            return default
        return value.lower() == "true"

    # Logging
    def info(self, message: str, dry_run: bool = False) -> None:
        """Print an info message.
        Args:
            message (str): The message to print.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if dry_run:
            print(f"-- {message}")
        else:
            self.logger.info(message)

    def warning(self, message: str, dry_run: bool = False) -> None:
        """Print a warning message.
        Args:
            message (str): The message to print.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if dry_run:
            print(f"-- WARNING: {message}")
        else:
            self.logger.warning(message)

    def error(self, message: str, dry_run: bool = False) -> None:
        """Print an error message.
        Args:
            message (str): The message to print.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if dry_run:
            print(f"-- ERROR: {message}")
        else:
            self.logger.error(message)

    def debug(self, message: str, dry_run: bool = False) -> None:
        """Print a debug message.
        Args:
            message (str): The message to print.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if dry_run:
            print(f"-- DEBUG: {message}")
        else:
            self.logger.debug(message)

    # SQL Execution
    def bindParams(self, stmt: str) -> str:
        """Bind parameters to the SQL statement.
        Args:
            stmt (str): The SQL statement.
        Returns:
            str: The SQL statement with the parameters bound
        """
        return stmt.format_map(self.safe_params)

    def execute_sql(self, session: Session, query: Optional[str], message: Optional[str] = None, dry_run: bool = False) -> List[Row]:
        """Execute the SQL.
        Args:
            session (Session): The Snowflake session.
            query (str): The SQL query to execute.
            message (Optional[str], optional): The optional message. Defaults to None.
            mode (Optional[StarlakeExecutionMode], optional): The optional execution mode. Defaults to None.
        Returns:
            List[Row]: The rows.
        """
        if query:
            if message:
                print(f"-- {message}")
            stmt: str = self.bindParams(query)
            print(f"{stmt};")
            if dry_run:
                print(f"-- Dry run mode, not executing the statement")
                return []
            else:
                df: DataFrame = session.sql(stmt)
                rows = df.collect()
                return rows
        else:
            return []

    def execute_sqls(self, session: Session, sqls: List[str], message: Optional[str] = None, dry_run: bool = False) -> None:
        """Execute the SQLs.
        Args:
            session (Session): The Snowflake session.
            sqls (List[str]): The SQLs.
            message (Optional[str], optional): The optional message. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if sqls:
            if dry_run and message:
                print(f"-- {message}")
            for sql in sqls:
                self.execute_sql(session, sql, None, dry_run)

    # Datasets
    def check_if_dataset_exists(self, session: Session, dataset: str) -> bool:
        """Check if the dataset exists.
        Args:
            session (Session): The Snowflake session.
            dataset (str): The dataset.
            Returns:
            bool: True if the dataset exists, False otherwise.
        """
        query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{dataset}'"
        return self.execute_sql(session, query, f"Check if dataset {dataset} exists:", False).__len__() > 0

    # Change Tracking
    def enable_change_tracking(self, session: Session, sink: str, dry_run: bool = False) -> None:
        """Enable change tracking.
        Args:
            session (Session): The Snowflake session.
            sink (str): The sink.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, f"ALTER TABLE {sink} SET CHANGE_TRACKING = TRUE", "Enable change tracking:", dry_run)

    # Dates
    def get_start_end_dates(self, cron_expr: str, current_date: datetime) -> tuple[datetime, datetime]:
        """Get the start and end dates by applying the cron expression to the current date.
        Args:
            cron_expr (str): The cron expression.
            current_date (datetime): The current date.
        Returns:
            tuple[datetime, datetime]: The start and end dates.
        """
        try:
            croniter(cron_expr)
            iter = croniter(cron_expr, current_date)
            curr = iter.get_current(datetime)
            previous = iter.get_prev(datetime)
            next = croniter(cron_expr, previous).get_next(datetime)
            if curr == next:
                end_date = curr
            else:
                end_date = previous
            start_date = croniter(cron_expr, end_date).get_prev(datetime)
            return start_date, end_date
        except CroniterBadCronError:
            raise ValueError(f"Invalid cron expression: {cron_expr}")

    def get_execution_date(self, session: Session, dry_run: bool = False) -> datetime:
        """Get the execution date of the current DAG run.
        Args:
            session (Session): The Snowflake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            datetime: The execution date.
        """
        query = "SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP')::timestamp_ltz"
        rows = self.execute_sql(session, query, "Get the original scheduled timestamp of the initial graph run", dry_run)
        if rows.__len__() == 1:
            execution_date = self.as_datetime(rows[0][0])
            self.info(f"Execution date set to the original scheduled timestamp of the initial graph run: {execution_date}", dry_run=dry_run)
        else:
            execution_date = datetime.fromtimestamp(datetime.now().timestamp()).astimezone(pytz.timezone(self.timezone))
            self.info(f"Execution date set to the current system date: {execution_date}", dry_run=dry_run)
        return execution_date

    def as_datetime(self, value: Union[str, datetime]) -> datetime:
        """Convert a string to a datetime object.
        Args:
            value (str): The string to convert.
        Returns:
            datetime: The datetime object.
        """
        if isinstance(value, str):
            from dateutil import parser
            value = parser.parse(value).astimezone(pytz.timezone(self.timezone))
        return value.astimezone(pytz.timezone(self.timezone))

class SnowflakeDAGHelper(SnowflakeHelper):
    def __init__(self, name: str, timezone: Optional[str] = None) -> None:
        super().__init__(name, timezone)
        self.name = name

    def get_dag_logical_date(self, session: Session, ts: datetime, backfill: bool = False, dry_run: bool = False) -> datetime:
        """Get the logical date of the running dag.
        Args:
            session (Session): The Snowflake session.
            ts (datetime): The timestamp of the current DAG run.
            backfill (bool, optional): Whether the current Dag run is a backfill. Defaults to False.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            datetime: The logical date of the running dag.
        """
        if not backfill:
            # the logical date is the optional one defined in the task graph config
            if dry_run:
                config = None
            else:
                config = session.call("system$get_task_graph_config")
            if config:
                import json
                config = json.loads(config)
            else:
                config = {}
            logical_date = config.get("logical_date", None)
            if logical_date:
                self.info(f"Logical date set to the one defined in the task graph config: {logical_date}", dry_run=dry_run)
        else:
            # the logical date is the partition end date
            query = "SELECT SYSTEM$TASK_RUNTIME_INFO('PARTITION_END')::timestamp_ltz"
            rows = self.execute_sql(session, query, "Get the original scheduled timestamp of the initial graph run", dry_run)
            if rows.__len__() == 1:
                logical_date = rows[0][0]
                self.info(f"Logical date set to the partition end date: {logical_date}", dry_run=dry_run)
        if not logical_date:
            return ts
        return self.as_datetime(logical_date)

    def get_previous_dag_run(self, session: Session, logical_date: datetime, dry_run: bool, at_scheduled_date: bool = False) -> Optional[tuple[datetime, datetime]]:
        """Get the previous DAG run.
        Args:
            session (Session): The Snowflake session.
            logical_date (datetime): The logical date.
            dry_run (bool): Whether to run in dry run mode.
            at_scheduled_date (bool): Whether to get the DAG run at the scheduled date.
        Returns:
            Optional[tuple[datetime, datetime]]: The previous DAG Run as a tuple of (scheduled_time, query_start_time) or None.
        """
        # if we are at the scheduled date, we look for the last successful dag run before or at the scheduled date
        # if we are not at the scheduled date, we look for the last successful dag run before the scheduled date
        comparison_operator = "<=" if at_scheduled_date else "<"
        query = f"""SELECT SCHEDULED_TIME, QUERY_START_TIME 
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        ERROR_ONLY => false
    )
)
WHERE GRAPH_RUN_GROUP_ID IN (
    SELECT GRAPH_RUN_GROUP_ID 
    FROM TABLE(
        INFORMATION_SCHEMA.TASK_HISTORY(
            ERROR_ONLY => false
        )
    )
    WHERE 
        NAME ilike '{self.name}$end' 
        AND STATE = 'SUCCEEDED'
        AND COMPLETED_TIME IS NOT NULL
        AND COMPLETED_TIME {comparison_operator} '{logical_date.strftime(datetime_format)}'
    ORDER BY COMPLETED_TIME DESC
    LIMIT 1
)
ORDER BY SCHEDULED_TIME DESC"""
        rows = self.execute_sql(session, query, f"Get the previous successful DAG run for {self.name} {comparison_operator} {logical_date.strftime(datetime_format)}", dry_run)
        if rows and rows.__len__() > 0:
            return (self.as_datetime(rows[0][0]), self.as_datetime(rows[0][1]))
        return None

    def get_current_graph_run_group_id(self, session: Session, dry_run: bool) -> Optional[str]:
        query = "SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')"
        rows = self.execute_sql(session, query, "Get the current graph run group id", dry_run)
        if rows.__len__() == 1:
            return rows[0][0]
        return None

    def is_current_graph_scheduled(self, session: Session, dry_run: bool) -> bool:
        """Check if the current graph is scheduled.
        Args:
            session (Session): The Snowflake session.
            dry_run (bool): Whether to run in dry run mode.
        Returns:
            bool: True if the current graph is scheduled, False otherwise.
        """
        current_graph_run_group_id = self.get_current_graph_run_group_id(session, dry_run)
        if current_graph_run_group_id:
            # we check if the current graph run group id is scheduled
            query = f"SELECT SCHEDULED_FROM FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE GRAPH_RUN_GROUP_ID = '{current_graph_run_group_id}'"
            rows = self.execute_sql(session, query, f"Check if the current graph run group id {current_graph_run_group_id} is scheduled", dry_run)
            if rows.__len__() == 1:
                scheduled_from = rows[0][0]
                scheduled = scheduled_from is not None and scheduled_from == 'SCHEDULE'
                self.info(f"Current graph run group id {current_graph_run_group_id} has been {'scheduled' if scheduled else 'launched manually'}", dry_run=dry_run)
                return scheduled
        self.info(f"By default, the current graph run is considered scheduled", dry_run=dry_run)
        return True

    def find_dataset_event(self, session: Session, dataset: str, scheduled_date_to_check_min: datetime, scheduled_date_to_check_max: datetime, ts: datetime, dry_run: bool) -> Optional[tuple[datetime, datetime]]:
        """Find the events for a dataset.
        Args:
            session (Session): The Snowflake session.
            dataset (str): The dataset.
            scheduled_date_to_check_min (datetime): The minimum scheduled date to check.
            scheduled_date_to_check_max (datetime): The maximum scheduled date to check.
            ts (datetime): The timestamp.
            dry_run (bool): Whether to run in dry run mode.
        Returns:
            Optional[tuple[datetime, datetime]]: The last event for the dataset as a tuple of (timestamp, scheduled_date) or None.
        """
        # we look for the last event for the dataset in the audit table
        # the event is the last successful run of the dataset before the max scheduled date and after the min scheduled date
        domainAndSchema = dataset.split('.')
        if len(domainAndSchema) != 2:
            raise ValueError(f"Invalid dataset name: {dataset}. It should be in the format 'domain.schema'.")
        domain = domainAndSchema[0]
        schema = domainAndSchema[-1]
        query = f"""SELECT TIMESTAMP, SCHEDULED_DATE 
FROM audit.audit 
WHERE DOMAIN ilike '{domain}'
    AND SCHEMA ilike '{schema}'
    AND SUCCESS = true
    AND SCHEDULED_DATE > TO_TIMESTAMP('{scheduled_date_to_check_min.strftime(datetime_format)}')
    AND SCHEDULED_DATE <= TO_TIMESTAMP('{scheduled_date_to_check_max.strftime(datetime_format)}')
ORDER BY SCHEDULED_DATE DESC, TIMESTAMP DESC
"""
        rows = self.execute_sql(session, query, f'Finding dataset event for {dataset} with scheduled date between {scheduled_date_to_check_min.strftime(datetime_format)} and {scheduled_date_to_check_max.strftime(datetime_format)}', dry_run)
        if rows and rows.__len__() > 0:
            return (self.as_datetime(rows[0][0]), self.as_datetime(rows[0][1]))
        return None

class SnowflakeTaskHelper(SnowflakeHelper):
    def __init__(self, sink: str, domain: str, table: str, audit: dict, expectations: dict, expectation_items: list, name: str, timezone: Optional[str] = None) -> None:
        super().__init__(name, timezone)
        self.sink = sink
        self.domain = domain
        self.table = table
        self.audit = audit
        self.expectations = expectations
        self.expectation_items = expectation_items

    def get_task_logical_date(self, session: Session, backfill: bool = False, dry_run: bool = False) -> datetime:
        """Get the logical date of the running dag.
        Args:
            session (Session): The Snowflake session.
            backfill (bool, optional): Whether the current Dag run is a backfill. Defaults to False.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            datetime: The logical date of the running dag.
        """
        if not backfill:
            # the logical date is the optional one defined in the task graph config
            if dry_run:
                config = None
            else:
                config = session.call("system$get_task_graph_config")
            if config:
                import json
                config = json.loads(config)
            else:
                config = {}
            logical_date = config.get("logical_date", None)
            if not logical_date:
                # the logical date is the return value of the current root task
                query = "SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME')"
                rows = self.execute_sql(session, query, "Get the current root task name", dry_run)
                if rows.__len__() == 1:
                    current_root_task_name = rows[0][0]
                else:
                    current_root_task_name = self.name
                query = f"SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE('{current_root_task_name.split('.')[-1]}')"
                rows = self.execute_sql(session, query, "Get the predecessor return value", dry_run)
                if rows.__len__() == 1:
                    logical_date = rows[0][0]

        else:
            # the logical date is the partition end date
            query = "SELECT SYSTEM$TASK_RUNTIME_INFO('PARTITION_END')::timestamp_ltz"
            rows = self.execute_sql(session, query, "Get the original scheduled timestamp of the initial graph run", dry_run)
            if rows.__len__() == 1:
                logical_date = rows[0][0]
        if not logical_date:
            return self.get_execution_date(session, dry_run)
        return self.as_datetime(logical_date)

    # Domains
    def create_domain_if_not_exists(self, session: Session, domain: str, dry_run: bool = False) -> None:
        """Create the schema if it does not exist.
        Args:
            session (Session): The Snowflake session.
            domain (str): The domain.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, f"CREATE SCHEMA IF NOT EXISTS {domain}", f"Create schema {domain} if not exists:", dry_run)

    # Transactions
    def begin_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Begin the transaction.
        Args:
            session (Session): The Snowflake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "BEGIN", "BEGIN transaction:", dry_run)

    def commit_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Commit the transaction.
        Args:
            session (Session): The Snowflake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "COMMIT", "COMMIT transaction:", dry_run)

    def rollback_transaction(self, session: Session, dry_run: bool = False) -> None:
        """Rollback the transaction.
        Args:
            session (Session): The Snowflake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        self.execute_sql(session, "ROLLBACK", "ROLLBACK transaction:", dry_run)

    # Schema
    def schema_as_dict(self, schema_string: str) -> dict:
        tableNativeSchema = map(lambda x: (x.split()[0].strip(), x.split()[1].strip()), schema_string.replace("\"", "").split(","))
        tableSchemaDict = dict(map(lambda x: (x[0].lower(), x[1].lower()), tableNativeSchema))
        return tableSchemaDict

    def add_columns_from_dict(self, dictionary: dict):
        return [f"ALTER TABLE IF EXISTS {self.domain}.{self.table} ADD COLUMN IF NOT EXISTS {k} {v} NULL;" for k, v in dictionary.items()]

    def drop_columns_from_dict(self, dictionary: dict):
        return [f"ALTER TABLE IF EXISTS {self.domain}.{self.table} DROP COLUMN IF EXISTS {col};" for col in dictionary.keys()]

    def update_table_schema(self, session: Session, schema_string: str, sync_strategy: Optional[str], dry_run: bool) -> bool:
        if not sync_strategy:
            sync_strategy = "NONE"
        sync_strategy = sync_strategy.upper()
        existing_schema_sql = f"select column_name, data_type from information_schema.columns where table_schema ilike '{self.domain}' and table_name ilike '{self.table}';"
        rows = self.execute_sql(session, existing_schema_sql, f"Retrieve existing schema for {self.domain}.{self.table}", False)
        existing_columns = []
        for row in rows:
            existing_columns.append((str(row[0]).lower(), str(row[1]).lower()))
        existing_schema = dict(existing_columns)
        if dry_run:
            print(f"-- Existing schema for {self.domain}.{self.table}: {existing_schema}") 
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
        alter_columns = self.add_columns_from_dict(new_columns_dict)
        if sync_strategy == "ALL" or sync_strategy == "ADD":
            self.execute_sqls(session, alter_columns, "Add columns", dry_run)
        old_columns_dict = {key: existing_schema[key] for key in old_columns}
        drop_columns = self.drop_columns_from_dict(old_columns_dict)
        if sync_strategy == "ALL":
            self.execute_sqls(session, drop_columns, "Drop columns", dry_run)
        return True

    # Audit
    def check_if_audit_table_exists(self, session: Session, dry_run: bool = False) -> bool:
        """Check if the audit table exists.
        Args:
            session (Session): The Snowflake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        Returns:
            bool: True if the audit table exists, False otherwise.
        """
        if self.audit:
            try:
                # create SQL domain
                domain = self.audit.get('domain', ['audit'])[0]
                self.create_domain_if_not_exists(session, domain, dry_run)
                # execute SQL preActions
                self.execute_sqls(session, self.audit.get('preActions', []), "Execute audit pre action:", dry_run)
                # check if the audit table exists
                if not self.check_if_dataset_exists(session, f"{domain}.audit"):
                    # execute SQL createSchemaSql
                    sqls: List[str] = self.audit.get('createSchemaSql', [])
                    if sqls:
                        self.execute_sqls(session, sqls, "Create audit table", dry_run)
                    return True
                else:
                    return True
            except Exception as e:
                self.error(f"Error creating audit table: {str(e)}", dry_run=dry_run)
                return False
        else:
            return False

    def log_audit(self, session: Session, paths: Optional[str], count: int, countAccepted: int, countRejected: int, success: bool, duration: int, message: str, ts: datetime, jobid: Optional[str] = None, step: Optional[str] = None, dry_run: bool = False, scheduled_date: Optional[datetime] = None) -> bool :
        """Log the audit record.
        Args:
            session (Session): The Snowflake session.
            paths (Optional[str]): The optional paths. Defaults to None.
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
            scheduled_date (Optional[datetime], optional): The optional scheduled date. Defaults to None.
        Returns:
            bool: True if the audit record was logged, False otherwise.
        """
        if self.audit and self.check_if_audit_table_exists(session, dry_run):
            audit_domain = self.audit.get('domain', ['audit'])[0]
            audit_sqls = self.audit.get('mainSqlIfExists', None)
            if audit_sqls:
                try:
                    ts = ts.astimezone(pytz.timezone(self.timezone))
                    scheduled_date = scheduled_date.astimezone(pytz.timezone(self.timezone)) if scheduled_date else ts
                    audit_sql = audit_sqls[0]
                    formatted_sql = audit_sql.format(
                        jobid = jobid or f'{self.domain}.{self.table}',
                        paths = paths or self.table,
                        domain = self.domain,
                        schema = self.table,
                        success = str(success),
                        count = str(count),
                        countAccepted = str(countAccepted),
                        countRejected = str(countRejected),
                        timestamp = ts.strftime(datetime_format),
                        duration = str(duration),
                        message = message,
                        step = step or "TRANSFORM",
                        database = "",
                        tenant = "",
                        scheduledDate = scheduled_date.strftime(datetime_format) if scheduled_date else ts.strftime(datetime_format)
                    )
                    insert_sql = f"INSERT INTO {audit_domain}.audit {formatted_sql}"
                    self.execute_sql(session, insert_sql, "Insert audit record:", dry_run)
                    return True
                except Exception as e:
                    self.error(f"Error inserting audit record: {str(e)}")
                    return False
            else:
                return False
        else:
            return False

    def get_audit_info(self, rows: List[Row], dry_run: bool) -> Tuple[str, str, str, int, int, int]:
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
                self.info(f"Row: {row}", dry_run=dry_run)
                row_dict = row.as_dict()
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

    # Expectations
    def check_if_expectations_table_exists(session: Session, dry_run: bool = False) -> bool:
        """Check if the expectations table exists.
        Args:
            session (Session): The Snowflake session.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
            Returns:
            bool: True if the expectations table exists, False otherwise.
        """
        if self.expectations:
            try:
                # create SQL domain
                domain = self.expectations.get('domain', ['audit'])[0]
                self.create_domain_if_not_exists(session, domain, dry_run)
                # check if the expectations table exists
                if not self.check_if_dataset_exists(session, f"{domain}.expectations"):
                    # execute SQL createSchemaSql
                    self.execute_sqls(session, self.expectations.get('createSchemaSql', []), "Create expectations table", dry_run)
                    return True
                else:
                    return True
            except Exception as e:
                self.error(f"Error creating expectations table: {str(e)}")
                return False
        else:
            return False

    def log_expectation(self, session: Session, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime, jobid: Optional[str] = None, dry_run: bool = False) -> bool :
        """Log the expectation record.
        Args:
            session (Session): The Snowflake session.
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
                    ts = ts.astimezone(pytz.timezone(timezone))
                    expectation_sql = expectation_sqls[0]
                    formatted_sql = expectation_sql.format(
                        jobid = jobid or f'{domain}.{table}',
                        database = "",
                        domain = domain,
                        schema = table,
                        count = count,
                        exception = exception,
                        timestamp = ts.strftime(datetime_format),
                        success = str(success),
                        name = name,
                        params = params,
                        sql = sql
                    )
                    insert_sql = f"INSERT INTO {expectation_domain}.expectations {formatted_sql}"
                    self.execute_sql(session, insert_sql, "Insert expectations record:", dry_run)
                    return True
                except Exception as e:
                    self.error(f"Error inserting expectations record: {str(e)}")
                    return False
            else:
                return False
        else:
            return False

    def run_expectation(self, session: Session, name: str, params: str, query: str, failOnError: bool = False, jobid: Optional[str] = None, dry_run: bool = False) -> None:
        """Run the expectation.
        Args:
            session (Session): The Snowflake session.
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
                        raise Exception(f'Expectation failed for {sink}: {query}. Expected 1 row but got {rows.__len__()}')
                else:
                    count = rows[0][0]
                #  log expectations as audit in expectation table here
                if count != 0:
                    raise Exception(f'Expectation failed for {sink}: {query}. Expected count to be equal to 0 but got {count}')
                self.log_expectation(session, True, name, params, query, count, "", datetime.now(), jobid, dry_run)
            else:
                raise Exception(f'Expectation failed for {sink}: {name}. Query not found')
        except Exception as e:
            self.error(f"Error running expectation {name}: {str(e)}")
            self.log_expectation(session, False, name, params, query, count, str(e), datetime.now(), jobid, dry_run)
            if failOnError and not dry_run:
                raise e

    def run_expectations(self, session: Session, jobid: Optional[str] = None, dry_run: bool = False) -> None:
        """Run the expectations.
        Args:
            session (Session): The Snowflake session.
            jobid (Optional[str], optional): The optional job id. Defaults to None.
            dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
        """
        if self.expectation_items and self.check_if_expectations_table_exists(session, dry_run):
            for expectation in self.expectation_items:
                self.run_expectation(session, expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), self.str_to_bool(expectation.get('failOnError', 'no')), jobid, dry_run)

class SnowflakeLoadTaskHelper(SnowflakeTaskHelper):
    def __init__(self, sl_incoming_file_stage: str, pattern: str, table_name: str, metadata: dict, variant: str, sink: str, domain: str, table: str, audit: dict, expectations: dict, expectation_items: list, name: str, timezone: Optional[str] = None) -> None:
        super().__init__(sink, domain, table, audit, expectations, expectation_items, name, timezone)
        if not sl_incoming_file_stage:
            raise ValueError("sl_incoming_file_stage option is required")
        self.sl_incoming_file_stage = sl_incoming_file_stage
        self.pattern = pattern
        self.table_name = table_name
        self.metadata = metadata
        self.metadata_options: dict = metadata.get("options", dict())
        self.variant = variant

        format: str = metadata.get('format', None)
        if not format:
            raise ValueError(f"Format for {self.sink} not found")
        else:
            format = format.upper()
        self.format = format

        compression: bool = self.is_true(self.get_option("compression"), False)
        self.compression = compression

        if compression:
            compression_format = "COMPRESSION = GZIP" 
        else:
            compression_format = "COMPRESSION = NONE"
        self.compression_format = compression_format

        null_if = self.get_option('NULL_IF')
        if not null_if and self.is_true(self.metadata.get('emptyIsNull', "true"), False):
            null_if = "NULL_IF = ('')"
        elif null_if:
            null_if = f"NULL_IF = {null_if}"
        else:
            null_if = ""
        self.null_if = null_if

        purge = self.get_option("PURGE")
        if not purge:
            purge = "FALSE"
        else:
            purge = purge.upper()
        self.purge = purge

    # Copy data
    def get_option(self, metadata_key: Optional[str] = None, default_value: Optional[str] = None) -> Optional[str]:
        if self.metadata_options and key.lower() in self.metadata_options:
            return self.metadata_options.get(key.lower(), None)
        elif metadata_key and self.metadata.get(metadata_key, None):
            return self.metadata[metadata_key].replace('\\', '\\\\')
        return default_value

    def copy_extra_options(self, common_options: list[str]):
        extra_options = ""
        if self.metadata_options:
            for k, v in self.metadata_options.items():
                if k.upper().startswith("SNOWFLAKE_"):
                    newKey = k[len("SNOWFLAKE_"):]
                    if not newKey in common_options:
                        extra_options += f"{newKey} = {v}\n"
        return extra_options

    def build_copy_csv(self) -> str:
        skipCount = self.get_option("SKIP_HEADER")
        if not skipCount and self.is_true(self.metadata.get('withHeader', 'true'), False):
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
COPY INTO {self.table_name} 
FROM @{self.sl_incoming_file_stage}/{self.domain}/
PATTERN = '{self.pattern}{extension}'
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
)'''
        return sql

    def build_copy_json(self) -> str:
        strip_outer_array = self.get_option("STRIP_OUTER_ARRAY", default_value='true')
        common_options = [
            'STRIP_OUTER_ARRAY', 
            'NULL_IF'
        ]
        extra_options = self.copy_extra_options(common_options)
        if (self.variant == "false"):
            match_by_columnName = "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        else:
            match_by_columnName = ''
        sql = f'''
COPY INTO {self.table_name} 
FROM @{self.sl_incoming_file_stage}/{self.domain}
PATTERN = '{self.pattern}'
PURGE = {self.purge}
FILE_FORMAT = (
    TYPE = JSON
    STRIP_OUTER_ARRAY = {strip_outer_array}
    {self.null_if}
    {extra_options}
    {self.compression_format}
)
{match_by_columnName}'''
        return sql

    def build_copy_other(self) -> str:
        common_options = [
            'NULL_IF'
        ]
        extra_options = self.copy_extra_options(common_options)
        sql = f'''
COPY INTO {self.table_name} 
FROM @{self.sl_incoming_file_stage}/{self.domain} 
PATTERN = '{self.pattern}'
PURGE = {self.purge}
FILE_FORMAT = (
    TYPE = {self.format}
    {self.null_if}
    {extra_options}
    {self.compression_format}
)'''
        return sql

    def build_copy(self) -> str:
        if self.format == 'DSV':
            return self.build_copy_csv()
        elif self.format == 'JSON' or self.format == 'JSON_FLAT':
            return self.build_copy_json()
        elif self.format == 'PARQUET':
            return self.build_copy_other()
        elif self.format == 'XML':
            return self.build_copy_other()
        else:
            raise ValueError(f"Unsupported format {self.format}")

