from datetime import datetime

from typing import Union, List

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions

from dagster import AssetKey, Failure, Output, In, Out, op, graph, AssetMaterialization

from dagster._core.definitions import NodeDefinition

from dagster._core.definitions.output import OutputDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterJob(IStarlakeJob[NodeDefinition], StarlakeOptions):
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)


    def sl_pre_load(self, domain: str, tables: set=set(), pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Union[NodeDefinition, None]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Dagster node that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to pre-load.
            tables (set): The optional tables to pre-load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Union[NodeDefinition, None]: The Dagster node or None.
        """

        if isinstance(pre_load_strategy, str):
            pre_load_strategy = \
                StarlakePreLoadStrategy(pre_load_strategy) if StarlakePreLoadStrategy.is_valid(pre_load_strategy) \
                    else self.pre_load_strategy

        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy

        if pre_load_strategy == StarlakePreLoadStrategy.NONE:
            return None
        else:
            kwargs.update({"ins": {"domain": In(str)}})

            schedule=kwargs.get("schedule", None)
            if schedule:
                name=f"{domain}_{schedule}"
            else:
                name=domain

            if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:
                kwargs.update({'out': 'incoming_files', 'failure': 'no_incoming_files', 'task_id': f'{name}_check_incoming_files'})
                import_node = self.sl_import(
                    task_id=f"{name}_import", 
                    domain=domain, 
                    tables=tables, 
                    ins={"incoming_files": In(str)}, 
                    out="domain_imported", 
                    required=False
                )

            elif pre_load_strategy == StarlakePreLoadStrategy.PENDING:
                kwargs.update({'out': 'pending_files', 'failure': 'no_pending_files', 'task_id': f'{name}_check_pending_files'})

            elif pre_load_strategy == StarlakePreLoadStrategy.ACK:
                kwargs.update({'out': 'ack_file', 'failure': 'no_ack_file', 'task_id': f'{name}_check_ack_file'})

                def current_dt():
                    return datetime.today().strftime('%Y-%m-%d')

                ack_wait_timeout = int(__class__.get_context_var(
                    var_name='ack_wait_timeout',
                    default_value=60*60, # 1 hour
                    options=self.options
                ))

                ack_file = __class__.get_context_var(
                    var_name='global_ack_file_path',
                    default_value=f'{self.sl_datasets}/pending/{domain}/{current_dt()}.ack',
                    options=self.options
                )

                kwargs.update({'retry_delay': ack_wait_timeout, 'ack_file': ack_file})

            pre_load = super().sl_pre_load(domain=domain, tables=tables, pre_load_strategy=pre_load_strategy, **kwargs)

            @graph(
                name=f"{name}_pre_load",
                description=f"Check if domain '{domain}' can be loaded by applying {pre_load_strategy} strategy",
                input_defs=pre_load.input_defs,
                output_defs=[OutputDefinition(name="load_domain", dagster_type=str, is_required=False), OutputDefinition(name="skipped", dagster_type=str, is_required=False)]
            )
            def pre_load_graph(domain, **kwargs):
                checked, skipped = pre_load(domain, **kwargs)
                if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:
                    return {"load_domain": import_node(checked), "skipped": skipped}
                else:
                    return {"load_domain": checked, "skipped": skipped}

            return pre_load_graph

    def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_import()
        Generate the Dagster node that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id ({domain}_import by default).
            domain (str): The required domain to import.
            tables (set): The optional tables to import.

        Returns:
            NodeDefinition: The Dagster node.
        """
        kwargs.update({'asset': AssetKey(self.sl_dataset(domain, **kwargs))})
        kwargs.update({'description': f"Starlake domain '{domain}' imported"})
        return super().sl_import(task_id=task_id, domain=domain, tables=tables, **kwargs)

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_load()
        Generate the Dagster node that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id ({domain}_{table}_load by default).
            domain (str): The required domain to load.
            table (str): The required table to load.

        Returns:
            NodeDefinition: The Dagster node.        
        """
        kwargs.update({'asset': AssetKey(self.sl_dataset(f"{domain}.{table}", **kwargs))})
        kwargs.update({'description': f"Starlake table '{domain}.{table}' loaded"})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Dagster node that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.
        """
        kwargs.update({'asset': AssetKey(self.sl_dataset(transform_name, **kwargs))})
        kwargs.update({'description': f"Starlake transform '{transform_name}' executed"})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.
        
        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
        
        Returns:
            NodeDefinition: The Dagster node.
        """

    def dummy_op(self, task_id: str, out: str = "result", required: bool = True, **kwargs) -> NodeDefinition:
        """Dummy op.
        Generate a Dagster dummy op.

        Args:
            task_id (str): The required task id.
            out (str, optional): The optional output name. Defaults to "result".
            required (bool, optional): The optional required flag. Defaults to True.

        Returns:
            NodeDefinition: The Dagster node.
        """

        assets: List[AssetKey] = kwargs.get("assets", [])

        @op(
            name=task_id,
            required_resource_keys=set(),
            ins=kwargs.get("ins", {}),
            out={out: Out(dagster_type=str, is_required=required)}
        )
        def dummy(**kwargs):
            yield Output(value=task_id, output_name=out)

            for asset in assets:
                yield AssetMaterialization(asset_key=asset.path, description=kwargs.get("description", f"Dummy op {task_id} execution succeeded"))
        return dummy
