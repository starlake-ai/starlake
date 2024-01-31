from datetime import datetime

from typing import Union

from ai.starlake.common import sanitize_id

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions

from dagster import AssetKey, Failure, InputMapping, OutputMapping, DependencyDefinition, GraphDefinition, OpDefinition, Output, In, Out

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterJob(IStarlakeJob[NodeDefinition], StarlakeOptions):
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)


    def sl_pre_load(self, domain: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Union[NodeDefinition, None]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Dagster node that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Union[NodeDefinition, None]: The Dagster node or None.
        """

        if isinstance(pre_load_strategy, str):
            pre_load_strategy = \
                StarlakePreLoadStrategy(pre_load_strategy) if StarlakePreLoadStrategy.is_valid(pre_load_strategy) \
                    else self.pre_load_strategy

        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy

        if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:

            incoming_path = self.__class__.get_context_var(
                var_name='incoming_path',
                default_value=f'{self.sl_root}/incoming',
                options=self.options
            )

            list_files_command = f'ls {incoming_path}/{domain}/* | wc -l'

            if incoming_path.startswith('gs://'):
                list_files_command = "gsutil " + list_files_command

            def list_files(context, config):
                output, return_code = execute_shell_command(
                    shell_command=list_files_command,
                    output_logging="STREAM",
                    log=context.log,
                    cwd=self.sl_root,
                    env=self.sl_env_vars,
                    log_shell_command=True,
                )

                if return_code:
                    raise Failure(description=f"Starlake command {list_files_command} execution failed with output: {output}")

                files_number = output.splitlines()[-1].strip()
                context.log.info('Number of files found: {}'.format(files_number))

                if int(files_number) > 0:
                    yield Output(domain, "import_domain")
                else:
                    yield Output(True, "skipped")

            skip_or_start_node = OpDefinition(
                name=f"{domain}_skip_or_start",
                compute_fn=list_files,
                ins={"domain": In(str)},
                outs={"import_domain": Out(str, is_required=False), "skipped": Out(bool, is_required=False)}, # branching
            )

            import_node = self.sl_import(task_id=None, domain=domain, ins={"domain": In(str)})

            input_mappings=[
                InputMapping(
                    graph_input_name="domain", 
                    mapped_node_name=skip_or_start_node._name,
                    mapped_node_input_name="domain",
                )
            ]

            output_mappings=[
                OutputMapping(
                    graph_output_name="load_domain", 
                    mapped_node_name=import_node._name,
                    mapped_node_output_name="asset",
                )
            ]

            dependencies = dict()
            dependencies[import_node._name] = {
                'domain': DependencyDefinition(skip_or_start_node._name, 'import_domain')
            }

            return GraphDefinition(
                name=f"{domain}_pre_load",
                node_defs=[skip_or_start_node, import_node],
                dependencies=dependencies,
                input_mappings=input_mappings,
                output_mappings=output_mappings,
            )

        elif pre_load_strategy == StarlakePreLoadStrategy.PENDING:

            imported_path = self.__class__.get_context_var(
                var_name='pending_path',
                default_value=f'{self.sl_datasets}/pending',
                options=self.options
            )

            list_files_command = f'ls {imported_path}/{domain}/* | wc -l'

            if imported_path.startswith('gs://'):
                list_files_command = "gsutil " + list_files_command

            def list_files(context, config):
                output, return_code = execute_shell_command(
                    shell_command=list_files_command,
                    output_logging="STREAM",
                    log=context.log,
                    cwd=self.sl_root,
                    env=self.sl_env_vars,
                    log_shell_command=True,
                )

                if return_code:
                    raise Failure(description=f"Starlake command {list_files_command} execution failed with output: {output}")

                files_number = output.splitlines()[-1].strip()
                context.log.info('Number of files found: {}'.format(files_number))

                if int(files_number) > 0:
                    yield Output(domain, "load_domain")
                else:
                    yield Output(True, "skipped")

            skip_or_start_node = OpDefinition(
                name=f"{domain}_skip_or_start",
                compute_fn=list_files,
                ins={"domain": In(str)},
                outs={"load_domain": Out(str, is_required=False), "skipped": Out(bool, is_required=False)}, # branching
            )

            dependencies = dict()

            input_mappings=[
                InputMapping(
                    graph_input_name="domain", 
                    mapped_node_name=skip_or_start_node._name,
                    mapped_node_input_name="domain",
                )
            ]

            output_mappings=[
                OutputMapping(
                    graph_output_name="load_domain", 
                    mapped_node_name=skip_or_start_node._name,
                    mapped_node_output_name="load_domain",
                )
            ]

            return GraphDefinition(
                name=f"{domain}_pre_load",
                node_defs=[skip_or_start_node],
                dependencies=dependencies,
                input_mappings=input_mappings,
                output_mappings=output_mappings,
            )

        elif pre_load_strategy == StarlakePreLoadStrategy.ACK:

            def current_dt():
                return datetime.today().strftime('%Y-%m-%d')

            ack_file = self.__class__.get_context_var(
                var_name='global_ack_file_path',
                default_value=f'{self.sl_datasets}/pending/{domain}/{current_dt()}.ack',
                options=self.options
            )

            check_ack_file_command = f'ls {ack_file} | wc -l'

            if ack_file.startswith('gs://'):
                check_ack_file_command = "gsutil " + check_ack_file_command

            def check_ack_file(context, config):
                output, return_code = execute_shell_command(
                    shell_command=check_ack_file_command,
                    output_logging="STREAM",
                    log=context.log,
                    cwd=self.sl_root,
                    env=self.sl_env_vars,
                    log_shell_command=True,
                )

                if return_code:
                    raise Failure(description=f"Starlake command {check_ack_file_command} execution failed with output: {output}")

                files_number = output.splitlines()[-1].strip()
                context.log.info('Number of files found: {}'.format(files_number))

                if int(files_number) > 0:
                    yield Output(True, "remove_ack_file")
                else:
                    yield Output(True, "skipped")

            skip_or_start_node = OpDefinition(
                name=f"{domain}_skip_or_start",
                compute_fn=check_ack_file,
                ins={"domain": In(str)},
                outs={"remove_ack_file": Out(bool, is_required=False), "skipped": Out(bool, is_required=False)}, # branching
            )

            remove_ack_file_command = f'rm {ack_file}'

            if ack_file.startswith('gs://'):
                remove_ack_file_command = "gsutil " + remove_ack_file_command

            def remove_ack_file(context, config):
                output, return_code = execute_shell_command(
                    shell_command=remove_ack_file_command,
                    output_logging="STREAM",
                    log=context.log,
                    cwd=self.sl_root,
                    env=self.sl_env_vars,
                    log_shell_command=True,
                )

                if return_code:
                    raise Failure(description=f"Starlake command {remove_ack_file_command} execution failed with output: {output}")

                yield Output(domain, "load_domain")

            remove_ack_file_node = OpDefinition(
                name=f"{domain}_remove_ack_file",
                compute_fn=remove_ack_file,
                ins={"remove_ack_file": In(bool)},
                outs={"load_domain": Out(str)},
            )

            dependencies = dict()
            dependencies[remove_ack_file_node._name] = {
                'remove_ack_file': DependencyDefinition(skip_or_start_node._name, 'remove_ack_file')
            }

            input_mappings=[
                InputMapping(
                    graph_input_name="domain", 
                    mapped_node_name=skip_or_start_node._name,
                    mapped_node_input_name="domain",
                )
            ]

            output_mappings=[
                OutputMapping(
                    graph_output_name="load_domain", 
                    mapped_node_name=remove_ack_file_node._name,
                    mapped_node_output_name="load_domain",
                )
            ]

            return GraphDefinition(
                name=f"{domain}_pre_load",
                node_defs=[skip_or_start_node, remove_ack_file_node],
                dependencies=dependencies,
                input_mappings=input_mappings,
                output_mappings=output_mappings,
            )

        else:
            return None

    def sl_import(self, task_id: str, domain: str, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_import()
        Generate the Dagster node that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id ({domain}_import by default).
            domain (str): The required domain to import.

        Returns:
            NodeDefinition: The Dastger node.
        """
        kwargs.update({'asset': AssetKey(sanitize_id(domain))})
        kwargs.update({'description': f"Starlake domain '{domain}' imported"})
#        kwargs.update({'ins': {"domain": In(str)}})
        return super().sl_import(task_id=task_id, domain=domain, **kwargs)

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_load()
        Generate the Dagster node that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id ({domain}_{table}_load by default).
            domain (str): The required domain to load.
            table (str): The required table to load.

        Returns:
            NodeDefinition: The Dastger node.        
        """
        kwargs.update({'asset': AssetKey(sanitize_id(f"{domain}.{table}"))})
        kwargs.update({'description': f"Starlake table '{domain}.{table}' loaded"})
#        kwargs.update({'ins': {"domain": In(str), "table": In(str)}})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Dagster node that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.

        Returns:
            NodeDefinition: The Dastger node.
        """
        kwargs.update({'asset': AssetKey(sanitize_id(transform_name))})
        kwargs.update({'description': f"Starlake transform '{transform_name}' executed"})
#        kwargs.update({'ins': {"transform_name": In(str)}})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.
        
        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
        
        Returns:
            NodeDefinition: The Dastger node.
        """

    def dummy_op(self, task_id: str, **kwargs) -> NodeDefinition:
        """Dummy op.
        Generate a Dagster dummy op.

        Args:
            task_id (str): The required task id.

        Returns:
            OpDefinition: The Dastger node.
        """
        def compute_fn(context, config):
            yield Output(value="dummy", output_name="result")

        return OpDefinition(
            compute_fn=compute_fn,
            name=task_id,
            ins=kwargs.get("ins", {}),
            outs={f"result": Out(str)},
        )
