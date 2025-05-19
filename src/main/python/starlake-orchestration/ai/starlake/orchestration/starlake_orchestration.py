from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, final, Generic, List, Optional, Set, Tuple, Type, TypeVar, Union

import os
import importlib
import inspect

from ai.starlake.common import StarlakeCronPeriod, sl_cron_start_end_dates, sort_crons_by_frequency, is_valid_cron, sanitize_id

from ai.starlake.job import StarlakeSparkConfig, IStarlakeJob, StarlakePreLoadStrategy, StarlakeExecutionMode

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from ai.starlake.orchestration import StarlakeSchedule, StarlakeDependencies, StarlakeDependency, StarlakeDependencyType, DependencyMixin, TreeNodeMixin

U = TypeVar("U") # type of DAG

E = TypeVar("E") # type of event

T = TypeVar("T") # type of task

GT = TypeVar("GT") # type of task group

class AbstractDependency(DependencyMixin, ABC):
    """Abstract interface to define a dependency."""
    def __init__(self, id: str) -> None:
        super().__init__(id=id)

    def __rshift__(self, other: Union[List["AbstractDependency"], "AbstractDependency"]) -> Union[List["AbstractDependency"], "AbstractDependency"]:
        """Add self as an upstream dependency to other.
        Args:
            other (AbstractDependency): the upstream dependency.
        """
        super().__rshift__(other)
        ctx = TaskGroupContext.current_context()
        if not ctx:
            raise ValueError("No task group context found")
        if isinstance(other, list):
            return [ctx.set_dependency(self, dep) for dep in other]
        return ctx.set_dependency(self, other)

    def __lshift__(self, other: Union[List["AbstractDependency"], "AbstractDependency"]) -> Union[List["AbstractDependency"], "AbstractDependency"]:
        """Add other as an upstream dependency to self.
        Args:
            other (AbstractDependency): the upstream dependency.
        """
        super().__lshift__(other)
        ctx = TaskGroupContext.current_context()
        if not ctx:
            raise ValueError("No task group context found")
        if isinstance(other, list):
            return [ctx.set_dependency(dep, self) for dep in other]
        return ctx.set_dependency(other, self)

    def __repr__(self):
        return f"Dependency(id={self.id})"

    def __repr__(self):
        return f"Dependency(id={self.id}, upstreams=[{','.join([dep.id for dep in self.upstreams])}], downstreams=[{','.join([dep.id for dep in self.downstreams])}], node={self.node})"

class AbstractTask(Generic[T], AbstractDependency):
    """Abstract interface to define a task."""
    
    def __init__(self, task_id: str, task: Optional[T] = None) -> None:
        current_context = TaskGroupContext.current_context()
        if not current_context:
            raise ValueError("No task group context found")
        super().__init__(id=task_id)
        self.__task_id = task_id
        self.__task = task
        # Automatically register the task to the current context
        current_context.add_dependency(self)

    @property
    def task_id(self) -> str:
        return self.__task_id

    @property
    def task(self) -> Optional[T]:
        return self.__task

class TaskGroupContext(AbstractDependency):
    """Task group context to manage dependencies."""
    _context_stack: List["TaskGroupContext"] = []

    def __init__(self, group_id: str, orchestration_cls: "AbstractOrchestration", parent: Optional["TaskGroupContext"] = None):
        super().__init__(id=group_id)
        self.__group_id = group_id
        self.__orchestration_cls = orchestration_cls
        self.__dependencies: List[AbstractDependency] = []
        self.__dependencies_dict: dict = dict()
        self.__upstream_dependencies: dict = dict()
        self.__downstream_dependencies: dict = dict()
        self.__level: int = len(TaskGroupContext._context_stack) + 1
        current_context = TaskGroupContext.current_context()
        self.__parent = current_context if not parent else parent
        if self.parent:
            self.parent.add_dependency(self)

    def __enter__(self):
        TaskGroupContext._context_stack.append(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        TaskGroupContext._context_stack.pop()
        return False

    @property
    def group_id(self) -> str:
        return self.__group_id

    @property
    def parent(self) -> Optional["TaskGroupContext"]:
        return self.__parent

    @property
    def dependencies(self) -> List[AbstractDependency]:
        return self.__dependencies

    @property
    def dependencies_dict(self) -> dict:
        return self.__dependencies_dict

    @property
    def upstream_dependencies(self) -> dict:
        return self.__upstream_dependencies

    @property
    def downstream_dependencies(self) -> dict:
        return self.__downstream_dependencies

    @property
    def level(self) -> int:
        return self.__level

    @classmethod
    def current_context(cls) -> Optional["TaskGroupContext"]:
        """Get the current context.
        Returns:
            Optional[TaskGroupContext]: the current context if any, None otherwise.
        """
        return cls._context_stack[-1] if cls._context_stack else None

    def set_dependency(self, upstream_dependency: Union[AbstractDependency, Any], downstream_dependency: Union[AbstractDependency, Any]) -> AbstractDependency:
        """Set a dependency between two tasks.
        Args:
            upstream_dependency (AbstractDependency): the upstream dependency.
            downstream_dependency (AbstractDependency): the downstream dependency.
        """
        if not isinstance(upstream_dependency, AbstractDependency):
            upstream_dependency = self.__orchestration_cls.from_native(upstream_dependency)
            if upstream_dependency is None:
                raise ValueError(f"Invalid upstream dependency: {upstream_dependency}")
        if not isinstance(downstream_dependency, AbstractDependency):
            downstream_dependency = self.__orchestration_cls.from_native(downstream_dependency)
            if downstream_dependency is None:
                raise ValueError(f"Invalid downstream dependency: {downstream_dependency}")
        upstream_dependency_id = upstream_dependency.id
        downstream_dependency_id = downstream_dependency.id
        upstream_deps = self.upstream_dependencies.get(upstream_dependency_id, [])
        if downstream_dependency_id not in upstream_deps:
            upstream_deps.append(downstream_dependency_id)
            self.upstream_dependencies[upstream_dependency_id] = upstream_deps
        downstream_deps = self.downstream_dependencies.get(downstream_dependency_id, [])
        if upstream_dependency_id not in downstream_deps:
            downstream_deps.append(upstream_dependency_id)
            self.downstream_dependencies[downstream_dependency_id] = downstream_deps
        return downstream_dependency

    @final
    def add_dependency(self, dependency: AbstractDependency) -> AbstractDependency:
        """Add a dependency to the current context.
        Args:
            dependency (AbstractDependency): the dependency to add.
        """
        if dependency.id in self.dependencies_dict.keys():
            raise ValueError(f"Dependency with id '{dependency.id}' already exists within group '{self.group_id}'")
        self.dependencies_dict[dependency.id] = dependency
        self.dependencies.append(dependency)
        return dependency

    @final
    def get_dependency(self, id: str) -> Optional[AbstractDependency]:
        """Get a dependency by its id.
        Args:
            id (str): the dependency id.
        Returns:
            Optional[AbstractDependency]: the dependency if found, None otherwise.
        """
        return self.dependencies_dict.get(id, None)

    @final
    @property
    def roots_keys(self) -> List[str]:
        upstream_keys = set(self.upstream_dependencies.keys())
        downstream_keys = set(self.downstream_dependencies.keys())
        if len(upstream_keys) == 0 and len(downstream_keys) == 0:
            # no dependencies, all tasks are considered roots and leaves
            return list(self.dependencies_dict.keys())
        else:
            return list(upstream_keys - downstream_keys)

    @final
    @property
    def leaves_keys(self) -> List[str]:
        upstream_keys = set(self.upstream_dependencies.keys())
        downstream_keys = set(self.downstream_dependencies.keys())
        if len(upstream_keys) == 0 and len(downstream_keys) == 0:
            # no dependencies, all tasks are considered roots and leaves
            return list(self.dependencies_dict.keys())
        else:
            return list(downstream_keys - upstream_keys)

    @final
    @property
    def roots(self) -> List[AbstractDependency]:
        return list(filter(lambda root: root is not None, [self.get_dependency(id) for id in self.roots_keys]))

    @final
    @property
    def leaves(self) -> List[AbstractDependency]:
        return list(filter(lambda leave: leave is not None, [self.get_dependency(id) for id in self.leaves_keys]))

    def __repr__(self):
        return f"TaskGroup(id={self.group_id}, parent={self.parent.id if self.parent else ''}, dependencies=[{','.join([dep.id for dep in self.dependencies])}], roots=[{','.join([root.id for root in self.roots])}], leaves=[{','.join([leave.id for leave in self.leaves])}], upstreams=[{','.join([dep.id for dep in self.upstreams])}], downstreams=[{','.join([dep.id for dep in self.downstreams])}], node={self.node})"

class AbstractTaskGroup(Generic[GT], TaskGroupContext):
    """Abstract interface to define a task group."""

    def __init__(self, group_id: str, orchestration_cls: "AbstractOrchestration", group: Optional[GT] = None, **kwargs):
        super().__init__(group_id, orchestration_cls)
        self.__group = group
        self.params = kwargs

    @property
    def group(self) -> Optional[GT]:
        return self.__group

    @final
    def print_group(self, level: int) -> int:
        def printTree(upstream_dependencies, root_key, level=level) -> int:
            print(' ' * level, root_key)
            updated_level = level
            root = self.get_dependency(root_key)
            if isinstance(root, AbstractTaskGroup) and root_key != self.group_id:
                updated_level = root.print_group(level + 1)
            if root_key in upstream_dependencies:
                for key in upstream_dependencies[root_key]:
                    updated_level = updated_level + 1
                    printTree(upstream_dependencies, key, updated_level)
            return updated_level
        upstream_keys = self.upstream_dependencies.keys()
        downstream_keys = self.downstream_dependencies.keys()
        root_keys = upstream_keys - downstream_keys
        if not root_keys and len(upstream_keys) == 0 and len(downstream_keys) == 0:
            root_keys = self.dependencies_dict.keys()
        if root_keys:
            return max([printTree(self.upstream_dependencies, root_key) for root_key in root_keys])
        else:
            return level

class AbstractPipeline(Generic[U, T, GT, E], AbstractTaskGroup[U], AbstractEvent[E]):
    """Abstract interface to define a pipeline."""
    def __init__(self, job: IStarlakeJob[T, E], orchestration_cls: "AbstractOrchestration", dag: Optional[U] = None, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[U, T, GT, E]] = None, add_dag_dependency: Optional[Callable[[Union[T, GT], Union[T, GT]], Any]] = None, **kwargs) -> None:
        if not schedule and not dependencies:
            raise ValueError("Either a schedule or dependencies must be provided")
        pipeline_id = job.caller_filename.replace(".py", "").replace(".pyc", "").lower()
        if schedule and schedule.name:
            schedule_name = sanitize_id(schedule.name).lower()
        else:
            schedule_name = None
        if schedule_name:
            pipeline_id = f"{pipeline_id}_{schedule_name}"
        super().__init__(group_id=pipeline_id, orchestration_cls=orchestration_cls, group=dag, **kwargs)
        self.__orchestration = orchestration
        self.__job = job
        self.__options = job.options
        self.__dag = dag
        self.__pipeline_id = pipeline_id
        self.__schedule = schedule
        self.__schedule_name = schedule_name
        self.__sl_schedule_parameter_name = job.sl_schedule_parameter_name
        self.__sl_schedule_format = job.sl_schedule_format
        self.__tasks: List[T] = [] # ordered list of tasks
        self.__tasks_names: List[str] = list()

        tags = self.get_context_var(var_name='tags', default_value="").split()

        catchup: bool = False

        cron: Optional[str] = None

        load_dependencies: Optional[bool] = None
 
        datasets: Optional[List[StarlakeDataset]] = None

        graphs: Optional[Set[TreeNodeMixin]] = None

        if schedule is not None:
            cron = schedule.cron
            for domain in schedule.domains:
                tags.append(domain.name)

        elif dependencies is not None:
            cron = job.caller_globals.get('cron', None)

            if cron is not None:
                if cron.lower().strip() == "none":
                    cron = None
                elif not is_valid_cron(cron):
                    raise ValueError(f"Invalid cron expression: {cron}")

            catchup = cron is not None and self.get_context_var(var_name='catchup', default_value='False').lower() == 'true'

            load_dependencies = self.get_context_var(var_name='load_dependencies', default_value='False').lower() == 'true'

            filtered_datasets: Set[str] = set(job.caller_globals.get('filtered_datasets', []))

            computed_schedule = dependencies.get_schedule(
                cron=cron, 
                load_dependencies=load_dependencies,
                filtered_datasets=filtered_datasets,
                sl_schedule_parameter_name=self.sl_schedule_parameter_name,
                sl_schedule_format=self.sl_schedule_format
            )

            if computed_schedule is not None:
                if isinstance(computed_schedule, str):
                    cron = computed_schedule
                else:
                    datasets = computed_schedule

            graphs = dependencies.graphs(load_dependencies=load_dependencies)

        self.__tags = tags

        self.__cron = cron

        self.__catchup = catchup

        self.__load_dependencies = load_dependencies

        self.__graphs = graphs

        self.__datasets = datasets

        if add_dag_dependency:
            TaskLinker = Callable[[Union[T, GT], Union[T, GT]], Any]
            self.__add_dag_dependency: TaskLinker = add_dag_dependency
        else:
            self.__add_dag_dependency = None

        uris: Set[str] = set(map(lambda dataset: dataset.uri, datasets or []))
        sorted_crons_by_frequency: Tuple[Dict[int, List[str]], List[str]] = sort_crons_by_frequency(set(self.scheduled_datasets.values()))
        crons_by_frequency = sorted_crons_by_frequency[0]
        self.__crons_by_frequency = crons_by_frequency
        sorted_crons = sorted_crons_by_frequency[1]
        if cron:
            cron_expr = cron
        elif len(uris) == len(self.scheduled_datasets) and len(crons_by_frequency.keys()) > 0:
            cron_expr = sorted_crons[0]
        else:
            cron_expr = None

        self.__cron_expr = cron_expr

        self.__inner_tasks: Dict[str, AbstractTask] = dict()

    def __exit__(self, exc_type, exc_value, traceback):
        # call the parent class __exit__ method to clean up tasks and groups
        super().__exit__(exc_type, exc_value, traceback)
        if self.orchestration:
            # register the pipeline to the orchestration
            self.orchestration.pipelines.append(self)

        def get_node(dependency: AbstractDependency) -> Optional[Union[T, GT]]:
            if isinstance(dependency, AbstractTaskGroup):
                return dependency.group
            elif isinstance(dependency, AbstractTask):
                return dependency.task
            else:
                return None

        def update_group_dependencies(group: AbstractTaskGroup):
            def update_dependencies(upstream_dependencies, root_key):
                root = group.get_dependency(root_key)
                root_node: Optional[Union[T, GT]] = get_node(root)
                if isinstance(root, AbstractTaskGroup) and root_key != group.group_id:
                    update_group_dependencies(root)
                if root_key in upstream_dependencies:
                    for key in upstream_dependencies[root_key]:
                        downstream = group.get_dependency(key)
                        downstream_node: Optional[Union[T, GT]] = get_node(downstream)
                        if isinstance(downstream, AbstractTaskGroup) and key != group.group_id:
                            update_group_dependencies(downstream)

                        if root_node and downstream_node:
                            #root >> downstream
                            children = set(map(lambda node: node.id, root.downstreams))
                            found = False
                            for parent in downstream.node.parents:
                                if parent.id in children:
                                    found = True
                                    break
                            if not found and self.__add_dag_dependency:
                                self.__add_dag_dependency(root_node, downstream_node)

                        update_dependencies(upstream_dependencies, key)

            upstream_dependencies = group.upstream_dependencies
            upstream_keys = upstream_dependencies.keys()
            downstream_keys = group.downstream_dependencies.keys()
            root_keys = upstream_keys - downstream_keys

            if not root_keys and len(upstream_keys) == 0 and len(downstream_keys) == 0:
                root_keys = group.dependencies_dict.keys()

            for root_key in root_keys:
                update_dependencies(upstream_dependencies, root_key)

        update_group_dependencies(self)

        def walk_tree(node: AbstractDependency, level:int = 0):
            if isinstance(node, AbstractTaskGroup):
                if len(node.roots) > 0:
                    for root in node.roots:
                        walk_tree(root, level+1)
                for dependency in node.dependencies:
                    walk_tree(dependency, level+1)
            elif isinstance(node, AbstractTask):
                check = True
                for dep in node.upstreams:
                    if isinstance(dep, AbstractTask) and dep.task not in self.tasks:
                        check = False
                        break
                if check and node.task not in self.tasks:
                    self.__tasks_names.append(node.task_id)
                    self.__tasks.append(node.task)
                for dep in node.downstreams:
                    walk_tree(dep, level+1)

        walk_tree(self)

        # print the resulting pipeline
        print(' >> '.join(self.tasks_names))

        return False

    @property
    def orchestration(self) -> Optional[AbstractOrchestration[U, T, GT, E]]:
        return self.__orchestration

    @property
    def dag(self) -> U:
        return self.__dag

    @dag.setter
    def dag(self, dag: U) -> None:
        self.__dag = dag

    @property
    def graphs(self) -> Optional[Set[TreeNodeMixin]]:
        return self.__graphs

    def __create_task(self, task_id: str, task_name: str, task_type: str, task_sink: Optional[str]) -> T:
        """Create a task.
        Args:
            task_id (str): The task id.
            task_name (str): The task name.
            task_type (str): The task type.
            task_sink (Optional[str]): The task sink.
        Returns:
            T: The task.
        """
        if task_id in self.__inner_tasks.keys():
            return self.__inner_tasks[task_id]
        if (task_type == StarlakeDependencyType.TASK):
            task = self.sl_transform(
                task_id=task_id, 
                transform_name=task_name,
                params={'sink': task_sink},
            )
            self.__inner_tasks[task_id] = task
            return task
        else:
            load_domain_and_table = task_name.split(".", 1)
            domain = load_domain_and_table[0]
            table = load_domain_and_table[-1]
            task = self.sl_load(
                task_id=task_id, 
                domain=domain, 
                table=table,
            )
            self.__inner_tasks[task_id] = task
            return task

    def dependency_to_task(self, dependency: DependencyMixin) -> Union[AbstractTask, AbstractTaskGroup]:
        if isinstance(dependency, AbstractTask) or isinstance(dependency, AbstractTaskGroup):
            return dependency
        elif isinstance(dependency, StarlakeDependency):
            task_name = dependency.name
            task_type = dependency.dependency_type
            task_id = f"{dependency.uri}_{task_type}"
            task_sink = dependency.sink
            return self.__create_task(task_id, task_name, task_type, task_sink)
        else:
            raise ValueError(f"Unsupported dependency type: {type(dependency)}")

    @property
    def tasks(self) -> List[T]:
        return self.__tasks

    @property
    def tasks_names(self) -> List[str]:
        return self.__tasks_names

    @final
    @property
    def job(self) -> IStarlakeJob[T, E]:
        return self.__job

    @final
    @property
    def pipeline_id(self) -> str:
        return self.__pipeline_id

    @final
    @property
    def schedule(self) -> Optional[StarlakeSchedule]:
        return self.__schedule

    @final
    @property
    def schedule_name(self) -> Optional[str]:
        return self.__schedule_name

    @final
    @property
    def sl_schedule_parameter_name(self) -> str:
        return self.__sl_schedule_parameter_name

    @final
    @property
    def sl_schedule_format(self) -> str:
        return self.__sl_schedule_format

    @final
    @property
    def cron(self) -> Optional[str]:
        return self.__cron

    @final
    @property
    def computed_cron_expr(self) -> Optional[str]:
        return self.__cron_expr

    @property
    def catchup(self) -> bool:
        return self.__catchup

    @property
    def tags(self) -> List[str]:
        return self.__tags

    def sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        if cron_expr:
            return sl_cron_start_end_dates(cron_expr) #FIXME using execution date from context
        return None

    @final
    @property
    def caller_globals(self) -> dict:
        return self.job.caller_globals

    @final
    @property
    def load_dependencies(self) -> Optional[bool]:
        return self.__load_dependencies

    @final
    @property
    def datasets(self) -> Optional[List[StarlakeDataset]]:
        return self.__datasets

    @final
    def find_dataset_by_name(self, name: str) -> Optional[StarlakeDataset]:
        return next((dataset for dataset in self.datasets or [] if dataset.name == name), None)

    @property
    def scheduled_datasets(self) -> dict:
        return {dataset.name: dataset.cron for dataset in self.datasets or [] if dataset.cron is not None and dataset.name is not None}

    @final
    @property
    def not_scheduled_datasets(self) -> List[StarlakeDataset]:
        return [dataset for dataset in self.datasets or [] if dataset.name not in self.scheduled_datasets.keys()]

    @final
    @property
    def crons_by_frequency(self) -> Dict[int, List[str]]:
        return self.__crons_by_frequency

    @final
    @property
    def least_frequent_datasets(self) -> List[StarlakeDataset]:
        least_frequent_datasets: List[StarlakeDataset] = []
        if len(self.crons_by_frequency.keys()) > 1: # we have at least 2 distinct frequencies
            most_frequent_datasets: List[str] = list(map(lambda dataset: dataset.name, self.most_frequent_datasets or []))
            least_frequent_datasets = list(filter(lambda dataset: dataset.name not in most_frequent_datasets, self.datasets or []))
        return least_frequent_datasets

    @final
    @property
    def most_frequent_datasets(self) -> List[StarlakeDataset]:
        if self.crons_by_frequency is None or len(self.crons_by_frequency.keys()) == 0:
            min_interval = None
        else:
            min_interval = min(self.crons_by_frequency.keys())
        if min_interval:
            most_frequent_crons = set(self.crons_by_frequency[min_interval])
        else:
            most_frequent_crons = []
        most_frequent_datasets: List[StarlakeDataset] = []
        for name, cron in self.scheduled_datasets.items() :
            if cron in most_frequent_crons:
                dataset = self.find_dataset_by_name(name)
                if dataset:
                    most_frequent_datasets.append(dataset)
        return most_frequent_datasets

    @final
    @property
    def events(self) -> Optional[List[E]]:
        return list(map(lambda dataset: self.to_event(dataset=dataset), self.datasets or []))

    @final
    @property
    def pre_load_strategy(self) -> StarlakePreLoadStrategy:
        return self.job.pre_load_strategy

    @final
    @property
    def cron_period_frequency(self) -> StarlakeCronPeriod:
        return self.job.cron_period_frequency

    @final
    @property
    def options(self) -> dict:
        return self.__options

    @final
    def get_context_var(self, var_name: str, default_value: Any) -> Any:
        return self.job.get_context_var(
            var_name=var_name, 
            default_value=default_value, 
            options=self.options
        )

    @final
    def sl_spark_config(self, spark_config_name: str) -> StarlakeSparkConfig:
        return self.job.get_spark_config(
            self.get_context_var('spark_config_name', spark_config_name), 
            **self.caller_globals.get('spark_properties', {})
        )

    @final
    def dummy_task(self, task_id: str, events: Optional[List[E]]= None, **kwargs) -> Union[AbstractTask[T], AbstractTaskGroup[GT]]:
        return self.orchestration.sl_create_task(
            task_id, 
            self.job.dummy_op(
                task_id=task_id, 
                events=events,
                **kwargs
            ),
            self
        )

    def trigger_least_frequent_datasets_task(self, **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        if not self.least_frequent_datasets:
            return None
        task_id = kwargs.get('task_id', 'trigger_least_frequent_datasets')
        kwargs.pop('task_id', None)
        return self.dummy_task(
            task_id=task_id, 
            output_datasets=self.least_frequent_datasets, #FIXME should take into account the execution date time of the underlying task
            **kwargs
        )

    @final
    def start_task(self, **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        task_id = kwargs.get('task_id', f'start_{self.schedule_name}' if self.schedule_name else 'start')
        kwargs.pop('task_id', None)
        return self.orchestration.sl_create_task(
            task_id, 
            self.job.start_op(
                task_id=task_id, 
                scheduled = self.cron is not None,
                not_scheduled_datasets=self.not_scheduled_datasets, 
                least_frequent_datasets = self.least_frequent_datasets, 
                most_frequent_datasets=self.most_frequent_datasets, 
                **kwargs
            ),
            self
        )

    @final
    def pre_tasks(self, *args, **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        task_id = kwargs.get('task_id', 'pre_tasks')
        kwargs.pop('task_id', None)
        return self.orchestration.sl_create_task(
            task_id, 
            self.job.pre_tasks(task_id=task_id, **kwargs),
            self
        )

    @final
    def sl_pre_load(self, domain: str, tables: Set[str], **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        params: dict = kwargs.get('params', dict())
        params.update({
            'cron': self.cron,
            'schedule': self.schedule_name
        })
        kwargs['params'] = params
        task_id = kwargs.get('task_id', IStarlakeJob.get_sl_pre_load_task_id(domain, self.pre_load_strategy, **kwargs))
        kwargs.pop('task_id', None)
        return self.orchestration.sl_create_task(
            task_id, 
            self.job.sl_pre_load(
                domain=domain, 
                tables=tables, 
                task_id=task_id, 
                **kwargs
            ),
            self
        )

    @final
    def skip_or_start(self, task_id: str, upstream_task: Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]], **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        if not upstream_task:
            return None
        if isinstance(upstream_task, AbstractTaskGroup):
            leaves = upstream_task.leaves
            if leaves.__len__() == 1:
                last_task = leaves[0]
                if isinstance(last_task, AbstractTask):
                    with upstream_task:
                        skip_or_start_task = self.orchestration.sl_create_task(
                            task_id, 
                            self.job.skip_or_start_op(
                                task_id=task_id, 
                                upstream_task=last_task.task, 
                                **kwargs
                            ),
                            self
                        )
                        last_task >> skip_or_start_task
                    return None
                else:
                    upstream_task = upstream_task.group
            else:
                upstream_task = upstream_task.group
        else:
            upstream_task = upstream_task.task
        return self.orchestration.sl_create_task(
            task_id, 
            self.job.skip_or_start_op(
                task_id=task_id, 
                upstream_task=upstream_task, 
                **kwargs
            ),
            self
        ) 

    @final
    def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        params: dict = kwargs.get('params', dict())
        params.update({
            'cron': self.cron,
            'schedule': self.schedule_name
        })
        kwargs['params'] = params
        return self.orchestration.sl_create_task(
            task_id,
            self.job.sl_import(
                task_id=task_id,
                domain=domain,
                tables=tables,
                **kwargs
            ),
            self
        )

    @final
    def sl_load(self, task_id: str, domain: str, table: str, **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        name=f'{domain}.{table}'
        params: dict = kwargs.get('params', dict())
        params.update({
            'cron': self.cron,
            'sl_schedule_parameter_name': self.sl_schedule_parameter_name, 
            'sl_schedule_format': self.sl_schedule_format
        })
        kwargs['params'] = params
        kwargs.pop('spark_config', None)
        kwargs.pop('dataset', None)
        return self.orchestration.sl_create_task(
            task_id, 
            self.job.sl_load(
                task_id=task_id, 
                domain=domain, 
                table=table, 
                spark_config=self.sl_spark_config(name.lower()), 
                dataset=StarlakeDataset(name, **kwargs),
                **kwargs
            ),
            self
        )

    @final
    def sl_transform(self, task_id: str, transform_name: str, **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        params: dict = kwargs.get('params', dict())
        params.update({
            'cron': self.cron,
            'cron_expr': self.computed_cron_expr,
            'sl_schedule_parameter_name': self.sl_schedule_parameter_name, 
            'sl_schedule_format': self.sl_schedule_format
        })
        kwargs['params'] = params
        kwargs.pop('transform_options', None)
        kwargs.pop('spark_config', None)
        kwargs.pop('dataset', None)
        return self.orchestration.sl_create_task(
            task_id, 
                self.job.sl_transform(
                task_id=task_id, 
                transform_name=transform_name, 
                transform_options=self.sl_transform_options(self.computed_cron_expr), 
                spark_config=self.sl_spark_config(transform_name.lower()),                 dataset=StarlakeDataset(transform_name, **kwargs),
                **kwargs
            ),
            self
        )

    @final
    def post_tasks(self, **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        task_id = kwargs.get('task_id', 'post_tasks')
        kwargs.pop('task_id', None)
        return self.orchestration.sl_create_task(
            task_id, 
            self.job.post_tasks(),
            self
        )

    @final
    def end_task(self, events: Optional[List[E]]= None, **kwargs) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        pipeline_id = self.pipeline_id
        if not events:
            events = list(map(lambda dataset: self.to_event(dataset=dataset, source=pipeline_id), [StarlakeDataset(name=pipeline_id, cron=self.cron)]))
        task_id = kwargs.get('task_id', f"end_{self.schedule_name}" if self.schedule_name else 'end')
        kwargs.pop('task_id', None)
        kwargs.pop('events', None)
        end = self.orchestration.sl_create_task(
            task_id, 
            self.job.end_op(
                task_id=task_id, 
                events=events, 
                **kwargs
            ),
            self
        )
        return end

    @final
    def print_pipeline(self) -> None:
        print(f"Pipeline {self.pipeline_id}:")
        self.print_group(0)

    @final
    def __repr__(self):
        return self.print_pipeline()

    def deploy(self, **kwargs) -> None:
        """Deploy the pipeline."""
        return None

    @abstractmethod
    def run(self, logical_date: Optional[str] = None, timeout: str = '120', mode: StarlakeExecutionMode = StarlakeExecutionMode.RUN, **kwargs) -> None:
        """Run the pipeline.
        Args:
            logical_date (Optional[str]): the logical date.
            timeout (str): the timeout in seconds.
            mode (StarlakeExecutionMode): the execution mode.
        """
        ...

    @final
    def dry_run(self, **kwargs) -> None:
        """Dry run the pipeline.
        Args:
            options (dict[str, Any]): the options to run the pipeline.
        """
        self.run(mode=StarlakeExecutionMode.DRY_RUN, **kwargs)

    def backfill(self, timeout: str = '120', start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> None:
        """Backfill the pipeline.
        Args:
            timeout (str): the timeout in seconds.
            start_date (Optional[str]): the start date.
            end_date (Optional[str]): the end date.
        """
        from datetime import datetime
        cron = self.cron
        if not cron or cron.strip().lower() == 'none':
            raise ValueError("The pipeline must have a cron expression to backfill")
        if not start_date or start_date.strip().lower() == 'none':
            raise ValueError("The pipeline must have a start date to backfill")
        if not end_date or end_date.strip().lower() == 'none':
            end_date = datetime.fromtimestamp(datetime.now().timestamp()).isoformat()
        from croniter import croniter
        start_time = datetime.fromisoformat(start_date)
        end_time = datetime.fromisoformat(end_date)
        if start_time > end_time:
            raise ValueError("The start date must be before the end date")
        iter = croniter(cron, start_time)
        # get the start and end date of the current cron iteration
        curr: datetime = iter.get_current(datetime)
        previous: datetime = iter.get_prev(datetime)
        next: datetime = croniter(cron, previous).get_next(datetime)
        if curr == next :
            sl_end_date = curr
        else:
            sl_end_date = previous
        sl_start_date: datetime = croniter(cron, sl_end_date).get_prev(datetime)
        while sl_start_date <= end_time:
            self.run(logical_date= sl_start_date.isoformat(), timeout=timeout, **kwargs)
            sl_end_date = croniter(cron, sl_end_date).get_next(datetime)
            sl_start_date = croniter(cron, sl_end_date).get_prev(datetime)

    def delete(self, **kwargs) -> None:
        """Delete the pipeline."""
        return None


class AbstractOrchestration(Generic[U, T, GT, E]):
    def __init__(self, job: IStarlakeJob[T, E], **kwargs) -> None:
        super().__init__(**kwargs)
        self.__job = job
        self._pipelines = []

    def __enter__(self):
        self.pipelines.clear()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    @classmethod
    def sl_orchestrator(cls) -> str:
        return None

    @property
    def job(self) -> IStarlakeJob[T, E]:
        return self.__job

    @property
    def pipelines(self) -> List[AbstractPipeline[U, T, GT, E]]:
        return self._pipelines

    @abstractmethod
    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[U, T, GT, E]:
        """Create a pipeline."""
        pass

    def sl_create_task(self, task_id: str, task: Optional[Union[T, GT]], pipeline: AbstractPipeline[U, T, GT, E]) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        if task is None:
            return None
        return AbstractTask(task_id, task)

    @abstractmethod
    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[U, T, GT, E], **kwargs) -> AbstractTaskGroup[GT]:
        pass

    @classmethod
    def from_native(cls, native: Any) -> Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]:
        """Create a task or task group from a native object.
        Args:
            native (Any): the native object.
        Returns:
            Optional[Union[AbstractTask[T], AbstractTaskGroup[GT]]]: the task or task group.
        """
        return None

class OrchestrationFactory:
    _registry = {}

    _initialized = False

    @classmethod
    def register_orchestrations_from_package(cls, package_name: str = "ai.starlake") -> None:
        """
        Dynamically load all classes implementing AbstractOrchestration from the given root package, including sub-packages,
        and register them in the OrchestrationRegistry.
        """
        print(f"Registering orchestrations from package {package_name}")
        package = importlib.import_module(package_name)
        package_path = os.path.dirname(package.__file__)

        for root, dirs, files in os.walk(package_path):
            # Convert the filesystem path back to a Python module path
            relative_path = os.path.relpath(root, package_path)
            if relative_path == ".":
                module_prefix = package_name
            else:
                module_prefix = f"{package_name}.{relative_path.replace(os.path.sep, '.')}"

            for file in files:
                if file.endswith(".py") and file != "__init__.py":
                    module_name = os.path.splitext(file)[0]
                    full_module_name = f"{module_prefix}.{module_name}"

                    try:
                        module = importlib.import_module(full_module_name)
                    except ImportError as e:
                        print(f"Failed to import module {full_module_name}: {e}")
                        continue
                    except AttributeError as e:
                        print(f"Failed to import module {full_module_name}: {e}")
                        continue

                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if issubclass(obj, AbstractOrchestration) and obj is not AbstractOrchestration:
                            OrchestrationFactory.register_orchestration(obj)

    @classmethod
    def register_orchestration(cls, orchestration_class: Type[AbstractOrchestration]):
        orchestrator = orchestration_class.sl_orchestrator()
        if orchestrator is None:
            raise ValueError("Orchestration must define a valid orchestrator")
        cls._registry.update({orchestrator: orchestration_class})
        print(f"Registered orchestration {orchestration_class} for orchestrator {orchestrator}")

    @classmethod
    def create_orchestration(cls, job: IStarlakeJob[T, E], **kwargs) -> AbstractOrchestration[U, T, GT, E]:
        if not cls._initialized:
            cls.register_orchestrations_from_package()
            cls._initialized = True
        orchestrator = job.sl_orchestrator()
        if orchestrator not in cls._registry:
            raise ValueError(f"Unknown orchestrator type: {orchestrator}")
        return cls._registry[orchestrator](job, **kwargs)
