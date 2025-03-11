from __future__ import annotations

from ai.starlake.common import sanitize_id, is_valid_cron

from ai.starlake.dataset import StarlakeDataset

from typing import List, Optional, Set, Union

from enum import Enum

class StarlakeDependencyType(str, Enum):
    TASK = "task"
    TABLE = "table"

    def __str__(self):
        return self.value

import warnings

warnings.simplefilter("default", DeprecationWarning)

class StarlakeDependency():
    def __init__(self, name: str, dependency_type: StarlakeDependencyType, cron: Optional[str]= None, dependencies: List[StarlakeDependency]= [], sink: Optional[str]= None, stream: Optional[str]= None, **kwargs):
        """Initializes a new StarlakeDependency instance.

        Args:
            name (str): The required dependency name.
            dependency_type (StarlakeDependencyType): The required dependency dependency_type.
            cron (str): The optional cron.
            dependencies (List[StarlakeDependency]): The optional dependencies.
            sink (str): The optional sink.
            stream (str): The optional stream.
        """
        self._name = name
        if sink:
            domain_table = sink.split(".")
        else:
            domain_table = name.split(".")
        self._domain = domain_table[0]
        self._table = domain_table[-1]
        self._uri = sanitize_id(self.sink).lower()
        self._dependency_type = dependency_type
        if cron is not None:
            if cron.lower().strip() == 'none':
                cron = None
            elif not is_valid_cron(cron):
                raise ValueError(f"Invalid cron expression: {cron} for dependency {name}")
        self._cron = cron
        self._dependencies = dependencies
        self._stream = stream

    @property
    def name(self) -> str:
        return self._name

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def table(self) -> str:
        return self._table

    @property
    def dependency_type(self) -> StarlakeDependencyType:
        return self._dependency_type

    @property
    def cron(self) -> Optional[str]:
        return self._cron

    @property
    def dependencies(self) -> List[StarlakeDependency]:
        return self._dependencies

    @property
    def stream(self) -> Optional[str]:
        return self._stream

    @property
    def sink(self) -> Optional[str]:
        return f"{self.domain}.{self.table}"

    def __repr__(self) -> str:
        return f"StarlakeDependency(name={self.name}, dependency_type={self.dependency_type}, cron={self.cron}, dependencies={self.dependencies}, sink={self.sink}, stream={self.stream})"
class StarlakeDependencies():
    def __init__(self, dependencies: Union[str, List[StarlakeDependency]], **kwargs):
        """Initializes a new StarlakeDependencies instance.

        Args:
            dependencies (List[StarlakeDependency]): The required dependencies.
        """
        def generate_dependency(task: dict) -> StarlakeDependency:
            data: dict = task.get('data', {})

            name = data.get('name', None)
            if name is None:
                raise ValueError(f"Missing name in task {task}")

            if data.get('typ', None) == 'task':
                dependency_type = StarlakeDependencyType.TASK
            else:
                dependency_type = StarlakeDependencyType.TABLE

            cron: Optional[str] = data.get('cron', None)

            sink = data.get('sink', None)

            stream: Optional[str] = data.get('stream', None)

            return StarlakeDependency(
                name=name,
                dependency_type=dependency_type, 
                cron=cron, 
                dependencies=[generate_dependency(dependency) for dependency in task.get('children', [])],
                sink=sink,
                stream=stream
            )

        if isinstance(dependencies, str):
            import json
            self.dependencies = [generate_dependency(task) for task in json.loads(dependencies)]
        else:
            self.dependencies = dependencies

        all_dependencies: Set[str] = set()
        first_level_tasks: Set[str] = set()
        filtered_datasets: Set[str] = set()

        def load_task_dependencies(task: StarlakeDependency):
            if len(task.dependencies) > 0:
                for dependency in task.dependencies:
                    all_dependencies.add(dependency.name)
                    load_task_dependencies(dependency)

        for task in self.dependencies:
            name = task.name
            first_level_tasks.add(name)
            filtered_datasets.add(task.uri)
            load_task_dependencies(task)

        self.all_dependencies = all_dependencies
        self.first_level_tasks = first_level_tasks
        self.filtered_datasets = filtered_datasets

    def get_schedule(self, cron: Optional[str], load_dependencies: bool, filtered_datasets: Optional[Set[str]] = None, sl_schedule_parameter_name: Optional[str] = None, sl_schedule_format: Optional[str] = None) -> Union[str, List[StarlakeDataset], None]:

        cron_expr = cron

        if cron_expr is not None:
            if cron_expr.lower().strip() == 'none':
                cron_expr = None
            elif not is_valid_cron(cron_expr):
                raise ValueError(f"Invalid cron expression: {cron_expr}")
        
        if cron_expr is not None:
            return cron_expr # return the cron expression

        elif not load_dependencies:
            uris: Set[str] = set()

            datasets: List[StarlakeDataset] = []

            temp_filtered_datasets: Set[str] = self.filtered_datasets.copy()

            if filtered_datasets:
                temp_filtered_datasets.update(filtered_datasets)

            def load_datasets(task: StarlakeDependency):
                if len(task.dependencies) > 0:
                    for dependency in task.dependencies:
                        name = dependency.name
                        sink = dependency.sink
                        uri = dependency.uri
                        stream = dependency.stream
                        if uri not in uris and uri not in temp_filtered_datasets:
                            kw = dict()
                            if dependency.cron is not None:
                                kw['cron'] = dependency.cron
                            if sl_schedule_parameter_name is not None:
                                kw['sl_schedule_parameter_name'] = sl_schedule_parameter_name
                            if sl_schedule_format is not None:
                                kw['sl_schedule_format'] = sl_schedule_format
                            if sink is not None:
                                kw['sink'] = sink
                            if stream is not None:
                                kw['stream'] = stream
                            dataset = StarlakeDataset(name=name, **kw)
                            uris.add(uri)
                            datasets.append(dataset)

            for task in self.dependencies:
                load_datasets(task)

            return datasets # return the datasets

        else:
            return None # return None

    def __repr__(self) -> str:
        return f"StarlakeDependencies(dependencies={self.dependencies})"

    def __str__(self) -> str:
        return f"StarlakeDependencies(dependencies={self.dependencies})"

    def __iter__(self):
        return iter(self.dependencies)

    def __getitem__(self, index):
        return self.dependencies[index]

    def __len__(self):
        return len(self.dependencies)
