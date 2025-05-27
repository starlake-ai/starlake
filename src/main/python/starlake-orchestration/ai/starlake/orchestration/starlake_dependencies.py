from __future__ import annotations

from ai.starlake.common import sanitize_id, is_valid_cron

from ai.starlake.dataset import StarlakeDataset

from collections import defaultdict

from typing import Dict, Iterable, List, Optional, Sequence, Set, Union

from enum import Enum

class StarlakeDependencyType(str, Enum):
    TASK = "task"
    TABLE = "table"

    def __str__(self):
        return self.value

import warnings

warnings.simplefilter("default", DeprecationWarning)

class DependencyMixin:
    """Abstract interface to define a dependency."""
    def __init__(self, id: str) -> None:
        self.__id = id
        self.__upstreams: Set["DependencyMixin"] = set()
        self.__downstreams: Set["DependencyMixin"] = set()

    @property
    def id(self) -> str:
        return self.__id

    def __rshift__(self, other: Union[Iterable["DependencyMixin"], "DependencyMixin"]) -> Union[Iterable["DependencyMixin"], "DependencyMixin"]:
        """Add self as an upstream dependency to other.
        Args:
            other (DependencyMixin): the upstream dependency.
        """
        self.__add_downstreams(other)
        if isinstance(other, Sequence):
            for dep in other:
                dep.__add_upstreams(self)
        else:
            other.__add_upstreams(self)
        return other

    def __lshift__(self, other: Union[Iterable["DependencyMixin"], "DependencyMixin"]) -> Union[Iterable["DependencyMixin"], "DependencyMixin"]:
        """Add other as an upstream dependency to self.
        Args:
            other (DependencyMixin): the upstream dependency.
        """
        self.__add_upstreams(other)
        if isinstance(other, Sequence):
            for dep in other:
                dep.__add_downstreams(self)
        else:
            other.__add_downstreams(self)
        return other

    def __add_upstreams(self, dependencies: Union[Iterable["DependencyMixin"], "DependencyMixin"]) -> None:
        """Add upstreams to the current dependency.
        Args:
            dependencies (Union[Iterable[DependencyMixin], DependencyMixin]): the upstreams to add.
        """
        if isinstance(dependencies, Sequence):
            self.__upstreams.update(dependencies)
        else:
            self.__upstreams.add(dependencies)

    @property
    def upstreams(self) -> Set["DependencyMixin"]:
        return self.__upstreams

    @upstreams.setter
    def upstreams(self, value: Set["DependencyMixin"]) -> None:
        self.__upstreams = value

    def __add_downstreams(self, dependencies: Union[Iterable["DependencyMixin"], "DependencyMixin"]) -> None:
        """Add downstreams to the current dependency.
        Args:
            dependencies (Union[Iterable[DependencyMixin], DependencyMixin]): the downstreams to add.
        """
        if isinstance(dependencies, Sequence):
            self.__downstreams.update(dependencies)
        else:
            self.__downstreams.add(dependencies)

    @property
    def downstreams(self) -> Set["DependencyMixin"]:
        return self.__downstreams

    @property
    def node(self):
        return TreeNodeMixin(self)

    @property
    def all_dependencies(self) -> List["DependencyMixin"]:
        """Return all dependencies."""
        dependencies: List["DependencyMixin"] = []
        dependencies.extend(self.upstreams)
        for dep in self.upstreams:
            dependencies.extend(dep.all_dependencies)
        return dependencies

    def __repr__(self):
        return f"Dependency(id={self.id}, upstreams=[{','.join([dep.id for dep in self.upstreams])}], downstreams=[{','.join([dep.id for dep in self.downstreams])}], node={self.node})"

class TreeNodeMixin:
    """Abstract interface to define a node."""
    def __init__(self, node: "DependencyMixin") -> None:
        self.__node = node
        self.__children: List["TreeNodeMixin"] = []
        self.__parents: List["TreeNodeMixin"] = []
        for parent in self.node.upstreams:
            pn = parent.node
            self.__add_parent(pn)
            pn.__add_child(self)

    @property
    def node(self) -> "DependencyMixin":
        return self.__node

    @property
    def id(self) -> str:
        return self.node.id

    def __add_parent(self, node: "TreeNodeMixin") -> None:
        """Add a parent to the current node.
        Args:
            node (TreeNodeMixin): the parent to add.
        """
        self.__parents.append(node)

    @property
    def parents(self) -> List["TreeNodeMixin"]:
        return self.__parents

    @parents.setter
    def parents(self, parents: List["TreeNodeMixin"]) -> None:
        self.__parents = parents

    def __add_child(self, node: "TreeNodeMixin") -> None:
        """Add a child to the current node.
        Args:
            node (TreeNodeMixin): the child to add.
        """
        self.__children.append(node)

    @property
    def children(self) -> List["TreeNodeMixin"]:
        return self.__children

    @children.setter
    def children(self, children: List["TreeNodeMixin"]) -> None:
        self.__children = children

    def __repr__(self):
        if len(self.parents) == 0:
            return self.node.id
        else:
            return self.node.id + " << " + "[" + ",".join([node.node.id for node in self.parents]) + "]"

class StarlakeDependency(DependencyMixin):
    def __init__(self, name: str, dependency_type: StarlakeDependencyType, cron: Optional[str]= None, dependencies: List[StarlakeDependency]= [], sink: Optional[str]= None, stream: Optional[str]= None, freshness: int = 0, **kwargs):
        """Initializes a new StarlakeDependency instance.

        Args:
            name (str): The required dependency name.
            dependency_type (StarlakeDependencyType): The required dependency dependency_type.
            cron (str): The optional cron.
            dependencies (List[StarlakeDependency]): The optional dependencies.
            sink (str): The optional sink.
            stream (str): The optional stream.
            freshness (int): The freshness in seconds. Defaults to 0.
        """
        super().__init__(name)
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
        self._freshness = freshness
        for dependency in self._dependencies:
            self << dependency

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

    @property
    def freshness(self) -> int:
        return self._freshness

    def __repr__(self) -> str:
        return f"StarlakeDependency(name={self.name}, dependency_type={self.dependency_type}, cron={self.cron}, dependencies={self.dependencies}, sink={self.sink}, stream={self.stream}, freshness={self.freshness}, upstreams=[{','.join([dep.id for dep in self.upstreams])}], downstreams=[{','.join([dep.id for dep in self.downstreams])}], node={self.node})"

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

            freshness: int = data.get('freshness', 0)

            return StarlakeDependency(
                name=name,
                dependency_type=dependency_type, 
                cron=cron, 
                dependencies=[generate_dependency(dependency) for dependency in task.get('children', [])],
                sink=sink,
                stream=stream,
                freshness=freshness,
            )

        if isinstance(dependencies, str):
            import json
            self.dependencies = [generate_dependency(task) for task in json.loads(dependencies)]
        else:
            self.dependencies = dependencies

        first_level_tasks: Set[str] = set()
        filtered_datasets: Set[str] = kwargs.get('filtered_datasets', set())
        all_dependencies: Dict[str, StarlakeDependency] = defaultdict()
        all_nodes: Dict[str, TreeNodeMixin] = defaultdict()

        def load_task_dependencies(task: StarlakeDependency):
            if len(task.dependencies) > 0:
                for dependency in task.dependencies:
                    if dependency.name not in all_dependencies.keys():
                        all_dependencies[dependency.name] = dependency
                        load_task_dependencies(dependency)

        def load_nodes(task: StarlakeDependency):
            node = dependency.node
            all_nodes[dependency.id] = node
            for parent in node.parents:
                if parent.id not in all_nodes.keys():
                    all_nodes[parent.id] = parent
                    load_nodes(parent)

        for dependency in self.dependencies:
            first_level_tasks.add(dependency.name)
            filtered_datasets.add(dependency.uri)
            load_task_dependencies(dependency)
            load_nodes(dependency)

        self.__all_dependencies = all_dependencies
        self.__first_level_tasks = first_level_tasks
        self.__filtered_datasets = filtered_datasets
        self.__all_nodes = all_nodes

    @property
    def all_dependencies(self) -> Set[str]:
        return self.__all_dependencies.keys()

    @property
    def first_level_tasks(self) -> Set[str]:
        return self.__first_level_tasks

    @property
    def filtered_datasets(self) -> Set[str]:
        """Return the filtered datasets."""
        return self.__filtered_datasets

    def get_dependency(self, name: str) -> Optional[StarlakeDependency]:
        return self.__dependencies_map.get(name, None)

    def graphs(self, load_dependencies: bool = False) -> Set[TreeNodeMixin]:
        """Return the graphs from the dependencies.
        Args:
            load_dependencies (bool): whether to load all dependencies or not.
        """
        temp_graphs: Dict[str, TreeNodeMixin] = dict()
        parents: Set[str] = set()
        for node in filter(lambda node: node.id not in temp_graphs.keys() and (len(node.parents) > 0 or node.id in self.first_level_tasks), self.__all_nodes.values()):
            if load_dependencies or node.id in self.first_level_tasks:
                temp_graphs[node.id] = node
                __parents = []
                for parent in node.parents:
                    if load_dependencies or parent.id in self.first_level_tasks:
                        __parents.append(parent)
                        parents.add(parent.id)
                if not load_dependencies:
                    node.parents = __parents
        graphs: Set[TreeNodeMixin] = set()
        for graph in temp_graphs.values():
            if load_dependencies and (graph.id not in parents or (len(graph.parents) > 0)):
                graphs.add(graph)
            elif not load_dependencies and graph.id in self.first_level_tasks and (graph.id not in parents or (len(graph.parents) > 0)):
                graphs.add(graph)
        return graphs

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
                        freshness = dependency.freshness
                        if uri not in uris and uri not in temp_filtered_datasets:
                            kw = dict()
                            kw['freshness'] = freshness
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
