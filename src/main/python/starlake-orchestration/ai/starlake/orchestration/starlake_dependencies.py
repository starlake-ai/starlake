from __future__ import annotations

from ai.starlake.common import sanitize_id, is_valid_cron

from ai.starlake.resource import StarlakeResource

from typing import List, Optional, Set, Union

from enum import Enum

StarlakeDependencyType = Enum("StarlakeDependencyType", ["task", "table"])

class StarlakeDependency():
    def __init__(self, name: str, dependency_type: StarlakeDependencyType, cron: Optional[str]= None, dependencies: List[StarlakeDependency]= [], **kwargs):
        """Initializes a new StarlakeDependency instance.

        Args:
            name (str): The required dependency name.
            dependency_type (StarlakeDependencyType): The required dependency dependency_type.
            cron (str): The optional cron.
            dependencies (List[StarlakeDependency]): The optional dependencies.
        """
        self.name = name
        self.dependency_type = dependency_type
        if cron is not None:
            if cron.lower().strip() == 'none':
                cron = None
            elif not is_valid_cron(cron):
                raise ValueError(f"Invalid cron expression: {cron} for dependency {name}")
        self.cron = cron
        self.dependencies = dependencies

class StarlakeDependencies():
    def __init__(self, dependencies: Union[str, List[StarlakeDependency]], **kwargs):
        """Initializes a new StarlakeDependencies instance.

        Args:
            dependencies (List[StarlakeDependency]): The required dependencies.
        """
        def generate_dependency(task: dict) -> StarlakeDependency:
            data: dict = task.get('data', {})

            if data.get('typ', None) == 'task':
                dependency_type = StarlakeDependencyType.task
            else:
                dependency_type = StarlakeDependencyType.table

            cron: Optional[str] = data.get('cron', None)

            return StarlakeDependency(
                name=data["name"], 
                dependency_type=dependency_type, 
                cron=cron, 
                dependencies=[generate_dependency(subtask) for subtask in task.get('children', [])]
            )

        if isinstance(dependencies, str):
            import json
            tasks: List[dict] = json.loads(dependencies)
            self.dependencies = [generate_dependency(task) for task in tasks]
        else:
            self.dependencies = dependencies

        all_dependencies: Set[str] = set()
        first_level_tasks: Set[str] = set()
        filtered_resources: Set[str] = set()

        def load_task_dependencies(task: StarlakeDependency):
            if len(task.dependencies) > 0:
                for subtask in task.dependencies:
                    all_dependencies.add(subtask.name)
                    load_task_dependencies(subtask)

        for task in self.dependencies:
            name = task.name
            first_level_tasks.add(name)
            filtered_resources.add(sanitize_id(name).lower())
            load_task_dependencies(task)

        self.all_dependencies = all_dependencies
        self.first_level_tasks = first_level_tasks
        self.filtered_resources = filtered_resources

    def get_schedule(self, cron: Optional[str], load_dependencies: bool, filtered_resources: Optional[Set[str]] = None, sl_schedule_parameter_name: Optional[str] = None, sl_schedule_format: Optional[str] = None) -> Union[str, List[StarlakeResource], None]:

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

            resources: List[StarlakeResource] = []

            temp_filtered_resources: Set[str] = self.filtered_resources.copy()

            if filtered_resources:
                temp_filtered_resources.update(filtered_resources)

            def load_resources(task: StarlakeDependency):
                if len(task.dependencies) > 0:
                    for child in task.dependencies:
                        uri = sanitize_id(child.name).lower()
                        if uri not in uris and uri not in temp_filtered_resources:
                            kw = dict()
                            if child.cron is not None:
                                kw['cron'] = child.cron
                            if sl_schedule_parameter_name is not None:
                                kw['sl_schedule_parameter_name'] = sl_schedule_parameter_name
                            if sl_schedule_format is not None:
                                kw['sl_schedule_format'] = sl_schedule_format
                            resource = StarlakeResource(uri, **kw)
                            uris.add(resource.uri)
                            resources.append(resource)

            for task in self.dependencies:
                load_resources(task)

            return resources # return the resources

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