from __future__ import annotations

from typing import List, Union

from enum import Enum

StarlakeDependencyType = Enum("StarlakeDependencyType", ["task", "table"])

class StarlakeDependency():
    def __init__(self, name: str, dependency_type: StarlakeDependencyType, cron: Union[str, None]= None, dependencies: List[StarlakeDependency]= [], **kwargs):
        """Initializes a new StarlakeDependency instance.

        Args:
            name (str): The required dependency name.
            dependency_type (StarlakeDependencyType): The required dependency dependency_type.
            cron (str): The optional cron.
            dependencies (List[StarlakeDependency]): The optional dependencies.
        """
        self.name = name
        self.dependency_type = dependency_type
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

            _cron: Union[str, None] = data.get('cron', None)

            if _cron is None or _cron.lower().strip() == 'none':
                cron = None
            else:
                cron = _cron

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
