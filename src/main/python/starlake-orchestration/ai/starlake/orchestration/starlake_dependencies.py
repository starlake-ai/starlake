from __future__ import annotations

from typing import List, Union

from enum import Enum

StarlakeDependencyType = Enum("StarlakeDependencyType", ["task", "table"])

class StarlakeDependency():
    def __init__(self, name: str, type: StarlakeDependencyType, cron: Union[str, None]= None, dependencies: List[StarlakeDependency]= [], **kwargs):
        """Initializes a new StarlakeDependency instance.

        Args:
            name (str): The required dependency name.
            type (StarlakeDependencyType): The required dependency type.
            cron (str): The optional cron.
            dependencies (List[StarlakeDependency]): The optional dependencies.
        """
        self.name = name
        self.type = type
        self.cron = cron
        self.dependencies = dependencies

class StarlakeDependencies():
    def __init__(self, dependencies: Union[str, List[StarlakeDependency]], **kwargs):
        """Initializes a new StarlakeDependencies instance.

        Args:
            dependencies (List[StarlakeDependency]): The required dependencies.
        """
        if isinstance(dependencies, str):
            import json
            task_deps: List[dict] = json.loads(str)
            def generate_dependency(task) -> StarlakeDependency:
                data: dict = task['data']

                if data.get('typ', None) == 'task':
                    _type = StarlakeDependencyType.task
                else:
                    _type = StarlakeDependencyType.table

                _cron: Union[str, None] = data.get('cron', None)

                if _cron is None or _cron.lower().strip() == 'none':
                    cron = None
                else:
                    cron = _cron

                return StarlakeDependency(name=data["name"], type=_type, cron=cron, dependencies=[generate_dependency(subtask) for subtask in data.get('children', [])])
            self.dependencies = [generate_dependency(task) for task in task_deps]
        else:
            self.dependencies = dependencies

