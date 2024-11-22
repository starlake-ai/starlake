from __future__ import annotations

from typing import List, Union

from enum import Enum

DependencyType = Enum("DependencyType", ["task", "table"])

class StarlakeDependency():
    def __init__(self, name: str, type: DependencyType, cron: Union[str, None]= None, dependencies: List[StarlakeDependency]= [], **kwargs):
        """Initializes a new StarlakeDependency instance.

        Args:
            name (str): The required dependency name.
            type (DependencyType): The required dependency type.
            cron (str): The optional cron.
            dependencies (List[StarlakeDependency]): The optional dependencies.
        """
        self.name = name
        self.type = type
        self.cron = cron
        self.dependencies = dependencies

class StarlakeDependencies():
    def __init__(self, dependencies: List[StarlakeDependency], **kwargs):
        """Initializes a new StarlakeDependencies instance.

        Args:
            dependencies (List[StarlakeDependency]): The required dependencies.
        """
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
    
    def __contains__(self, item):
        return item in self.dependencies
    
    def __add__(self, other):
        return StarlakeDependencies(self.dependencies + other.dependencies)
    
    def __iadd__(self, other):
        self.dependencies += other.dependencies
        return self
