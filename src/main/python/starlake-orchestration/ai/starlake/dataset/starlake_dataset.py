from __future__ import annotations

from abc import abstractmethod

from ai.starlake.common import asQueryParameters, sanitize_id, sl_schedule, sl_schedule_format, is_valid_cron, StarlakeParameters

from datetime import datetime

from typing import Generic, List, Optional, TypeVar, Union

class StarlakeDataset():
    def __init__(self, name: str, parameters: Optional[dict] = None, cron: Optional[str] = None, sink: Optional[str] = None, stream: Optional[str] = None, start_time: Optional[Union[str, datetime]] = None, freshness: int = 0, **kwargs):
        """Initializes a new StarlakeDataset instance.

        Args:
            name (str): The required dataset name.
            parameters (dict, optional): The optional dataset parameters. Defaults to None.
            cron (str, optional): The optional cron. Defaults to None.
            sink (str, optional): The optional sink. Defaults to None.
            stream (str, optional): The optional stream. Defaults to None.
            start_time (Optional[Union[str, datetime]], optional): The optional start time. Defaults to None.
            freshness (int): The freshness in seconds. Defaults to 0.
        """
        self.__name = name
        self.__sink = sink
        if sink:
            domain_table = sink.split(".")
        else:
            domain_table = name.split(".")
        if len(domain_table) == 2:
            self.__domain = domain_table[0]
            self.__table = domain_table[-1]
        else:
            self.__domain = None
            self.__table = None
        self.__uri = sanitize_id(self.sink).lower()
        params = kwargs.get('params', dict())
        if cron is None:
            if parameters is not None and 'cron' in parameters:
                cron = parameters['cron']
            else:
                cron = params.get('cron', None)
        if cron:
            if cron.lower().strip() == 'none':
                cron = None
            elif not is_valid_cron(cron):
                raise ValueError(f"Invalid cron expression: {cron} for dataset {self.uri}")
        if cron is not None and parameters is not None:
            parameters.pop('cron', None)
        temp_parameters: dict = dict()
        if parameters is not None:
            temp_parameters.update(parameters)
        self.__sl_schedule_parameter_name = kwargs.get('sl_schedule_parameter_name', params.get('sl_schedule_parameter_name', StarlakeParameters.SCHEDULED_DATE_PARAMETER.value))
        self.__sl_schedule_format = kwargs.get('sl_schedule_format', params.get('sl_schedule_format', sl_schedule_format))
        self.__start_time = start_time
        if cron is not None:
            temp_parameters[self.sl_schedule_parameter_name] = sl_schedule(cron=cron, start_time=self.start_time, format=self.sl_schedule_format)
        self.__cron = cron
        self.__queryParameters = asQueryParameters(temp_parameters)
        self.__parameters = parameters
        self.__url = self.uri + self.queryParameters
        self.__stream = stream
        self.__freshness = freshness

    @property
    def name(self) -> str:
        return self.__name

    @property
    def cron(self) -> Optional[str]:
        return self.__cron

    @property
    def uri(self) -> str:
        return self.__uri

    @property
    def sl_schedule_parameter_name(self) -> str:
        return self.__sl_schedule_parameter_name

    @property
    def sl_schedule_format(self) -> str:
        return self.__sl_schedule_format

    @property
    def parameters(self) -> dict:
        return self.__parameters

    @property
    def queryParameters(self) -> str:
        return self.__queryParameters

    @property
    def url(self) -> str:
        return self.__url

    @property
    def domain(self) -> str:
        return self.__domain

    @property
    def table(self) -> str:
        return self.__table

    @property
    def stream(self) -> Optional[str]:
        return self.__stream

    @property
    def start_time(self) -> Optional[Union[str, datetime]]:
        return self.__start_time

    @property
    def sink(self) -> Optional[str]:
        return self.__sink or (f"{self.domain}.{self.table}" if self.domain and self.table else self.__name)

    @property
    def freshness(self) -> int:
        return self.__freshness

    def refresh(self, start_time: Optional[Union[str, datetime]] = None) -> StarlakeDataset:
        return StarlakeDataset(self.name, self.parameters, self.cron, self.sink, self.stream, freshness=self.freshness, start_time=start_time or self.start_time, sl_schedule_parameter_name=self.sl_schedule_parameter_name, sl_schedule_format=self.sl_schedule_format)

    @staticmethod
    def refresh_datasets(datasets: Optional[List[StarlakeDataset]]) -> Optional[List[StarlakeDataset]]:
        if datasets is not None:
            return [dataset.refresh() for dataset in datasets if dataset.cron is not None]
        else:
            return None

E = TypeVar("E")

class AbstractEvent(Generic[E]):
    @classmethod
    @abstractmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> E:
        pass

from enum import Enum

class DatasetTriggeringStrategy(str, Enum):
    ALL = "all"
    ANY = "any"

    @classmethod
    def is_valid(cls, strategy: str) -> bool:
        """Validate a dataset triggering strategy."""
        return strategy in cls.all_strategies()

    @classmethod
    def all_strategies(cls) -> set[str]:
        """Return all dataset triggering strategies."""
        return set(cls.__members__.values())

    def __str__(self):
        return self.value
