from __future__ import annotations

from abc import abstractmethod

from ai.starlake.common import asQueryParameters, sanitize_id, sl_schedule, sl_schedule_format, is_valid_cron

from typing import Generic, List, Optional, TypeVar

class StarlakeDataset():
    def __init__(self, sink: str, parameters: Optional[dict] = None, cron: Optional[str] = None, **kwargs):
        """Initializes a new StarlakeDataset instance.

        Args:
            sink (str): The required dataset sink.
            parameters (dict, optional): The optional dataset parameters. Defaults to None.
        """
        self._sink = sink
        domain_table = sink.split(".")
        self._domain = domain_table[0]
        self._table = domain_table[-1]
        if cron is None:
            if parameters is not None and 'cron' in parameters:
                cron = parameters['cron']
            elif 'params' in kwargs:
                cron = kwargs['params'].get('cron', None)
        if cron:
            if cron.lower().strip() == 'none':
                cron = None
            elif not is_valid_cron(cron):
                raise ValueError(f"Invalid cron expression: {cron} for dataset {uri}")
        if cron is not None and parameters is not None:
            parameters.pop('cron', None)
        temp_parameters: dict = dict()
        if parameters is not None:
            temp_parameters.update(parameters)
        if cron is not None:
            schedule_parameter_name = kwargs.get('sl_schedule_parameter_name', 'sl_schedule')
            schedule_parameter_value = sl_schedule(cron=cron, format=kwargs.get('sl_schedule_format', sl_schedule_format))
            temp_parameters[schedule_parameter_name] = schedule_parameter_value
        self._cron = cron
        self._uri = sanitize_id(sink).lower()
        self._queryParameters = asQueryParameters(temp_parameters)
        self._parameters = parameters
        self._url = self.uri + self.queryParameters

    @property
    def sink(self) -> str:
        return self._sink

    @property
    def cron(self) -> Optional[str]:
        return self._cron

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def parameters(self) -> dict:
        return self._parameters

    @property
    def queryParameters(self) -> str:
        return self._queryParameters

    @property
    def url(self) -> str:
        return self._url

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def table(self) -> str:
        return self._table

    def refresh(self) -> StarlakeDataset:
        return StarlakeDataset(self.sink, self.parameters, self.cron)

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
