"""
Common ETL specific utilities and methods
"""
import string
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from random import random
from typing import Any, List

from cassandra.cqlengine import columns, ValidationError
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import LWTException


class EtlStates(Enum):
    """Enumerator for the possible states of an ETL"""
    Ready = 'ready'
    Processing = 'processing'
    Succeeded = 'succeeded'
    Failed = 'failed'


class EtlSinkRecordState(Model):
    """Cassandra ORM model for the Etl Sink States"""
    etl_signature = columns.Text(primary_key=True, required=True)
    record_identifier = columns.Text(primary_key=True, required=True)
    record_state = columns.Text(required=True, default=EtlStates.Ready.value)
    state_created = columns.DateTime(required=True, default=datetime.now())
    state_last_updated = columns.DateTime(required=True, default=datetime.now())


def sync_etl_state_table():
    """
    Utility method to sync (Create) the table as per ORM model
    Returns: None
    """
    sync_table(EtlSinkRecordState)


def _state_change_validation(current: EtlStates, new: EtlStates) -> None:
    if new in (EtlStates.Succeeded, EtlStates.Failed) and current != EtlStates.Processing:
        raise ValidationError(
            f"Current State '{current}' cannot transition to '{new}'")

    if new == EtlStates.Processing and current != EtlStates.Ready:
        raise ValidationError(
            f"Current State '{current}' cannot transition to '{new}'")

    if new == current:
        raise ValidationError(
            f"Current State '{current}' cannot transition to '{new}', "
            f"you might have duplicate records in your data set")

    if current == EtlStates.Succeeded:
        raise ValidationError(
            f"Current State '{current}' does not allow any transitions")


class EtlSinkRecordStateManager:
    """
    The Generic ETL Sink State manager that manages and persists the state of a record
    Args:
        record_identifier (str): A unique Identifier string to identify the sink record
        etl_signature (str): The Unique ETL Signature to identify the ETL
    """

    def __init__(self, record_identifier: str, etl_signature: str):
        self.record_identifier = record_identifier
        self.etl_signature = etl_signature
        self.remote_state = self._get_or_create_state()

    def _get_or_create_state(self):
        try:
            return EtlSinkRecordState.if_not_exists().create(
                etl_signature=self.etl_signature,
                record_identifier=self.record_identifier)
        except LWTException as e:
            return EtlSinkRecordState.get(
                etl_signature=e.existing['etl_signature'],
                record_identifier=e.existing['record_identifier'])

    def _change_state(self, new_state):
        _state_change_validation(self.current_state(), new_state)

        self.remote_state.record_state = new_state.value
        self.remote_state.state_last_updated = datetime.now()
        self.remote_state.save()

    def current_state(self) -> EtlStates:
        """
        Get current state of the sid record
        Returns (EtlStates): current state

        """
        return EtlStates(self.remote_state.record_state)

    def succeeded(self) -> None:
        """
        Mark Record as succeeded
        Returns: None
        """
        self._change_state(EtlStates.Succeeded)

    def failed(self) -> None:
        """
        Mark Record as failed
        Returns: None
        """
        self._change_state(EtlStates.Failed)

    def processing(self) -> None:
        """
        Mark Record as processing
        Returns: None
        """
        self._change_state(EtlStates.Processing)

    def ready(self) -> None:
        """
        Mark Record as ready
        Returns: None
        """
        self._change_state(EtlStates.Ready)


def current_epoch() -> int:
    """
    Get the current epoch to millisecond precision
    Returns: int
    """
    return int(round(time.time() * 1000))


def get_random_string(length: int) -> str:
    """
    A random ascii lowercase string of certain length
    Args:
        length (int): length of the random string
    Returns: str
    """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


@dataclass
class SourceSettings:
    pass


@dataclass
class ExtractorState:
    time: datetime


class CheckpointHandler:

    @abstractmethod
    def save_state(self, state: ExtractorState) -> None:
        pass

    @abstractmethod
    def latest_state(self):
        pass


class Extractor(ABC):
    def __init__(self, settings: SourceSettings):
        self.settings = settings
        pass

    @abstractmethod
    def extract_next(self):
        pass

    @abstractmethod
    def has_next(self) -> bool:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass


class Transformer(ABC):
    @abstractmethod
    def transform(self, data) -> Any:
        pass


@dataclass
class SinkSettings:
    pass


class Loader(ABC):
    def __init__(self, settings: SinkSettings):
        self.settings = settings
        pass

    @abstractmethod
    def load(self, data: Any) -> None:
        pass


class ETL(ABC):
    def __init__(self, extractor: Extractor, transformers: List[Transformer], loader: Loader):
        self.extractor = extractor
        self.transformers = transformers
        self.loader = loader

    def has_next(self) -> bool:
        return self.extractor.has_next()

    def execute_next(self) -> None:
        extracted_data = self.extractor.extract_next()
        transformed_data = extracted_data
        for transformer in self.transformers:
            transformed_data = transformer.transform(transformed_data)
        self.loader.load(transformed_data)

    def execute_all(self) -> None:
        while self.has_next():
            self.execute_next()

    def reset_source(self) -> None:
        self.extractor.reset()
