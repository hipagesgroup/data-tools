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
from typing import Any, List, TypeVar, Generic, Optional, Dict

from cassandra.cqlengine import columns, ValidationError
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import LWTException
from pandas import DataFrame
from hip_data_tools.common import LOG


class EtlStates(Enum):
    """Enumerator for the possible states of an ETL"""
    Ready = 'ready'
    Processing = 'processing'
    Succeeded = 'succeeded'
    Failed = 'failed'


class EtlSinkRecordState(Model):
    """Cassandra ORM model for the Etl Sink States"""
    etl_signature = columns.Text(primary_key=True, partition_key=True, required=True)
    record_identifier = columns.Text(primary_key=True, partition_key=True, required=True)
    record_state = columns.Text(required=True, default=EtlStates.Ready.value)
    state_created = columns.DateTime(required=True, default=datetime.now())
    state_last_updated = columns.DateTime(required=True, default=datetime.now())


def sync_etl_state_table():
    """
    Utility method to sync (Create) the table as per ORM model
    Returns: None
    """
    LOG.debug("Sinking Cassandra Table using model")
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

            LOG.debug("Creating ETL record sink using etl_signature: "
                      "%s, record_identifier: %s",
                      self.etl_signature,
                      self.record_identifier)

            return EtlSinkRecordState.if_not_exists().create(
                etl_signature=self.etl_signature,
                record_identifier=self.record_identifier)

        except LWTException as e:

            LOG.debug("LWTException raised: \n %s", e)

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
    """ Abstract base dataclass for source settings """
    pass


class Extractor(ABC):
    """
    Abstract base class to define the functionalities of an Extractor

    Args:
        settings (SourceSettings): settings used to connect to a source
    """

    def __init__(self, settings: SourceSettings):
        self._settings = settings
        pass

    @abstractmethod
    def extract_next(self) -> Any:
        """
        Extracts a single datapoint

        Returns: Any

        """
        pass

    @abstractmethod
    def has_next(self) -> bool:
        """
        Checks if the extractor has any more data points

        Returns: bool

        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """
        Reset the state of the Extractor, usually reverting the state to its initial position

        Returns: None

        """
        pass


FromDataElementType = TypeVar('FromDataElementType')
ToDataElementType = TypeVar('ToDataElementType')


class Transformer(ABC, Generic[FromDataElementType, ToDataElementType]):
    """Abstract Base class for handling data transformations"""

    @abstractmethod
    def transform(self, data: FromDataElementType) -> ToDataElementType:
        """
        Transform the data element provided

        Args:
            data (Any): A data Element

        Returns: Any

        """
        pass


class DictTransformer(Transformer[Dict, Dict]):
    """ Abstract base class to handle dict to dict transformation """

    @abstractmethod
    def transform(self, data: Dict) -> Dict:
        """
        Transform the data element provided

        Args:
            data (Any): A data Element

        Returns: Any

        """
        pass


class DataFrameTransformer(Transformer[DataFrame, DataFrame]):
    """ Abstract base class to handle DataFrame to DataFrame transformation """

    @abstractmethod
    def transform(self, data: DataFrame) -> DataFrame:
        """
        Transform the data element provided

        Args:
            data (Any): A data Element

        Returns: Any

        """
        pass


@dataclass
class SinkSettings:
    """Dataclass to encapsulate settings to connect to and write to a data sink"""
    pass


class Loader(ABC):
    """
    Abstract Base class for defining Loaders that write data to sinks

    Args:
        settings (SinkSettings): setting to connect to the sink
    """
    def __init__(self, settings: SinkSettings):
        self._settings = settings
        pass

    @abstractmethod
    def load(self, data: Any) -> None:
        """
        Load a given data point onto the sink

        Args:
            data (Any): Any single data point

        Returns: None

        """
        pass


class ETL(ABC):
    """
    Base ETL class that defines the interaction of the three components of an ETL
    Args:
        extractor (Extractor): source data extractor
        transformers (List[Transformer]): series of transformation
        loader (Loader): loader to write data into a sink
    """

    def __init__(self, extractor: Extractor, loader: Loader,
                 transformers: Optional[List[Transformer]] = None):
        self.extractor = extractor
        if not transformers:
            transformers = []
        self.transformers = transformers
        self.loader = loader

    def has_next(self) -> bool:
        """
        Does the Source have any more data elements

        Returns (bool): True if data exists

        """
        return self.extractor.has_next()

    def execute_next(self) -> None:
        """
        Execute the Extraction, transformation and loading of the next data element from the source

        Returns: None

        """
        extracted_data = self.extractor.extract_next()
        transformed_data = extracted_data
        for transformer in self.transformers:
            transformed_data = transformer.transform(transformed_data)
        self.loader.load(transformed_data)

    def execute_all(self) -> None:
        """
        Execute the Extraction, transformation and loading of all data element from the source

        Returns: None

        """
        while self.has_next():
            self.execute_next()

    def reset_source(self) -> None:
        """
        Reset state of the source Loader, this will cause the loader to forget the items that have
        been loaded already and start afresh

        Returns:

        """
        self.extractor.reset()
