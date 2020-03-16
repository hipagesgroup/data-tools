"""
Common ETL specific utilities and methods
"""
from datetime import datetime
from enum import Enum

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
    record_state = columns.Text(required=True, index=True, default=EtlStates.Ready.value)
    state_created = columns.DateTime(required=True, default=datetime.now())
    state_last_updated = columns.DateTime(required=True, default=datetime.now())


def sync_etl_state_table():
    """
    Utility method to sync (Create) the table as per ORM model
    Returns: None
    """
    sync_table(EtlSinkRecordState)


class EtlSinkRecordStateManager:
    def __init__(self, record_identifier: str, etl_signature: str):
        """
        The Generic ETL Sink State manager that manages and persists the state of a record
        Args:
            record_identifier (str): A unique Identifier string to identify the sink record
            etl_signature (str): The Unique ETL Signature to identify the ETL
        """
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

    def _state_change_validation(self, new_state):
        if (new_state == EtlStates.Succeeded or new_state == EtlStates.Failed) and \
            self.current_state() != EtlStates.Processing:
            raise ValidationError(
                f"Current State '{self.current_state()}' cannot transition to '{new_state}'")

        if new_state == EtlStates.Processing and self.current_state() != EtlStates.Ready:
            raise ValidationError(
                f"Current State '{self.current_state()}' cannot transition to '{new_state}'")

        if self.current_state() == EtlStates.Succeeded:
            raise ValidationError(
                f"Current State '{self.current_state()}' does not allow any transitions")

    def _change_state(self, new_state):
        self._state_change_validation(new_state)

        EtlSinkRecordState.update(
            self.remote_state,
            record_state=new_state.value,
            state_last_updated=datetime.now())

    def current_state(self):
        """
        Get surrent state of the sid record
        Returns (EtlStates): current state

        """
        return EtlStates(self.remote_state['record_state'])

    def succeeded(self):
        """
        Mark Record as succeeded
        Returns: None
        """
        self._change_state(EtlStates.Succeeded)

    def failed(self):
        """
        Mark Record as failed
        Returns: None
        """
        self._change_state(EtlStates.Failed)

    def processing(self):
        """
        Mark Record as processing
        Returns: None
        """
        self._change_state(EtlStates.Processing)

    def ready(self):
        """
        Mark Record as ready
        Returns: None
        """
        self._change_state(EtlStates.Ready)
