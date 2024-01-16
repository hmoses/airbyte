#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import functools
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.stream.cursor import DefaultFileBasedCursor
from airbyte_cdk.sources.message import MessageRepository
from airbyte_cdk.sources.streams.concurrent.cursor import ConcurrentCursor, CursorField
from airbyte_cdk.sources.streams.concurrent.partitions.record import Record
from airbyte_cdk.sources.streams.concurrent.state_converters.abstract_stream_state_converter import AbstractStreamStateConverter

if TYPE_CHECKING:
    from airbyte_cdk.sources.file_based.stream.concurrent.adapters import FileBasedStreamPartition


class FileBasedConcurrentCursor(ConcurrentCursor):

    def __init__(
        self,
        stream_name: str,
        stream_namespace: Optional[str],
        stream_state: Any,
        message_repository: MessageRepository,
        connector_state_manager: ConnectorStateManager,
        connector_state_converter: AbstractStreamStateConverter,
        cursor_field: CursorField,
        slice_boundary_fields: Optional[Tuple[str, str]],
        cursor: DefaultFileBasedCursor
    ) -> None:
        self._stream_name = stream_name
        self._stream_namespace = stream_namespace
        self._message_repository = message_repository
        self._connector_state_converter = connector_state_converter
        self._connector_state_manager = connector_state_manager
        self._cursor_field = cursor_field
        # To see some example where the slice boundaries might not be defined, check https://github.com/airbytehq/airbyte/blob/1ce84d6396e446e1ac2377362446e3fb94509461/airbyte-integrations/connectors/source-stripe/source_stripe/streams.py#L363-L379
        self._slice_boundary_fields = slice_boundary_fields if slice_boundary_fields else tuple()
        self._cursor = cursor
        self._most_recent_record: Optional[Record] = None
        self._min_pending = None
        self.state = stream_state

    def observe(self, record: Record) -> None:
        pass

    def close_partition(self, partition: "FileBasedStreamPartition") -> None:
        if self._cursor._pending_files is None:
            raise RuntimeError(f"The cursor's `_pending_files` value should be set before processing partitions, but was not. This is unexpected. Please contact support.")

        self._cursor.close_pending_file(self._get_file_from_partition(partition))
        self.state = self._cursor.get_state()
        self._emit_state_message()




"""


{
    "type": "STATE",
    "state": {
        "type": "STREAM",
        "stream": {
            "stream_descriptor": {"name": "test"},
            "stream_state": {
                "_ab_source_file_last_modified": {
                    "history": {"simple_test.csv": "2021-07-25T15:33:04.000000Z"},
                    "_ab_source_file_last_modified": "2021-07-25T15:33:04.000000Z_simple_test.csv"
                }
            }
        },
        "data": {
            "test": {
                "_ab_source_file_last_modified": {
                    "history": {"simple_test.csv": "2021-07-25T15:33:04.000000Z"},
                    "_ab_source_file_last_modified": "2021-07-25T15:33:04.000000Z_simple_test.csv"
                }
            }
        }
    }
}



{
    "type": "STATE",
    "state": {
        "type": "STREAM",
        "stream": {
            "stream_descriptor": {"name": "test"},
            "stream_state": {
                "_ab_source_file_last_modified": "2021-07-25T15:33:04.000000Z_simple_test.csv",
                "history": {"simple_test.csv": "2021-07-25T15:33:04.000000Z"}
            }
        },
        "data": {
            "test": {
                "_ab_source_file_last_modified": "2021-07-25T15:33:04.000000Z_simple_test.csv",
                "history": {"simple_test.csv": "2021-07-25T15:33:04.000000Z"}
            }
        }
    }
}




"""

    def _emit_state_message(self) -> None:
        self._connector_state_manager.update_state_for_stream(
            self._stream_name,
            self._stream_namespace,
            self.state,
        )
        state_message = self._connector_state_manager.create_state_message(
            self._stream_name, self._stream_namespace, send_per_stream_state=True
        )
        self._message_repository.emit_message(state_message)

    def set_pending_partitions(self, pending_partitions: List["FileBasedStreamPartition"]):
        """
        Set the slices waiting to be processed.
        """
        self._cursor.set_pending_files([self._get_file_from_partition(p) for p in pending_partitions])

    def _get_file_from_partition(self, partition: "FileBasedStreamPartition") -> RemoteFile:
        """
        Return the partition key, which consists of the last-modified time and the filename,
        formatted as `iso-timestamp_filename`.
        """
        return list(partition.to_slice().values())[0]
