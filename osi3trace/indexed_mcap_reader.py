from typing import Optional, Iterable, Iterator, Tuple, List, Any
import io

from mcap.reader import SeekingReader, NonSeekingReader, breakup_chunk, _chunks_matching_topics, DecodedMessageTuple

from mcap._message_queue import MessageQueue
from mcap.data_stream import ReadDataStream
from mcap.exceptions import DecoderNotFoundError
from mcap.records import (
    Channel,
    Chunk,
    ChunkIndex,
    Message,
    Schema,
)


class IndexedSeekingReader(SeekingReader):
    """
    Extends SeekingReader with global message indexing.

    - Every message gets a global integer index (0..N-1).
    - Builds a table mapping global index -> (chunk_offset, index_in_chunk).
    - Provides .get(global_index) to retrieve a message by index.
    """

    def __init__(
        self,
        stream,
        validate_crcs: bool = False,
        decoder_factories=(),
        record_size_limit: Optional[int] = 4 * 2**30,
    ):
        super().__init__(
            stream,
            validate_crcs=validate_crcs,
            decoder_factories=decoder_factories,
            record_size_limit=record_size_limit,
        )
        self._index_map: List[Tuple[Optional[int], int]] = []  # (chunk_offset, idx_in_chunk)

    def iter_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message, int]]:
        """
        Same as SeekingReader.iter_messages, but yields an extra global index:
            (schema, channel, message, global_index)
        """
        self._index_map.clear()
        global_idx = 0

        summary = self.get_summary()
        if summary is None or len(summary.chunk_indexes) == 0:
            # No chunk indices: fall back to NonSeekingReader
            self._stream.seek(0, io.SEEK_SET)
            for schema, channel, msg in NonSeekingReader(self._stream).iter_messages(
                topics, start_time, end_time, log_time_order
            ):
                self._index_map.append((None, global_idx))
                yield schema, channel, msg, global_idx
                global_idx += 1
            return

        message_queue = MessageQueue(log_time_order=log_time_order, reverse=reverse)
        for chunk_index in _chunks_matching_topics(
            summary, topics, start_time, end_time
        ):
            message_queue.push(chunk_index)

        while message_queue:
            next_item = message_queue.pop()
            if isinstance(next_item, ChunkIndex):
                self._stream.seek(next_item.chunk_start_offset + 1 + 8, io.SEEK_SET)
                chunk = Chunk.read(ReadDataStream(self._stream))
                for index_in_chunk, record in enumerate(
                    breakup_chunk(chunk, validate_crc=self._validate_crcs)
                ):
                    if not isinstance(record, Message):
                        continue
                    channel = summary.channels[record.channel_id]
                    if topics is not None and channel.topic not in topics:
                        continue
                    if start_time is not None and record.log_time < start_time:
                        continue
                    if end_time is not None and record.log_time >= end_time:
                        continue
                    schema = (
                        None
                        if channel.schema_id == 0
                        else summary.schemas[channel.schema_id]
                    )
                    message_queue.push(
                        (
                            (schema, channel, record),
                            next_item.chunk_start_offset,
                            index_in_chunk,
                        )
                    )
            else:
                (schema, channel, record), chunk_offset, idx_in_chunk = next_item
                self._index_map.append((chunk_offset, idx_in_chunk))
                print(self._index_map)
                yield schema, channel, record, global_idx
                global_idx += 1

    def get(self, global_index: int) -> Tuple[Optional[Schema], Channel, DecodedMessageTuple, Message]:
        """
        Retrieve a message by its global index.
        Returns the same tuple shape as iter_decoded_messages().
        """

        if global_index >= len(self._index_map):
            for _, _, _, idx in self.iter_messages():
                if idx >= global_index:
                    break

        if global_index < 0 or global_index >= len(self._index_map):
            raise IndexError(global_index)

        chunk_offset, idx_in_chunk = self._index_map[global_index]

        if chunk_offset is None:
            # Non-seeking path: re-run a linear iteration
            for i, (schema, channel, decoded, raw) in enumerate(super().iter_decoded_messages()):
                if i == global_index:
                    return schema, channel, decoded, raw
            raise IndexError(global_index)

        # Seeking path
        self._stream.seek(chunk_offset + 1 + 8, io.SEEK_SET)
        chunk = Chunk.read(ReadDataStream(self._stream))
        for j, record in enumerate(
            breakup_chunk(chunk, validate_crc=self._validate_crcs)
        ):
            if not isinstance(record, Message):
                continue
            if j == idx_in_chunk:
                summary = self.get_summary()
                channel = summary.channels[record.channel_id]
                schema = (
                    None if channel.schema_id == 0 else summary.schemas[channel.schema_id]
                )
                def decoded_message(
                    schema: Optional[Schema], channel: Channel, message: Message
                ) -> Any:
                    decoder = self._decoders.get(message.channel_id)
                    if decoder is not None:
                        return decoder(message.data)
                    for factory in self._decoder_factories:
                        decoder = factory.decoder_for(channel.message_encoding, schema)
                        if decoder is not None:
                            self._decoders[message.channel_id] = decoder
                            return decoder(message.data)

                    raise DecoderNotFoundError(
                        f"no decoder factory supplied for message encoding {channel.message_encoding}, "
                        f"schema {schema}"
                    )
                decoded = decoded_message(schema, channel, record)
                return DecodedMessageTuple(schema, channel, record, decoded)

        raise IndexError(global_index)
