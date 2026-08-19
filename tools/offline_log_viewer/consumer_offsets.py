import base64
import datetime
from io import BytesIO

from model import *
from reader import Endianness, Reader
from storage import BatchType, Segment


class TxRecordParser:
    def __init__(self, hdr, record) -> None:
        self.r = record
        self.hdr = hdr

    def parse(self):
        key = self.decode_key()
        val = self.decode_value(key["type"])
        val["producer_id"] = self.hdr.producer_id
        val["producer_epoch"] = self.hdr.producer_epoch
        return (key, val)

    def decode_key(self):
        key_rdr = Reader(BytesIO(self.r.key))
        ret = {}
        v = key_rdr.read_int8()
        ret["type"] = self.decode_key_type(v)
        ret["id"] = key_rdr.read_int64()
        return ret

    def decode_fence(self, rdr):
        # Only supports latest fence batch
        ret = {}
        rdr.skip(1)
        ret["group"] = rdr.read_string()
        ret["tx_seq"] = rdr.read_int64()
        ret["tx_timeout"] = rdr.read_int64()
        ret["partition"] = rdr.read_int32()
        return ret

    def decode_commit(self, rdr):
        ret = {}
        rdr.skip(1)
        ret["group"] = rdr.read_string()
        return ret

    def decode_abort(self, rdr):
        ret = {}
        rdr.skip(1)
        ret["group"] = rdr.read_string()
        ret["tx_seq"] = rdr.read_int64()
        return ret

    def decode_value(self, key_type):
        if not self.r.value:
            return "tombstone"
        val_rdr = Reader(BytesIO(self.r.value))
        if key_type == "tx_fence":
            return self.decode_fence(val_rdr)
        elif key_type == "tx_commit":
            return self.decode_commit(val_rdr)
        return self.decode_abort(val_rdr)

    def decode_key_type(self, v):
        if v == 10:
            return "tx_fence"
        elif v == 15:
            return "tx_commit"
        elif v == 16:
            return "tx_abort"
        return "unknown"


def kafka_tag_reader(payload):
    return Reader(BytesIO(payload), endianness=Endianness.BIG_ENDIAN)


def hex_tags(tags):
    return {tag: payload.hex() for tag, payload in tags.items()}


def decode_topic_partitions(rdr):
    ret = {}
    ret["topic_id"] = rdr.read_uuid()
    ret["partitions"] = rdr.read_kafka_flex_array(lambda r: r.read_int32())
    tags = rdr.read_kafka_tags()
    epochs = tags.pop(0, None)
    if epochs is not None:
        ret["assignment_epochs"] = kafka_tag_reader(epochs).read_kafka_flex_array(
            lambda r: r.read_int32()
        )
    if tags:
        ret["unknown_tags"] = hex_tags(tags)
    return ret


def decode_classic_protocol(rdr):
    ret = {}
    ret["name"] = rdr.read_kafka_flex_string()
    ret["metadata"] = base64.b64encode(rdr.read_kafka_flex_bytes()).decode("utf-8")
    tags = rdr.read_kafka_tags()
    if tags:
        ret["unknown_tags"] = hex_tags(tags)
    return ret


def decode_classic_member_metadata(rdr):
    # A leading zero varint marks the struct absent.
    if rdr.read_unsigned_varint() == 0:
        return None
    ret = {}
    ret["session_timeout"] = rdr.read_int32()
    ret["supported_protocols"] = rdr.read_kafka_flex_array(decode_classic_protocol)
    tags = rdr.read_kafka_tags()
    if tags:
        ret["unknown_tags"] = hex_tags(tags)
    return ret


# `consumer_group_*` are the KIP-848 protocol's records, keyed by the version
# their key carries. See kafka::group_metadata_type in
# src/v/kafka/server/group_metadata.h.
CONSUMER_GROUP_KEY_TYPES = {
    3: "consumer_group_metadata",
    5: "consumer_group_member_metadata",
    6: "consumer_group_target_assignment_metadata",
    7: "consumer_group_target_assignment_member",
    8: "consumer_group_current_member_assignment",
}

# The key types that name a member after the group.
MEMBER_KEY_TYPES = {
    "consumer_group_member_metadata",
    "consumer_group_target_assignment_member",
    "consumer_group_current_member_assignment",
}

# kafka::consumer_group_member_state
MEMBER_STATES = {
    0: "stable",
    1: "unrevoked_partitions",
    2: "unreleased_partitions",
    127: "unknown",
}


class NonTxRecordParser:
    def __init__(self, record) -> None:
        self.r = record

    def parse(self):
        key = self.decode_key()
        return (key, self.decode_value(key["type"]))

    def decode_value(self, key_type):
        if not self.r.value:
            return "tombstone"
        decoders = {
            "group_metadata": self.decode_metadata,
            "offset_commit": self.decode_offset_commit,
            "consumer_group_metadata": self.decode_consumer_group_metadata,
            "consumer_group_member_metadata": self.decode_consumer_group_member_metadata,
            "consumer_group_target_assignment_metadata": self.decode_target_assignment_metadata,
            "consumer_group_target_assignment_member": self.decode_target_assignment_member,
            "consumer_group_current_member_assignment": self.decode_current_member_assignment,
        }
        decode = decoders.get(key_type)
        if decode is None:
            # Keep the record in the dump, so one unreadable type does not cost
            # the whole segment.
            return {"raw": self.r.value.hex()}
        return decode(Reader(BytesIO(self.r.value), endianness=Endianness.BIG_ENDIAN))

    def decode_key_type(self, v):
        if v == 0 or v == 1:
            return "offset_commit"
        elif v == 2:
            return "group_metadata"

        return CONSUMER_GROUP_KEY_TYPES.get(v, "unknown")

    def decode_consumer_group_metadata(self, rdr):
        ret = {}
        ret["version"] = rdr.read_int16()
        ret["epoch"] = rdr.read_int32()
        tags = rdr.read_kafka_tags()
        metadata_hash = tags.pop(0, None)
        # Tag 0 is the subscription-metadata hash, which is not written when 0.
        ret["metadata_hash"] = (
            kafka_tag_reader(metadata_hash).read_int64()
            if metadata_hash is not None
            else 0
        )
        if tags:
            ret["unknown_tags"] = hex_tags(tags)
        return ret

    def decode_consumer_group_member_metadata(self, rdr):
        ret = {}
        ret["version"] = rdr.read_int16()
        ret["instance_id"] = rdr.read_kafka_flex_string()
        ret["rack_id"] = rdr.read_kafka_flex_string()
        ret["client_id"] = rdr.read_kafka_flex_string()
        ret["client_host"] = rdr.read_kafka_flex_string()
        ret["subscribed_topic_names"] = rdr.read_kafka_flex_array(
            lambda r: r.read_kafka_flex_string()
        )
        ret["subscribed_topic_regex"] = rdr.read_kafka_flex_string()
        ret["rebalance_timeout"] = rdr.read_int32()
        ret["server_assignor"] = rdr.read_kafka_flex_string()
        tags = rdr.read_kafka_tags()
        classic_metadata = tags.pop(0, None)
        # Tag 0 holds the classic-protocol metadata of a converted member. An
        # omitted tag means the default, which is present and empty.
        if classic_metadata is not None:
            ret["classic_metadata"] = decode_classic_member_metadata(
                kafka_tag_reader(classic_metadata)
            )
        if tags:
            ret["unknown_tags"] = hex_tags(tags)
        return ret

    def decode_target_assignment_metadata(self, rdr):
        ret = {}
        ret["version"] = rdr.read_int16()
        ret["assignment_epoch"] = rdr.read_int32()
        tags = rdr.read_kafka_tags()
        timestamp = tags.pop(0, None)
        # Tag 0 is the assignment timestamp, which is not written when 0.
        ret["assignment_timestamp"] = (
            kafka_tag_reader(timestamp).read_int64() if timestamp is not None else 0
        )
        if tags:
            ret["unknown_tags"] = hex_tags(tags)
        return ret

    def decode_target_assignment_member(self, rdr):
        ret = {}
        ret["version"] = rdr.read_int16()
        ret["topic_partitions"] = rdr.read_kafka_flex_array(decode_topic_partitions)
        tags = rdr.read_kafka_tags()
        if tags:
            ret["unknown_tags"] = hex_tags(tags)
        return ret

    def decode_current_member_assignment(self, rdr):
        ret = {}
        ret["version"] = rdr.read_int16()
        ret["member_epoch"] = rdr.read_int32()
        ret["previous_member_epoch"] = rdr.read_int32()
        state = rdr.read_int8()
        ret["state"] = MEMBER_STATES.get(state, state)
        ret["assigned_partitions"] = rdr.read_kafka_flex_array(decode_topic_partitions)
        ret["partitions_pending_revocation"] = rdr.read_kafka_flex_array(
            decode_topic_partitions
        )
        tags = rdr.read_kafka_tags()
        if tags:
            ret["unknown_tags"] = hex_tags(tags)
        return ret

    def decode_member_proto(self, rdr):
        ret = {}
        ret["name"] = rdr.read_string()
        ret["metadata"] = rdr.read_iobuf().hex()
        return ret

    def decode_member(self, rdr):
        ret = {}
        ret["v"] = rdr.read_int16()
        ret["member_id"] = rdr.read_kafka_string()
        ret["instance_id"] = rdr.read_kafka_optional_string()
        ret["client_id"] = rdr.read_kafka_string()
        ret["client_host"] = rdr.read_kafka_string()
        ret["rebalance_timeout"] = rdr.read_int32()
        ret["session_timeout"] = rdr.read_int32()
        ret["subscription"] = base64.b64encode(rdr.read_kafka_bytes()).decode("utf-8")
        ret["assignment"] = base64.b64encode(rdr.read_kafka_bytes()).decode("utf-8")

        return ret

    def decode_metadata(self, rdr):
        ret = {}
        ret["version"] = rdr.read_int16()
        ret["protocol_type"] = rdr.read_kafka_string()
        ret["generation_id"] = rdr.read_int32()
        ret["protocol_name"] = rdr.read_kafka_optional_string()
        ret["leader"] = rdr.read_kafka_optional_string()
        ret["state_timestamp"] = rdr.read_int64()
        ret["member_state"] = rdr.read_vector(self.decode_member)
        return ret

    def decode_key(self):
        key_rdr = Reader(BytesIO(self.r.key), endianness=Endianness.BIG_ENDIAN)
        ret = {}
        v = key_rdr.read_int16()
        ret["type"] = self.decode_key_type(v)
        if ret["type"] == "unknown":
            # Report the version, so a dump names what it could not read.
            ret["version"] = v
            ret["raw"] = self.r.key.hex()
            return ret
        ret["group_id"] = key_rdr.read_kafka_string()
        if ret["type"] == "offset_commit":
            ret["topic"] = key_rdr.read_kafka_string()
            ret["partition"] = key_rdr.read_int32()
        elif ret["type"] in MEMBER_KEY_TYPES:
            ret["member_id"] = key_rdr.read_kafka_string()

        return ret

    def decode_offset_commit(self, v_rdr):
        ret = {}
        ret["version"] = v_rdr.read_int16()
        ret["committed_offset"] = v_rdr.read_int64()
        if ret["version"] >= 3:
            ret["leader_epoch"] = v_rdr.read_int32()

        ret["committed_metadata"] = v_rdr.read_kafka_string()
        ret["commit_timestamp"] = v_rdr.read_int64()
        if ret["version"] == 1:
            ret["expiry_timestamp"] = v_rdr.read_int64()

        return ret


def is_transactional_type(hdr):
    return hdr.type != 1


def timestamp_to_str(timestamp: int):
    return datetime.datetime.fromtimestamp(
        timestamp / 1000.0, datetime.timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def decode_record(hdr, r):
    v = {}
    v["epoch"] = hdr.first_ts
    v["offset"] = hdr.base_offset + r.offset_delta
    v["ts"] = timestamp_to_str(hdr.first_ts)
    if is_transactional_type(hdr):
        v["key"], v["val"] = TxRecordParser(hdr, r).parse()
    else:
        v["key"], v["val"] = NonTxRecordParser(r).parse()
    return v


# 1 - raft_data - regular offset commits and group metadata
# 10 - tx_fence - tx offset commits
# 15 - group_commit_tx
# 16 - group_abort_tx

GROUP_RECORDS = {1, 10, 15, 16}


class OffsetsLog:
    def __init__(self, ntp, decode_all_batches=False):
        self.ntp = ntp
        self.decode_all = decode_all_batches

    def __iter__(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for b in s:
                if b.header.type not in GROUP_RECORDS:
                    if not self.decode_all:
                        continue
                    yield b.header_dict()
                else:
                    for r in b:
                        yield decode_record(b.header, r)


class ConsumerGroupsSummaryGenerator:
    def __init__(self, ntp):
        self.ntp = ntp
        self.log = OffsetsLog(ntp, decode_all_batches=True)

    def build_summary(self):
        configurations = []
        archival_batches = []
        compaction_placeholders = []
        groups = {}

        def update_group_state(group, state):
            if group in groups:
                groups[group]["state"] = state
            else:
                groups[group] = {
                    "state": state,
                    "offsets": {},
                }

        def update_group_offsets(group, topic_partition, offset_metadata, log_offset):
            if group not in groups:
                groups[group] = {
                    "state": None,
                    "offsets": {},
                }

            offsets = groups[group]["offsets"]
            if offset_metadata == "tombstone":
                offsets.pop(topic_partition, None)
            else:
                offsets[topic_partition] = {
                    "committed_offset": offset_metadata["committed_offset"],
                    "log_offset": log_offset,
                    "leader_epoch": offset_metadata["leader_epoch"],
                    "commit_timestamp": offset_metadata["commit_timestamp"],
                    "commit_timestamp_str": timestamp_to_str(
                        offset_metadata["commit_timestamp"]
                    ),
                }

        for b in self.log:
            if "type" in b:
                if b["type"] == BatchType.raft_configuration.value:
                    configurations.append(
                        {
                            "base_offset": b["base_offset"],
                            "term": b["term"],
                            "timestamp": b["first_ts"],
                            "timestamp_string": timestamp_to_str(b["first_ts"]),
                        }
                    )
                elif b["type"] == BatchType.archival_metadata.value:
                    archival_batches.append(b["base_offset"])
                elif b["type"] == BatchType.compaction_placeholder.value:
                    compaction_placeholders.append(
                        {"base_offset": b["base_offset"], "term": b["term"]}
                    )
            else:
                if b["key"]["type"] == "group_metadata":
                    group = b["key"]["group_id"]
                    if b["val"] == "tombstone":
                        groups.pop(group, None)
                    else:
                        update_group_state(group, b["val"])
                elif b["key"]["type"] == "offset_commit":
                    group = b["key"]["group_id"]
                    update_group_offsets(
                        group,
                        f"{b['key']['topic']}/{b['key']['partition']}",
                        b["val"],
                        b["offset"],
                    )

        return {
            "raft_configurations": configurations,
            "archival_metadata": archival_batches,
            "compaction_placeholders": compaction_placeholders,
            "groups": groups,
        }
