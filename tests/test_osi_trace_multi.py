import os
import tempfile
import unittest
import warnings

from osi3trace.osi_trace import OSITrace
from osi3 import __version__ as osi3_version
from osi3.osi_sensorview_pb2 import SensorView
from osi3.osi_sensorviewconfiguration_pb2 import SensorViewConfiguration
from osi3.osi_groundtruth_pb2 import GroundTruth
from osi3.osi_hostvehicledata_pb2 import HostVehicleData
from osi3.osi_sensordata_pb2 import SensorData
from osi3.osi_trafficcommand_pb2 import TrafficCommand
from osi3.osi_trafficcommandupdate_pb2 import TrafficCommandUpdate
from osi3.osi_trafficupdate_pb2 import TrafficUpdate
from osi3.osi_motionrequest_pb2 import MotionRequest
from osi3.osi_streamingupdate_pb2 import StreamingUpdate

from mcap.writer import Writer
from mcap.well_known import MessageEncoding

from google.protobuf import __version__ as google_protobuf_version
from google.protobuf.descriptor import FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorSet


class TestOSITraceMulti(unittest.TestCase):
    def test_osi_trace_sv(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_sv.txth")
            path_input = os.path.join(tmpdirname, "input_sv.mcap")
            create_sample_sv(path_input)

            trace = OSITrace(path_input)
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, SensorView)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_svc(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_svc.txth")
            path_input = os.path.join(tmpdirname, "input_svc.mcap")
            create_sample_svc(path_input)

            trace = OSITrace(path_input, "SensorViewConfiguration")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, SensorViewConfiguration)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_gt(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_gt.txth")
            path_input = os.path.join(tmpdirname, "input_gt.mcap")
            create_sample_gt(path_input)

            trace = OSITrace(path_input, "GroundTruth")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, GroundTruth)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_hvd(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_hvd.txth")
            path_input = os.path.join(tmpdirname, "input_hvd.mcap")
            create_sample_hvd(path_input)

            trace = OSITrace(path_input, "HostVehicleData")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, HostVehicleData)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_sd(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_sd.txth")
            path_input = os.path.join(tmpdirname, "input_sd.mcap")
            create_sample_sd(path_input)

            trace = OSITrace(path_input, "SensorData")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, SensorData)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_tc(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_tc.txth")
            path_input = os.path.join(tmpdirname, "input_tc.mcap")
            create_sample_tc(path_input)

            trace = OSITrace(path_input, "TrafficCommand")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, TrafficCommand)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_tcu(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_tcu.txth")
            path_input = os.path.join(tmpdirname, "input_tcu.mcap")
            create_sample_tcu(path_input)

            trace = OSITrace(path_input, "TrafficCommandUpdate")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, TrafficCommandUpdate)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_tu(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_tu.txth")
            path_input = os.path.join(tmpdirname, "input_tu.mcap")
            create_sample_tu(path_input)

            trace = OSITrace(path_input, "TrafficUpdate")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, TrafficUpdate)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_mr(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_mr.txth")
            path_input = os.path.join(tmpdirname, "input_mr.mcap")
            create_sample_mr(path_input)

            trace = OSITrace(path_input, "MotionRequest")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, MotionRequest)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))

    def test_osi_trace_su(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            path_output = os.path.join(tmpdirname, "output_su.txth")
            path_input = os.path.join(tmpdirname, "input_su.mcap")
            create_sample_su(path_input)

            trace = OSITrace(path_input, "StreamingUpdate")
            with open(path_output, "wt") as f:
                for message in trace:
                    self.assertIsInstance(message, StreamingUpdate)
                    f.write(str(message))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    self.assertEqual(len(trace.retrieve_offsets()), 10)
                except NotImplementedError:
                    pass
            trace.close()

            self.assertTrue(os.path.exists(path_output))


def build_file_descriptor_set(message_class) -> FileDescriptorSet:
    file_descriptor_set = FileDescriptorSet()
    seen_dependencies = set()

    def append_file_descriptor(file_descriptor: FileDescriptor):
        for dep in file_descriptor.dependencies:
            if dep.name not in seen_dependencies:
                seen_dependencies.add(dep.name)
                append_file_descriptor(dep)
        file_descriptor.CopyToProto(file_descriptor_set.file.add())

    append_file_descriptor(message_class.DESCRIPTOR.file)
    return file_descriptor_set


def prepare_mcap_writer(path):
    mcap_writer = Writer(
        output=str(path),
    )

    mcap_writer.start(library=f"osi-python mcap test suite")
    mcap_writer.add_metadata(
        name="net.asam.osi.trace",
        data={
            "version": osi3_version,
            "min_osi_version": osi3_version,
            "max_osi_version": osi3_version,
            "min_protobuf_version": google_protobuf_version,
            "max_protobuf_version": google_protobuf_version,
            "description": "Test trace created by osi-python test suite",
        },
    )
    return mcap_writer


def add_channel(mcap_writer, message_class, topic_name):
    file_descriptor_set = build_file_descriptor_set(message_class)
    schema_id = mcap_writer.register_schema(
        name=f"osi3.{message_class.__name__}",
        encoding=MessageEncoding.Protobuf,
        data=file_descriptor_set.SerializeToString(),
    )
    channel_id = mcap_writer.register_channel(
        topic=topic_name,
        message_encoding=MessageEncoding.Protobuf,
        schema_id=schema_id,
        metadata={
            "net.asam.osi.trace.channel.osi_version": osi3_version,
            "net.asam.osi.trace.channel.protobuf_version": google_protobuf_version,
            "net.asam.osi.trace.channel.description": f"Channel for OSI message type {message_class.__name__}",
        },
    )
    return channel_id


def create_sample_sv(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, SensorView, "SensorViewTopic")

    sensorview = SensorView()

    sensorview.version.version_major = 3
    sensorview.version.version_minor = 0
    sensorview.version.version_patch = 0

    sensorview.timestamp.seconds = 0
    sensorview.timestamp.nanos = 0

    sensorview.sensor_id.value = 42

    sensorview.host_vehicle_id.value = 114

    sv_ground_truth = sensorview.global_ground_truth
    sv_ground_truth.version.version_major = 3
    sv_ground_truth.version.version_minor = 0
    sv_ground_truth.version.version_patch = 0

    sv_ground_truth.timestamp.seconds = 0
    sv_ground_truth.timestamp.nanos = 0

    sv_ground_truth.host_vehicle_id.value = 114

    moving_object = sv_ground_truth.moving_object.add()
    moving_object.id.value = 114

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        sensorview.timestamp.seconds += 1
        sensorview.timestamp.nanos += 100000

        sv_ground_truth.timestamp.seconds += 1
        sv_ground_truth.timestamp.nanos += 100000

        moving_object.vehicle_classification.type = 2

        moving_object.base.dimension.length = 5
        moving_object.base.dimension.width = 2
        moving_object.base.dimension.height = 1

        moving_object.base.position.x = 0.0 + i
        moving_object.base.position.y = 0.0
        moving_object.base.position.z = 0.0

        moving_object.base.orientation.roll = 0.0
        moving_object.base.orientation.pitch = 0.0
        moving_object.base.orientation.yaw = 0.0

        time = sensorview.timestamp.seconds + sensorview.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=sensorview.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_svc(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(
        mcap_writer, SensorViewConfiguration, "SensorViewConfigurationTopic"
    )

    sensorviewconfig = SensorViewConfiguration()

    sensorviewconfig.version.version_major = 3
    sensorviewconfig.version.version_minor = 0
    sensorviewconfig.version.version_patch = 0

    sensorviewconfig.sensor_id.value = 42

    sensorviewconfig.mounting_position.position.x = 0.8
    sensorviewconfig.mounting_position.position.y = 1.0
    sensorviewconfig.mounting_position.position.z = 0.5

    sensorviewconfig.mounting_position.orientation.roll = 0.10
    sensorviewconfig.mounting_position.orientation.pitch = 0.15
    sensorviewconfig.mounting_position.orientation.yaw = 0.25

    time = 0
    mcap_writer.add_message(
        channel_id=channel_id,
        log_time=int(time * 1000000000),
        data=sensorviewconfig.SerializeToString(),
        publish_time=int(time * 1000000000),
    )

    mcap_writer.finish()


def create_sample_gt(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, GroundTruth, "GroundTruthTopic")

    ground_truth = GroundTruth()

    ground_truth.version.version_major = 3
    ground_truth.version.version_minor = 0
    ground_truth.version.version_patch = 0

    ground_truth.timestamp.seconds = 0
    ground_truth.timestamp.nanos = 0

    moving_object = ground_truth.moving_object.add()
    moving_object.id.value = 114

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        ground_truth.timestamp.seconds += 1
        ground_truth.timestamp.nanos += 100000

        moving_object.vehicle_classification.type = 2

        moving_object.base.dimension.length = 5
        moving_object.base.dimension.width = 2
        moving_object.base.dimension.height = 1

        moving_object.base.position.x = 0.0 + i
        moving_object.base.position.y = 0.0
        moving_object.base.position.z = 0.0

        moving_object.base.orientation.roll = 0.0
        moving_object.base.orientation.pitch = 0.0
        moving_object.base.orientation.yaw = 0.0

        time = ground_truth.timestamp.seconds + ground_truth.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=ground_truth.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_hvd(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, HostVehicleData, "HostVehicleDataTopic")

    hostvehicledata = HostVehicleData()

    hostvehicledata.version.version_major = 3
    hostvehicledata.version.version_minor = 0
    hostvehicledata.version.version_patch = 0

    hostvehicledata.timestamp.seconds = 0
    hostvehicledata.timestamp.nanos = 0

    hostvehicledata.host_vehicle_id.value = 114

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        hostvehicledata.timestamp.seconds += 1
        hostvehicledata.timestamp.nanos += 100000

        hostvehicledata.location.dimension.length = 5
        hostvehicledata.location.dimension.width = 2
        hostvehicledata.location.dimension.height = 1

        hostvehicledata.location.position.x = 0.0 + i
        hostvehicledata.location.position.y = 0.0
        hostvehicledata.location.position.z = 0.0

        hostvehicledata.location.orientation.roll = 0.0
        hostvehicledata.location.orientation.pitch = 0.0
        hostvehicledata.location.orientation.yaw = 0.0

        time = hostvehicledata.timestamp.seconds + hostvehicledata.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=hostvehicledata.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_sd(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, SensorData, "SensorDataTopic")

    sensordata = SensorData()

    sensordata.version.version_major = 3
    sensordata.version.version_minor = 0
    sensordata.version.version_patch = 0

    sensordata.timestamp.seconds = 0
    sensordata.timestamp.nanos = 0

    sensordata.sensor_id.value = 42

    moving_object = sensordata.moving_object.add()
    moving_object.header.tracking_id.value = 1
    gt_id = moving_object.header.ground_truth_id.add()
    gt_id.value = 114

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        sensordata.timestamp.seconds += 1
        sensordata.timestamp.nanos += 100000

        moving_object.base.dimension.length = 5
        moving_object.base.dimension.width = 2
        moving_object.base.dimension.height = 1

        moving_object.base.position.x = 0.0 + i
        moving_object.base.position.y = 0.0
        moving_object.base.position.z = 0.0

        moving_object.base.orientation.roll = 0.0
        moving_object.base.orientation.pitch = 0.0
        moving_object.base.orientation.yaw = 0.0

        time = sensordata.timestamp.seconds + sensordata.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=sensordata.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_tc(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, TrafficCommand, "TrafficCommandTopic")

    trafficcommand = TrafficCommand()

    trafficcommand.version.version_major = 3
    trafficcommand.version.version_minor = 0
    trafficcommand.version.version_patch = 0

    trafficcommand.timestamp.seconds = 0
    trafficcommand.timestamp.nanos = 0

    trafficcommand.traffic_participant_id.value = 114

    action = trafficcommand.action.add()

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        trafficcommand.timestamp.seconds += 1
        trafficcommand.timestamp.nanos += 100000

        action.speed_action.action_header.action_id.value = 1000 + i

        action.speed_action.absolute_target_speed = 10.0 + 0.5 * i

        time = trafficcommand.timestamp.seconds + trafficcommand.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=trafficcommand.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_tcu(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(
        mcap_writer, TrafficCommandUpdate, "TrafficCommandUpdateTopic"
    )

    trafficcommandupdate = TrafficCommandUpdate()

    trafficcommandupdate.version.version_major = 3
    trafficcommandupdate.version.version_minor = 0
    trafficcommandupdate.version.version_patch = 0

    trafficcommandupdate.timestamp.seconds = 0
    trafficcommandupdate.timestamp.nanos = 0

    trafficcommandupdate.traffic_participant_id.value = 114

    action = trafficcommandupdate.dismissed_action.add()

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        trafficcommandupdate.timestamp.seconds += 1
        trafficcommandupdate.timestamp.nanos += 100000

        action.dismissed_action_id.value = 1000 + i
        action.failure_reason = "Cannot complete!"

        time = (
            trafficcommandupdate.timestamp.seconds
            + trafficcommandupdate.timestamp.nanos / 1e9
        )
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=trafficcommandupdate.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_tu(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, TrafficUpdate, "TrafficUpdateTopic")

    trafficupdate = TrafficUpdate()

    trafficupdate.version.version_major = 3
    trafficupdate.version.version_minor = 0
    trafficupdate.version.version_patch = 0

    trafficupdate.timestamp.seconds = 0
    trafficupdate.timestamp.nanos = 0

    moving_object = trafficupdate.update.add()
    moving_object.id.value = 114

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        trafficupdate.timestamp.seconds += 1
        trafficupdate.timestamp.nanos += 100000

        moving_object.vehicle_classification.type = 2

        moving_object.base.dimension.length = 5
        moving_object.base.dimension.width = 2
        moving_object.base.dimension.height = 1

        moving_object.base.position.x = 0.0 + i
        moving_object.base.position.y = 0.0
        moving_object.base.position.z = 0.0

        moving_object.base.orientation.roll = 0.0
        moving_object.base.orientation.pitch = 0.0
        moving_object.base.orientation.yaw = 0.0

        time = trafficupdate.timestamp.seconds + trafficupdate.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=trafficupdate.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_mr(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, MotionRequest, "MotionRequestTopic")

    motionrequest = MotionRequest()

    motionrequest.version.version_major = 3
    motionrequest.version.version_minor = 0
    motionrequest.version.version_patch = 0

    motionrequest.timestamp.seconds = 0
    motionrequest.timestamp.nanos = 0

    desired_state = motionrequest.desired_state

    desired_state.timestamp.seconds = 0
    desired_state.timestamp.nanos = 0

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        motionrequest.timestamp.seconds += 1
        motionrequest.timestamp.nanos += 100000

        desired_state.timestamp.seconds += 1
        desired_state.timestamp.nanos += 100000

        desired_state.position.x = 0.0 + i
        desired_state.position.y = 0.0
        desired_state.position.z = 0.0

        desired_state.orientation.roll = 0.0
        desired_state.orientation.pitch = 0.0
        desired_state.orientation.yaw = 0.10

        time = motionrequest.timestamp.seconds + motionrequest.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=motionrequest.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()


def create_sample_su(path):
    mcap_writer = prepare_mcap_writer(path)
    channel_id = add_channel(mcap_writer, StreamingUpdate, "StreamingUpdateTopic")

    streamingupdate = StreamingUpdate()

    streamingupdate.version.version_major = 3
    streamingupdate.version.version_minor = 0
    streamingupdate.version.version_patch = 0

    streamingupdate.timestamp.seconds = 0
    streamingupdate.timestamp.nanos = 0

    moving_object = streamingupdate.moving_object_update.add()
    moving_object.id.value = 114

    # Generate 10 OSI messages for 9 seconds
    for i in range(10):
        # Increment the time
        streamingupdate.timestamp.seconds += 1
        streamingupdate.timestamp.nanos += 100000

        moving_object.vehicle_classification.type = 2

        moving_object.base.dimension.length = 5
        moving_object.base.dimension.width = 2
        moving_object.base.dimension.height = 1

        moving_object.base.position.x = 0.0 + i
        moving_object.base.position.y = 0.0
        moving_object.base.position.z = 0.0

        moving_object.base.orientation.roll = 0.0
        moving_object.base.orientation.pitch = 0.0
        moving_object.base.orientation.yaw = 0.0

        time = streamingupdate.timestamp.seconds + streamingupdate.timestamp.nanos / 1e9
        mcap_writer.add_message(
            channel_id=channel_id,
            log_time=int(time * 1000000000),
            data=streamingupdate.SerializeToString(),
            publish_time=int(time * 1000000000),
        )

    mcap_writer.finish()
