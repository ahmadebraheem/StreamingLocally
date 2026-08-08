import json
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.output_tag import OutputTag

DLQ_TAG = OutputTag("dlq", Types.STRING())


class ValidateAndSplit(ProcessFunction):
    def process_element(self, value, ctx):
        try:
            obj = json.loads(value)
            reading = float(obj["value"])
            yield json.dumps({"event_id": obj["event_id"], "value": reading})
        except Exception as e:
            yield DLQ_TAG, json.dumps({"raw": value, "error": f"{type(e).__name__}: {e}"})

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    ckpt = env.get_checkpoint_config()
    ckpt.set_min_pause_between_checkpoints(5000)
    ckpt.set_checkpoint_timeout(60000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("test-topic")
        .set_group_id("flink-debug-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    main_stream = raw_stream.process(ValidateAndSplit(), output_type=Types.STRING())
    dlq_stream = main_stream.get_side_output(DLQ_TAG)

    def make_sink(topic):
        return (
            KafkaSink.builder()
            .set_bootstrap_servers("kafka:9092")
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

    main_stream.sink_to(make_sink("processed"))
    dlq_stream.sink_to(make_sink("raw-events-dlq"))

    env.execute("kafka-validate-dlq-pipeline")


if __name__ == "__main__":
    main()