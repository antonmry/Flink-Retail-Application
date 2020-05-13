package com.SensorData;

import com.SensorData.Functions.Counting;
import com.SensorData.Functions.WindowedFootTraffic;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ReadFromKafka {
    public static void main(String[] args) throws Exception{

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Define Kafka Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","kafka1:19092,kafka2:29092");
        props.setProperty("zookeeper.connect","localhost:32181");
        props.setProperty("group.id","firstGroup");
        props.setProperty("schema.registry.url","schema-registry:8081");

        // create a kafka Consumer
        DataStream<SensorData> incomingStream = env
                .addSource( new FlinkKafkaConsumer011<>("test", AvroDeserializationSchema.forSpecific(SensorData.class), props))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorData>(){
                    @Override
                    public long extractAscendingTimestamp(SensorData element){
                        return element.getTimestamp();
                    }
                });
        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address
        DataStream<WindowedFootTraffic> mallFootTraffic = incomingStream
                .window(TumblingEventTimeWindows.of(7200, TimeUnit.SECONDS))
                .aggregate(new Counting(), new WindowedFootTraffic())
                .name("Visitors Counter");

        // Create a kafka sink to push Flink result to the output topics
        mallFootTraffic.addSink(new FlinkKafkaProducer011<>(
                "mallFootTrafic-History",
                new AvroRowSerializationSchema(props),
                props,
                FlinkKafkaProducer011.Semantic.AT_LEAST_ONCE))
                .name("FootTraffic Sink");

        //2nd query : Foot Traffic pattern- zone, very important indicator for each retail store, it gives information about under-performing areas




        env.execute();

    }
}
