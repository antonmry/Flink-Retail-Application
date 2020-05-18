package com.SensorData;

import com.SensorData.Functions.Counting;
import com.SensorData.Functions.FootTrafficCollector;
import com.SensorData.Functions.FootTrafficSerializationSchema;
import com.SensorData.Functions.WindowedFootTraffic;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class ReadFromKafka {
    public static void main(String[] args) throws Exception{

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);
        // we set the time characteristic to include an event in a window |event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Define Kafka Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","kafka1:19092,kafka2:29092");
        props.setProperty("zookeeper.connect","localhost:32181");
        props.setProperty("group.id","firstGroup");
        props.setProperty("schema.registry.url","schema-registry:8081");

        // create a kafka Consumer
        FlinkKafkaConsumer011<SensorData> kafkaConsumer = new FlinkKafkaConsumer011<>("SensorData", AvroDeserializationSchema.forSpecific(SensorData.class), props );
        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorData>() {
            @Override
            public long extractAscendingTimestamp(SensorData element) {
                return element.getTimestamp();
            }
        });
        kafkaConsumer.setStartFromEarliest();


        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address
        DataStream<SensorData> mallFootTraffic = env.addSource(kafkaConsumer);
        DataStream<WindowedFootTraffic> outStream = mallFootTraffic
                .keyBy(SensorData::getSensorUID)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(72000)))
                .aggregate(new Counting(), new FootTrafficCollector())
                .name("Visitors Counter");

         // create a kafka sink
        outStream.addSink(new FlinkKafkaProducer011<WindowedFootTraffic>(
                parameterTool.getRequired("mallFootTraficHistory"),
                new FootTrafficSerializationSchema("mallFootTrafficHistory"),
                props))
                .name("FootTraffic Sink");
        env.execute();

        //2nd query : Foot Traffic pattern- zone, very important indicator in a retail store, it gives information about under-performing areas


    }
}
