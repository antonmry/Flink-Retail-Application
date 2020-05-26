package com.SensorData;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;


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
        props.setProperty("bootstrap.servers","kafka1:19092");
        props.setProperty("zookeeper.connect","localhost:32181");
        props.setProperty("group.id","firstGroup");
        props.setProperty("schema.registry.url","schema-registry:8081");


        //FlinkKafkaConsumer<SensorData> kafkaconsumer = new FlinkKafkaConsumer<SensorData>("Sensor-Data", schema,props);

        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address

        DataStreamSource<SensorData> mallFootTraffic = env.addSource(new FlinkKafkaConsumer<>("Sensor-Data", AvroDeserializationSchema.forSpecific(SensorData.class), props)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorData>() {
            @Override
            public long extractAscendingTimestamp(SensorData element) {
                return element.getTimestamp();
            }
        }));
        mallFootTraffic.timeWindowAll(Time.hours(2))
                .apply(new AllWindowFunction<SensorData, Tuple2<Date,Long>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<SensorData> iterable, Collector<Tuple2<Date, Long>> collector) throws Exception {
                        long count =0;
                        for(SensorData visitor : iterable)
                            count++;
                        collector.collect(new Tuple2<>(new Date(timeWindow.getEnd()), count));
                    }
                });

        // 2nd query : In-Mall Proximity Traffic- zone, very important indicator in a retail store, it gives information about under-performing areas

         // create a kafka sink
        // mallFootTraffic.addSink(new FlinkKafkaProducer<>("mallFootTraficHistory", new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), props));

        mallFootTraffic.addSink(new FlinkKafkaProducer<>("mallFootTraficHistory", new ObjSerializationSchema("mallFootTraficHistory"),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute();

    }
}

