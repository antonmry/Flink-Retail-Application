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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
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

        // create a kafka Consumer
        AvroDeserializationSchema schema = new AvroDeserializationSchema(SensorData.class);

        FlinkKafkaConsumer<SensorData> kafkaconsumer = new FlinkKafkaConsumer<SensorData>("Sensor-Data", schema,props);

        kafkaconsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorData>() {
            @Override
            public long extractAscendingTimestamp(SensorData element) {
                return element.getTimestamp();
            }
        });

        //1st query : Mall Foot Traffic-2 hours, each customer will be identified by it's Mac address

        DataStream<SensorData> mallFootTraffic = env.addSource(kafkaconsumer);

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
        mallFootTraffic.print();

        // 2nd query : In-Mall Proximity Traffic- zone, very important indicator in a retail store, it gives information about under-performing areas

         // create a kafka sink
      FlinkKafkaProducer<Tuple2> kafkaproducer = new FlinkKafkaProducer<Tuple2>("mallFootTraficHistory",
              new SimpleStringSchema()
              , props);

        env.execute();

    }
}

class AvroDeserializationSchema<T> implements DeserializationSchema<T> {

    private final Class<T> avroType;

    private transient DatumReader<T> reader;
    private transient BinaryDecoder decoder;

    public AvroDeserializationSchema(Class<T> avroType) {
        this.avroType = avroType;
    }

    @Override
    public T deserialize(byte[] message) {
        ensureInitialized();

        try {
            decoder = DecoderFactory.get().binaryDecoder(message, decoder);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(avroType);
    }

    private void ensureInitialized() {
        if (reader == null) {
            if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(avroType)) {
                reader = new SpecificDatumReader<T>(avroType);
            } else {
                reader = new ReflectDatumReader<T>(avroType);
            }
        }
    }
}