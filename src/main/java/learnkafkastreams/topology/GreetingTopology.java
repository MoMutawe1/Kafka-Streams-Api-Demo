package learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingTopology {

    // kafka Topic 1 - Source
    public static String GREETINGS = "greetings";

    // kafka Topic 2 - Sink
    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    // this is a very simple Kafka Streams app which takes care of consuming the messages from the Kafka
    // topic GREETINGS, and then writes the messages into the Kafka topic GREETINGS_UPPERCASE.
    // Topology is a class which represents the whole processing logic for your Kafka Streams application,
    public static Topology buildTopology(){

        // So Streams Builder is a building block using which you can define the (source processor) and your
        // (stream processing) and then (sink processor) logic All these things can be built using the StreamsBuilder.
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // To the stream you basically pass the Kafka topic that you want to read the messages from.
        KStream<String, String> greetingStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));// key:String value:String

        //There is a handy function named print that's going to show you what are all the messages that are read by this application.
        greetingStream.print(Printed.<String,String>toSysOut().withLabel("greetingStream"));

        // process and update stream value
        KStream<String, String> modifiedStream = greetingStream.mapValues((readOnlyKey, value) -> value.toUpperCase());

        modifiedStream.print(Printed.<String,String>toSysOut().withLabel("modifiedStream"));

        // publish value to another topic.
        // to() function is going to take care of automatically publishing the message into the Kafka topic that you provide as a function argument.
        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));// key:String value:String

        // Returns type Topology that represents the specified processing logic.
        return streamsBuilder.build();
    }
}
