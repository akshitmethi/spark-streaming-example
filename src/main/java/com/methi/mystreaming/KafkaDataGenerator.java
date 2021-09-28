package com.methi.mystreaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDataGenerator implements Runnable {

    //Kafka topic to publish to
    public static final String topic = "item.topic";

    public static void main(String[] args){
        KafkaDataGenerator dataGenerator = new KafkaDataGenerator();
        dataGenerator.run();
    }

    @Override
    public void run() {
        System.out.println("Starting the data generator for kafka");
        try {
            Thread.sleep(3000);
            //Setup Kafka Client
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "localhost:29092");

            kafkaProps.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String,String> producer = new KafkaProducer<String, String>(kafkaProps);
            //items list
            List<String> names = new ArrayList<>();
            names.add("Soyabean");
            names.add("broclli");
            names.add("bhindi");
            names.add("mushrooms");

            Map<String,Integer> priceMap = new HashMap<>();
            priceMap.put("Soyabean",30);
            priceMap.put("broclli",10);
            priceMap.put("bhindi",5);
            priceMap.put("mushrooms",40);

            Random rand = new Random();

            //creating next 100 objects
            for(int i=0;i<100;i++){

                int quantity = rand.nextInt(10);
                int nameIdx = rand.nextInt(names.size());


                Item item = new Item(i+100,names.get(nameIdx),quantity,priceMap.get(names.get(nameIdx)) * quantity);
                ObjectMapper mapper  = new ObjectMapper();

                String key  = String.valueOf(item.getId());
                String value = mapper.writeValueAsString(item);

                ProducerRecord<String,String> record = new ProducerRecord<>(topic,key,value);

                Future<RecordMetadata> rmd = producer.send(record);
                System.out.println(
                        "Record is sent to kafka:\n"+
                        "key: "+key+
                        "\tvalue: "+value);

                //sleeping the thread for 1-3secs
                Thread.sleep(rand.nextInt(2000)+1000);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }


    }
}
