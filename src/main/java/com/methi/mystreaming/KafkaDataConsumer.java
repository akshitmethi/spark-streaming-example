package com.methi.mystreaming;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class KafkaDataConsumer implements Runnable {

    public static final String topic =  "item.topic";
    public static final int giveup = 1;

    public static void main(String[] args){
        KafkaDataConsumer dataConsumer = new KafkaDataConsumer();
        dataConsumer.run();
    }

    @Override
    public void run() {
        System.out.println("Starting Kafka data consumer");
        try{
            Thread.sleep(3000);
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers","localhost:29092");
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                    "KafkaExampleConsumer");
            Consumer<String,String> consumer = new KafkaConsumer<String, String>(kafkaProps);
            consumer.subscribe(Collections.singletonList(topic));
            while(true){
                //fetching records every 2 secs
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(5000));
                //no break condition
                if(giveup < 0)
                    break;
                Iterator<ConsumerRecord<String,String>> iterator = records.iterator();
                while(iterator.hasNext()){
                    ConsumerRecord<String,String> record = iterator.next();
                    System.out.println("Consumed record\n"+
                            "key: "+record.key() +
                            "\tvalue: "+record.value());
                }
            }
            consumer.close();
            System.out.println("Kafka data consumer closed");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
