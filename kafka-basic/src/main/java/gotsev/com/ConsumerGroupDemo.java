package gotsev.com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupDemo.class.getSimpleName());
    public static void main(String[] args) {
        String groupId = "fourth-group";
        String topic = "demo_java";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("detected shutdown");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try {
            consumer.subscribe(Collections.singletonList(topic));

            while (true){

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));

                for (ConsumerRecord<String, String> record : records){
                    log.info("key = {}, value = {}, partition = {}, offset = {}",
                            record.key(),
                            record.value(),
                            record.partition(), record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("wakeup exception!");
        } catch (Exception e){
            log.error("unexpected exception");
        } finally {
            consumer.close();
            log.info("The consumer is gracefully closed");
        }
    }
}
