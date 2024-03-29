package gotsev.com;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.util.Objects.isNull;

public class ProducerCallbacksAndSticky {

    private static final Logger log = LoggerFactory.getLogger(ProducerCallbacksAndSticky.class.getSimpleName());
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i<10; i++){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello java kafka World");
            producer.send(producerRecord, (metadata, exception) -> {
                if (isNull(exception)){
                    log.info("partition = " + metadata.partition());
                }
            });


            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e){
                log.error(e.getMessage());
            }
        }
        producer.flush();
        producer.close();
    }
}
