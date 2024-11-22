package MohSolehuddin.belajar_kafka_java.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {
        // Konfigurasi Kafka Consumer
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Mulai dari offset awal jika tidak ada komit
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "helloword"); // Nama Consumer Group

        // Membuat Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Berlangganan ke topic
        String topic = "helloword"; // Pastikan topic ini ada di broker Kafka
        consumer.subscribe(Collections.singletonList(topic));

        // Membaca pesan dari Kafka
        System.out.println("Menunggu pesan dari topic: " + topic);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // Tunggu pesan masuk
                for (ConsumerRecord<String, String> message : records) {
                    System.out.println("Pesan diterima: " + message.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close(); // Tutup Consumer
        }
    }
}
