package MohSolehuddin.belajar_kafka_java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerApp {
    public static void main(String[] args) {
        // Konfigurasi Kafka Producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Membuat Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Mengirim pesan ke topic
        String topic = "helloword"; // Nama topic
        try {
            for (int i = 0; i < 100; i++) {
                Thread.sleep(3000);
                String key = Integer.toString(i);
                String value = "hello" + i;
                ProducerRecord<String, String> message = new ProducerRecord<>(topic, key, value);
                producer.send(message); // Kirim pesan
                System.out.println("Pesan dikirim: Key = " + key + ", Value = " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close(); // Tutup Kafka Producer
        }
    }
}
