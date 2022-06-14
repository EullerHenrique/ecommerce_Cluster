package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, T> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

        /*

        ACKS_CONFIG:

        O número de confirmações que o produtor exige que o líder tenha recebido antes de considerar uma solicitação
        concluída. Isso controla a durabilidade dos registros enviados. As seguintes configurações são permitidas:
        acks=0 Se definido como zero, o produtor não aguardará nenhuma confirmação do servidor. O registro será
        imediatamente adicionado ao buffer do soquete e considerado enviado. Nenhuma garantia pode ser feito que o
        servidor tenha recebido o registro neste caso, e a configuração de retentas não terá efeito (já que o
        cliente geralmente não saberá de nenhuma falha). O deslocamento retornado para cada registro será sempre ser
        definido como -1. acks=1 Isso significa que o líder gravará o registro em seu log local, mas responderá sem
        aguardar o reconhecimento completo de todos os seguidores. Nesse caso, se o líder falhar imediatamente após
        reconhecer o registro, mas antes que os seguidores o tenham replicado, o registro será perdido.
        acks=all Isso significa que o líder aguardará o conjunto completo de réplicas sincronizadas
        para confirmar o registro. Isso garante que o registro não será perdido enquanto pelo menos uma réplica em
        sincronia permanecer ativa. Esta é a garantia mais forte disponível. Isso é equivalente à configuração acks=-1.
        Observe que habilitar a idempotência requer que este valor de configuração seja 'all'. Se configurações
        conflitantes forem definidas e a idempotência não estiver explicitamente habilitada, a idempotência será
        desabilitada.

        */

        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
