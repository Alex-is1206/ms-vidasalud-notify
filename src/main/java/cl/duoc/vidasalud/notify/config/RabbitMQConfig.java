package cl.duoc.vidasalud.notify.config;

import static cl.duoc.vidasalud.notify.config.RabbitTopologia.*;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración centralizada de RabbitMQ.
 *
 * Toda la topología se declara aquí como beans: al arrancar la
 * aplicación, Spring crea en el broker los exchanges, las colas y los
 * bindings que falten. No hay que crear nada a mano en la consola web,
 * y la topología queda versionada junto al código.
 *
 * Son tres flujos de trabajo, cada uno con su cola y su cola de
 * mensajes fallidos (DLQ):
 *
 *   cmd.topic  --atencion.*----------> vidasalud.notificaciones
 *   cmd.topic  --atencion.confirmada-> vidasalud.tickets
 *   cmd.direct --catalogo.cambio-----> vidasalud.auditoria
 *
 * Un mensaje que el consumidor rechaza no se pierde ni se reintenta
 * para siempre: el broker lo envía a cmd.dead.dlx, que lo deposita en
 * la DLQ de su cola de origen para revisarlo después.
 */
@Configuration
public class RabbitMQConfig {

    // ================= Exchanges =================

    /**
     * Topic: la clave de ruteo admite comodines, así que un mismo evento
     * puede llegar a varias colas a la vez. Al confirmarse una atención,
     * el evento "atencion.confirmada" entra tanto a notificaciones
     * (que escucha "atencion.*") como a tickets.
     */
    @Bean
    TopicExchange exchangeAtenciones() {
        return new TopicExchange(EXCHANGE_TOPIC, true, false);
    }

    /**
     * Direct: la clave de ruteo debe coincidir exactamente. Se usa para
     * el catálogo, donde cada evento tiene un único destino.
     */
    @Bean
    DirectExchange exchangeCatalogo() {
        return new DirectExchange(EXCHANGE_DIRECT, true, false);
    }

    /** Exchange al que el broker envía los mensajes rechazados. */
    @Bean
    DirectExchange exchangeDeadLetter() {
        return new DirectExchange(EXCHANGE_DLX, true, false);
    }

    // ================= Colas de trabajo =================

    /**
     * Cada cola es durable (sobrevive a un reinicio del broker) y apunta
     * al DLX: si el consumidor rechaza un mensaje, el broker lo reenvía
     * ahí con la clave de su propia DLQ.
     */
    private static Queue colaConDlq(String nombre, String dlq) {
        return QueueBuilder.durable(nombre)
                .deadLetterExchange(EXCHANGE_DLX)
                .deadLetterRoutingKey(dlq)
                .build();
    }

    @Bean
    Queue colaNotificaciones() {
        return colaConDlq(COLA_NOTIFICACIONES, DLQ_NOTIFICACIONES);
    }

    @Bean
    Queue colaTickets() {
        return colaConDlq(COLA_TICKETS, DLQ_TICKETS);
    }

    @Bean
    Queue colaAuditoria() {
        return colaConDlq(COLA_AUDITORIA, DLQ_AUDITORIA);
    }

    // ================= Colas de mensajes fallidos =================

    /** Una DLQ no apunta a otro DLX: es el final del camino. */
    @Bean
    Queue dlqNotificaciones() {
        return QueueBuilder.durable(DLQ_NOTIFICACIONES).build();
    }

    @Bean
    Queue dlqTickets() {
        return QueueBuilder.durable(DLQ_TICKETS).build();
    }

    @Bean
    Queue dlqAuditoria() {
        return QueueBuilder.durable(DLQ_AUDITORIA).build();
    }

    // ================= Bindings =================

    @Bean
    Binding bindNotificaciones() {
        return BindingBuilder.bind(colaNotificaciones())
                .to(exchangeAtenciones())
                .with(RK_ATENCION_TODAS);
    }

    @Bean
    Binding bindTickets() {
        return BindingBuilder.bind(colaTickets())
                .to(exchangeAtenciones())
                .with(RK_ATENCION_CONFIRMADA);
    }

    @Bean
    Binding bindAuditoria() {
        return BindingBuilder.bind(colaAuditoria())
                .to(exchangeCatalogo())
                .with(RK_CATALOGO_CAMBIO);
    }

    @Bean
    Binding bindDlqNotificaciones() {
        return BindingBuilder.bind(dlqNotificaciones())
                .to(exchangeDeadLetter())
                .with(DLQ_NOTIFICACIONES);
    }

    @Bean
    Binding bindDlqTickets() {
        return BindingBuilder.bind(dlqTickets())
                .to(exchangeDeadLetter())
                .with(DLQ_TICKETS);
    }

    @Bean
    Binding bindDlqAuditoria() {
        return BindingBuilder.bind(dlqAuditoria())
                .to(exchangeDeadLetter())
                .with(DLQ_AUDITORIA);
    }

    // ================= Serialización =================

    /**
     * Los mensajes viajan como JSON, no como objetos serializados de
     * Java: así cualquier servicio puede leerlos y el contenido se
     * entiende a simple vista en la consola de RabbitMQ.
     */
    @Bean
    MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                  MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        return template;
    }

    // ================= Consumo de mensajes =================

    /**
     * Fabrica los contenedores donde corren los @RabbitListener.
     *
     * La confirmacion es MANUAL: el broker no da por entregado un
     * mensaje hasta que el consumidor lo confirma con basicAck. Si el
     * servicio se cae a mitad del procesamiento, el mensaje sigue en la
     * cola y se vuelve a entregar, en lugar de perderse.
     *
     * Con la confirmacion automatica, en cambio, el mensaje se borra en
     * cuanto sale de la cola, antes de saber si el trabajo salio bien.
     */
    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);

        // Cuantos mensajes recibe cada consumidor antes de confirmar: evita
        // que un consumidor acapare la cola mientras otro esta libre.
        factory.setPrefetchCount(10);

        return factory;
    }
}
