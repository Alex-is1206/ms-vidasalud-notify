package cl.duoc.vidasalud.notify.config;

import static cl.duoc.vidasalud.notify.config.RabbitTopologia.RK_ATENCION_CONFIRMADA;
import static cl.duoc.vidasalud.notify.config.RabbitTopologia.RK_ATENCION_TODAS;
import static cl.duoc.vidasalud.notify.config.RabbitTopologia.RK_CATALOGO_CAMBIO;

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
 * Configuracion centralizada de RabbitMQ.
 *
 * Toda la topologia se declara aqui como beans leyendo los nombres desde
 * NombresRabbit (application.properties): al arrancar la aplicacion, Spring
 * crea en el broker los exchanges, las colas y los bindings que falten.
 *
 * Son tres flujos de trabajo, cada uno con su cola y su cola de
 * mensajes fallidos (DLQ):
 *
 *   cmd.topic  --atencion.*----------> vidasalud.notificaciones
 *   cmd.topic  --atencion.confirmada-> vidasalud.tickets
 *   cmd.direct --catalogo.cambio-----> vidasalud.auditoria
 */
@Configuration
public class RabbitMQConfig {

    private final NombresRabbit nombres;

    public RabbitMQConfig(NombresRabbit nombres) {
        this.nombres = nombres;
    }

    // ================= Exchanges =================

    /**
     * Topic: la clave de ruteo admite comodines, asi que un mismo evento
     * puede llegar a varias colas a la vez. Al confirmarse una atencion,
     * el evento "atencion.confirmada" entra tanto a notificaciones
     * (que escucha "atencion.*") como a tickets.
     */
    @Bean
    TopicExchange exchangeAtenciones() {
        return new TopicExchange(nombres.exchangeTopic(), true, false);
    }

    /**
     * Direct: la clave de ruteo debe coincidir exactamente. Se usa para
     * el catalogo, donde cada evento tiene un unico destino.
     */
    @Bean
    DirectExchange exchangeCatalogo() {
        return new DirectExchange(nombres.exchangeDirect(), true, false);
    }

    /** Exchange al que el broker envia los mensajes rechazados. */
    @Bean
    DirectExchange exchangeDeadLetter() {
        return new DirectExchange(nombres.exchangeDlx(), true, false);
    }

    // ================= Colas de trabajo =================

    /**
     * Cada cola es durable (sobrevive a un reinicio del broker) y apunta
     * al DLX: si el consumidor rechaza un mensaje, el broker lo reenvia
     * ahi con la clave de su propia DLQ.
     */
    private Queue colaConDlq(String nombre, String dlq) {
        return QueueBuilder.durable(nombre)
                .deadLetterExchange(nombres.exchangeDlx())
                .deadLetterRoutingKey(dlq)
                .build();
    }

    @Bean
    Queue colaNotificaciones() {
        return colaConDlq(nombres.colaNotificaciones(), nombres.dlqNotificaciones());
    }

    @Bean
    Queue colaTickets() {
        return colaConDlq(nombres.colaTickets(), nombres.dlqTickets());
    }

    @Bean
    Queue colaAuditoria() {
        return colaConDlq(nombres.colaAuditoria(), nombres.dlqAuditoria());
    }

    // ================= Colas de mensajes fallidos =================

    /** Una DLQ no apunta a otro DLX: es el final del camino. */
    @Bean
    Queue dlqNotificaciones() {
        return QueueBuilder.durable(nombres.dlqNotificaciones()).build();
    }

    @Bean
    Queue dlqTickets() {
        return QueueBuilder.durable(nombres.dlqTickets()).build();
    }

    @Bean
    Queue dlqAuditoria() {
        return QueueBuilder.durable(nombres.dlqAuditoria()).build();
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
                .with(nombres.dlqNotificaciones());
    }

    @Bean
    Binding bindDlqTickets() {
        return BindingBuilder.bind(dlqTickets())
                .to(exchangeDeadLetter())
                .with(nombres.dlqTickets());
    }

    @Bean
    Binding bindDlqAuditoria() {
        return BindingBuilder.bind(dlqAuditoria())
                .to(exchangeDeadLetter())
                .with(nombres.dlqAuditoria());
    }

    // ================= Serializacion =================

    /**
     * Los mensajes viajan como JSON, no como objetos serializados de
     * Java: asi cualquier servicio puede leerlos y el contenido se
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