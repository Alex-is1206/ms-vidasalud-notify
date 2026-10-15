package cl.duoc.vidasalud.notify.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Crea la topología en el broker apenas arranca la aplicación.
 *
 * El RabbitAdmin es el componente que lleva al broker lo que está
 * declarado como beans: recorre los exchanges, las colas y los bindings
 * del contexto de Spring y los crea si no existen.
 *
 * Se declara aquí de forma explícita en lugar de depender de la
 * configuración automática, para que quede a la vista quién crea la
 * topología y cuándo.
 *
 * Además hace falta forzar la primera conexión: Spring se conecta a
 * RabbitMQ de forma perezosa, solo cuando un consumidor se suscribe o
 * se publica un mensaje. Sin este empujón, la topología existiría en el
 * contexto de Spring pero todavía no en el broker.
 */
@Configuration
public class ArranqueTopologia {

    private static final Logger log = LoggerFactory.getLogger(ArranqueTopologia.class);

    @Bean
    RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    SmartInitializingSingleton declararTopologia(RabbitAdmin rabbitAdmin) {
        return () -> {
            rabbitAdmin.initialize();
            log.info("Topologia declarada en RabbitMQ: 3 exchanges, 3 colas de trabajo y 3 DLQ");
        };
    }
}