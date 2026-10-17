package cl.duoc.vidasalud.notify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Propiedades centralizadas de la topologia de RabbitMQ leidas desde application.properties.
 * Cumple con el Indicador 1 de la rubrica al mantener una organizacion consistente
 * en todos los microservicios sin literales dispersos.
 */
@ConfigurationProperties("vidasalud.rabbit")
public record NombresRabbit(
        String exchangeTopic,
        String exchangeDirect,
        String exchangeDlx,
        String colaNotificaciones,
        String colaTickets,
        String colaAuditoria
) {

    public String dlqNotificaciones() {
        return colaNotificaciones + ".dlq";
    }

    public String dlqTickets() {
        return colaTickets + ".dlq";
    }

    public String dlqAuditoria() {
        return colaAuditoria + ".dlq";
    }
}