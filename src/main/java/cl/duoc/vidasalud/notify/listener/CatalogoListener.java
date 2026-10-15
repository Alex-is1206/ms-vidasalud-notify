package cl.duoc.vidasalud.notify.listener;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.vidasalud.notify.config.RabbitTopologia;
import cl.duoc.vidasalud.notify.evento.EventoInvalidoException;
import cl.duoc.vidasalud.notify.evento.EventoVidaSalud;
import cl.duoc.vidasalud.notify.evento.RegistroIdempotencia;

/**
 * Consumidor del dominio de catálogo.
 *
 * Escucha el exchange de tipo direct, donde la clave de ruteo debe
 * coincidir exactamente: cada cambio del catálogo tiene un único
 * destino, la auditoría.
 */
@Component
public class CatalogoListener {

    private static final Logger log = LoggerFactory.getLogger(CatalogoListener.class);

    private final RegistroIdempotencia registro;

    public CatalogoListener(RegistroIdempotencia registro) {
        this.registro = registro;
    }

    /** Deja constancia de quién cambió qué en el catálogo. */
    @RabbitListener(queues = RabbitTopologia.COLA_AUDITORIA)
    public void auditarCambio(
            EventoVidaSalud evento,
            Channel canal,
            @Header(AmqpHeaders.DELIVERY_TAG) long entrega,
            @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean reentregado)
            throws IOException {

        try {
            evento.validar();

            if (!registro.registrarSiEsNuevo("auditoria-" + evento.eventId())) {
                log.info("auditoria | evento {} ya procesado, se descarta", evento.eventId());
                canal.basicAck(entrega, false);
                return;
            }

            registrarEnBitacora(evento);
            canal.basicAck(entrega, false);

        } catch (EventoInvalidoException e) {
            log.error("auditoria | mensaje descartado a la DLQ: {}", e.getMessage());
            canal.basicNack(entrega, false, false);

        } catch (Exception e) {
            boolean primerIntento = !Boolean.TRUE.equals(reentregado);
            log.warn("auditoria | fallo al procesar {} ({}). {}",
                    evento.eventId(), e.getMessage(),
                    primerIntento ? "Se reintenta." : "Se envia a la DLQ.");
            canal.basicNack(entrega, false, primerIntento);
        }
    }

    /**
     * Las claves leidas aqui son las mismas que escribe PublicadorCatalogo
     * en ms-vidasalud-catalog: entidad, accion, recursoId y usuario.
     */
    private void registrarEnBitacora(EventoVidaSalud evento) {
        log.info("auditoria | {} {} id {} | realizado por {} | {}",
                evento.dato("entidad"),
                evento.dato("accion"),
                evento.dato("recursoId"),
                evento.dato("usuario"),
                evento.timestamp());
    }
}