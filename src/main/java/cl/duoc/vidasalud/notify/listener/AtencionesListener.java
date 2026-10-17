package cl.duoc.vidasalud.notify.listener;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.vidasalud.notify.evento.EventoInvalidoException;
import cl.duoc.vidasalud.notify.evento.EventoVidaSalud;
import cl.duoc.vidasalud.notify.evento.RegistroIdempotencia;

/**
 * Consumidores del dominio de atenciones.
 *
 * Los dos escuchan el mismo exchange de tipo topic, pero con claves
 * distintas: notificaciones recibe cualquier cambio de estado
 * ("atencion.*") y tickets solo las confirmaciones. Cuando una
 * atencion se confirma, el mismo evento llega a las dos colas.
 *
 * La confirmacion al broker es explicita: el mensaje se descuenta de la
 * cola unicamente cuando el trabajo termino bien. Si el servicio se
 * cae a mitad del procesamiento, el mensaje sigue en la cola y se
 * vuelve a entregar.
 */
@Component
public class AtencionesListener {

    private static final Logger log = LoggerFactory.getLogger(AtencionesListener.class);

    private final RegistroIdempotencia registro;

    public AtencionesListener(RegistroIdempotencia registro) {
        this.registro = registro;
    }

    /** Avisa al paciente cada vez que su atencion cambia de estado. */
    @RabbitListener(queues = "${vidasalud.rabbit.cola-notificaciones}")
    public void notificarAlPaciente(
            EventoVidaSalud evento,
            Channel canal,
            @Header(AmqpHeaders.DELIVERY_TAG) long entrega,
            @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean reentregado)
            throws IOException {

        try {
            evento.validar();

            if (!registro.registrarSiEsNuevo(evento.eventId())) {
                log.info("notificaciones | evento {} ya procesado, se descarta", evento.eventId());
                canal.basicAck(entrega, false);
                return;
            }

            enviarAviso(evento);
            canal.basicAck(entrega, false);

        } catch (EventoInvalidoException e) {
            // Error permanente: reintentarlo daria el mismo resultado.
            log.error("notificaciones | mensaje descartado a la DLQ: {}", e.getMessage());
            canal.basicNack(entrega, false, false);

        } catch (Exception e) {
            // Error pasajero: se reintenta una vez; si vuelve a fallar, a la DLQ.
            boolean primerIntento = !Boolean.TRUE.equals(reentregado);
            log.warn("notificaciones | fallo al procesar {} ({}). {}",
                    evento.eventId(), e.getMessage(),
                    primerIntento ? "Se reintenta." : "Se envia a la DLQ.");
            canal.basicNack(entrega, false, primerIntento);
        }
    }

    /** Genera el ticket de admision para el box cuando la atencion se confirma. */
    @RabbitListener(queues = "${vidasalud.rabbit.cola-tickets}")
    public void generarTicket(
            EventoVidaSalud evento,
            Channel canal,
            @Header(AmqpHeaders.DELIVERY_TAG) long entrega,
            @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean reentregado)
            throws IOException {

        try {
            evento.validar();

            if (!registro.registrarSiEsNuevo("ticket-" + evento.eventId())) {
                log.info("tickets | evento {} ya procesado, se descarta", evento.eventId());
                canal.basicAck(entrega, false);
                return;
            }

            emitirTicket(evento);
            canal.basicAck(entrega, false);

        } catch (EventoInvalidoException e) {
            log.error("tickets | mensaje descartado a la DLQ: {}", e.getMessage());
            canal.basicNack(entrega, false, false);

        } catch (Exception e) {
            boolean primerIntento = !Boolean.TRUE.equals(reentregado);
            log.warn("tickets | fallo al procesar {} ({}). {}",
                    evento.eventId(), e.getMessage(),
                    primerIntento ? "Se reintenta." : "Se envia a la DLQ.");
            canal.basicNack(entrega, false, primerIntento);
        }
    }

    /**
     * En una version completa aqui se enviaria el correo o la notificacion
     * push. Para esta entrega se deja registrado en el log, que es lo que
     * permite demostrar el recorrido del mensaje.
     */
    private void enviarAviso(EventoVidaSalud evento) {
        log.info("notificaciones | aviso enviado a {} | atencion {} -> {} | trace {}",
                evento.dato("pacienteEmail"),
                evento.dato("atencionId"),
                evento.dato("estado"),
                evento.traceId());
    }

    private void emitirTicket(EventoVidaSalud evento) {
        log.info("tickets | ticket de admision generado | atencion {} | box {} | paciente {}",
                evento.dato("atencionId"),
                evento.dato("boxId"),
                evento.dato("pacienteEmail"));
    }
}