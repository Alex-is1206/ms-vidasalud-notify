package cl.duoc.vidasalud.notify.evento;

import java.util.Map;

/**
 * Sobre común a todos los mensajes de VidaSalud.
 *
 * Todos los eventos viajan con la misma estructura, sin importar de qué
 * microservicio vengan. Así un consumidor puede registrar, rastrear y
 * descartar duplicados sin conocer el contenido del mensaje.
 *
 * @param type          qué ocurrió, por ejemplo "atencion.confirmada"
 * @param eventId       identificador único del evento; es la clave para no procesarlo dos veces
 * @param timestamp     cuándo ocurrió, en formato ISO-8601
 * @param traceId       identificador del recorrido completo, para seguirlo entre servicios
 * @param correlationId relaciona este evento con el que lo originó
 * @param payload       los datos propios del evento
 */
public record EventoVidaSalud(
        String type,
        String eventId,
        String timestamp,
        String traceId,
        String correlationId,
        Map<String, Object> payload) {

    /**
     * Comprueba que el sobre venga completo.
     *
     * Un mensaje al que le falta el identificador o el contenido no se
     * puede procesar nunca, por muchas veces que se reintente: es un
     * error permanente y su destino es la cola de mensajes fallidos.
     */
    public void validar() {
        if (eventId == null || eventId.isBlank()) {
            throw new EventoInvalidoException("El evento no trae eventId");
        }
        if (type == null || type.isBlank()) {
            throw new EventoInvalidoException("El evento " + eventId + " no trae type");
        }
        if (payload == null || payload.isEmpty()) {
            throw new EventoInvalidoException("El evento " + eventId + " no trae payload");
        }
    }

    /** Lee un dato del payload, o null si no viene. */
    public String dato(String clave) {
        if (payload == null) {
            return null;
        }
        Object valor = payload.get(clave);
        return valor == null ? null : valor.toString();
    }
}
