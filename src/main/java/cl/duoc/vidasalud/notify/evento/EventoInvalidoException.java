package cl.duoc.vidasalud.notify.evento;

/**
 * El mensaje llegó mal formado.
 *
 * Es un error permanente: reintentarlo daría exactamente el mismo
 * resultado, así que el consumidor lo rechaza sin reencolar y el
 * broker lo deposita en la cola de mensajes fallidos.
 */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
