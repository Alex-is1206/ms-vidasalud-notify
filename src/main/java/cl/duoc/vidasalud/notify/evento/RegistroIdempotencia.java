package cl.duoc.vidasalud.notify.evento;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * Recuerda qué eventos ya se procesaron, para no repetirlos.
 *
 * RabbitMQ garantiza que un mensaje se entrega al menos una vez, no
 * exactamente una vez: si el consumidor confirma y la conexión se corta
 * justo antes de que el broker reciba esa confirmación, el mensaje se
 * vuelve a entregar. Sin este control, un paciente recibiría dos veces
 * el mismo aviso.
 *
 * Guarda en memoria los últimos identificadores vistos. Para este
 * alcance es suficiente; en un sistema en producción el registro
 * viviría en una base de datos o en una caché compartida, para que
 * resista reinicios y funcione con varias instancias del servicio.
 */
@Component
public class RegistroIdempotencia {

    private static final int MAXIMO_RECORDADOS = 1000;

    /** Caché acotada: al superar el máximo, descarta el identificador más antiguo. */
    private final Map<String, Boolean> procesados =
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> masAntiguo) {
                    return size() > MAXIMO_RECORDADOS;
                }
            };

    /**
     * Registra el evento y responde si era nuevo.
     *
     * @return true la primera vez que se ve el identificador; false si ya se había procesado
     */
    public synchronized boolean registrarSiEsNuevo(String eventId) {
        return procesados.putIfAbsent(eventId, Boolean.TRUE) == null;
    }
}
