package cl.duoc.vidasalud.notify.config;

/**
 * Nombres de la topología de mensajería de VidaSalud.
 *
 * Están centralizados en un solo lugar para que productores y
 * consumidores usen exactamente los mismos valores: un nombre mal
 * escrito en un binding no produce ningún error, solo mensajes que
 * nunca llegan a su cola.
 */
public final class RabbitTopologia {

    private RabbitTopologia() {
    }

    // ---------- Exchanges ----------

    /** Eventos de atenciones. Topic: una misma publicación puede llegar a varias colas. */
    public static final String EXCHANGE_TOPIC = "cmd.topic";

    /** Eventos del catálogo. Direct: la clave de ruteo debe coincidir exactamente. */
    public static final String EXCHANGE_DIRECT = "cmd.direct";

    /** Dead Letter Exchange: recibe los mensajes que sus colas rechazaron. */
    public static final String EXCHANGE_DLX = "cmd.dead.dlx";

    // ---------- Claves de ruteo ----------

    /** Cualquier evento de atención: solicitada, confirmada, cerrada, cancelada. */
    public static final String RK_ATENCION_TODAS = "atencion.*";

    /** Una atención que acaba de ser confirmada. */
    public static final String RK_ATENCION_CONFIRMADA = "atencion.confirmada";

    /** Un cambio en el catálogo: prestación creada o modificada. */
    public static final String RK_CATALOGO_CAMBIO = "catalogo.cambio";

    // ---------- Colas de trabajo ----------

    /** Avisa al paciente cada vez que su atención cambia de estado. */
    public static final String COLA_NOTIFICACIONES = "vidasalud.notificaciones";

    /** Genera el ticket de admisión para el box clínico. */
    public static final String COLA_TICKETS = "vidasalud.tickets";

    /** Registra los cambios del catálogo para auditoría. */
    public static final String COLA_AUDITORIA = "vidasalud.auditoria";

    // ---------- Colas de mensajes fallidos ----------

    public static final String DLQ_NOTIFICACIONES = COLA_NOTIFICACIONES + ".dlq";
    public static final String DLQ_TICKETS = COLA_TICKETS + ".dlq";
    public static final String DLQ_AUDITORIA = COLA_AUDITORIA + ".dlq";
}