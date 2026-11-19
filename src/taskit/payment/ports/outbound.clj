(ns taskit.payment.ports.outbound
  "Puertos de salida. La infraestructura los implementa; el dominio solo los nombra.")

(defprotocol PaymentGatewayPort
  (create-preference [this preference-request]
    "Crea una preferencia Split Payment. Devuelve {:mp-preference-id :init-point}.")
  (fetch-payment [this mp-payment-id]
    "Lee el pago en la pasarela y devuelve un gateway-payment, sin datos de tarjeta.")
  (disburse [this transaction]
    "Libera en Mercado Pago los fondos retenidos de la transaccion.")
  (refund [this transaction]
    "Devuelve el cobro retenido al demandante."))

(defprotocol TransactionRepositoryPort
  (insert-transaction! [this ctx transaction])
  (update-transaction! [this ctx transaction])
  (update-transaction-if-status! [this ctx transaction expected-status]
    "Persiste la transaccion solo si todavia esta en expected-status. true si escribio.")
  (find-transaction-by-id [this ctx transaction-id])
  (find-transaction-by-task-id [this ctx task-id])
  (find-transaction-by-mp-payment-id [this ctx mp-payment-id])
  (list-transactions-by-user [this ctx user-id limit])
  (insert-outbox! [this ctx event])
  (unpublished-events [this])
  (mark-published! [this event-id]))

(defprotocol TransactionManagerPort
  (in-transaction [this f]
    "Abre una transaccion ACID, invoca (f ctx) y confirma o revierte."))

(defprotocol TaskSnapshotPort
  (save-task-snapshot! [this task])
  (find-task-snapshot [this task-id]))

(defprotocol IdempotencyPort
  (acquire! [this idempotency-key ttl-seconds]
    "true si esta entrega es la primera. false si la clave ya existia.")
  (release! [this idempotency-key]
    "Borra la clave para que un reintento pueda procesar el pago."))

(defprotocol PaymentEventPublisherPort
  (publish! [this event]
    "Publica PaymentAuthorizedEvent, FundsReleasedEvent o PaymentFailedEvent.
    Devuelve :sent o :skipped."))

(defprotocol FraudFreezePort
  (freeze-task! [this task-id reason])
  (frozen-task [this task-id])
  (unfreeze-task! [this task-id]))