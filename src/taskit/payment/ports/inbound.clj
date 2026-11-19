(ns taskit.payment.ports.inbound
  "Puertos de entrada. Cada caso de uso es un componente con un solo metodo.
  Los nombres viven en este namespace y por eso no se repiten.")

(defprotocol CreatePaymentCheckoutUseCase
  (create-payment-checkout [this command]
    "Asocia una tarea SEMI_COMPLETED y prepara la preferencia Split en la pasarela."))

(defprotocol ProcessMercadoPagoWebhookUseCase
  (process-mercado-pago-webhook [this notification]
    "Aplica una notificacion de pago. Debe ser idempotente ante entregas duplicadas."))

(defprotocol ReleaseEscrowFundsUseCase
  (release-escrow-funds [this command]
    "Libera el escrow al prestador cuando la tarea queda COMPLETED."))

(defprotocol ProcessHitlDisputeUseCase
  (process-hitl-dispute [this resolution]
    "Ejecuta el reembolso o la liberacion dictados por un administrador HITL."))

(defprotocol GetTransactionHistoryUseCase
  (get-transaction-history [this query]
    "Devuelve el historial del usuario sin datos sensibles de tarjeta."))
