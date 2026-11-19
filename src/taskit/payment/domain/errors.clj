(ns taskit.payment.domain.errors
  "Codigos estables para el manejador HTTP.
  La logica pura devuelve estos codigos en mapas; no lanza por un rechazo de negocio.")

(def invalid-payload "INVALID_PAYLOAD")
(def checkout-not-allowed "CHECKOUT_NOT_ALLOWED")
(def checkout-conflict "CHECKOUT_CONFLICT")
(def already-paid "ALREADY_PAID")
(def missing-payee "MISSING_PAYEE")
(def card-data-forbidden "CARD_DATA_FORBIDDEN")
(def invalid-transition "INVALID_TRANSITION")
(def transaction-not-found "TRANSACTION_NOT_FOUND")
(def amount-mismatch "AMOUNT_MISMATCH")
(def currency-mismatch "CURRENCY_MISMATCH")
(def unauthorized-hitl "UNAUTHORIZED_HITL")
(def gateway-conflict "GATEWAY_CONFLICT")
(def gateway-unavailable "GATEWAY_UNAVAILABLE")
(def task-mismatch "TASK_MISMATCH")

(def domain-error-type :taskit.payment/domain-error)

(defn domain-exception
  "Frontera para los adaptadores de entrada. El mapa :code/:details
  es el cuerpo JSON que devolvera el interceptor de Reitit."
  ([code message]
   (domain-exception code message nil))
  ([code message details]
   (ex-info message {:type domain-error-type
                     :code code
                     :details details})))

(defn domain-error?
  [ex]
  (and (instance? Throwable ex)
       (= domain-error-type (:type (ex-data ex)))))
