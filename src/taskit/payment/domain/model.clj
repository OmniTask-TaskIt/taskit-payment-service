(ns taskit.payment.domain.model
  "Esquemas Malli del dominio de pagos.

  Los mapas son cerrados en los contratos que controla Task It.
  No hay campos de tarjeta, CVV, PAN ni saldo de billetera: la
  tokenizacion y la custodia del dinero viven en Mercado Pago."
  (:require [malli.core :as m]
            [malli.error :as me]))

(def entity-id
  [:and {:title "UUID"}
   :string
   [:re #"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"]])

(def instant-string
  [:and {:title "ISO-8601 UTC"}
   :string
   [:re #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$"]])

(def task-status
  [:enum "ACTIVE" "COMPLETED" "SEMI_COMPLETED" "CANCELLED"
   "ASSIGNED" "REPORTED" "DISPUTED"])

(def transaction-status
  [:enum "PENDING" "RETAINED" "RELEASED" "REFUNDED" "REJECTED"])

(def currency
  [:enum "COP" "ARS" "BRL" "MXN" "CLP" "PEN" "UYU"])

(def amount
  [:int {:min 1}])

(def fee
  [:int {:min 0}])

(def non-blank
  [:string {:min 1 :max 200}])

(def task
  [:map {:closed true}
   [:task-id entity-id]
   [:task-name non-blank]
   [:task-status task-status]
   [:task-owner entity-id]
   [:task-assignee [:maybe entity-id]]
   [:category [:string {:min 1 :max 80}]]
   [:price amount]
   [:currency currency]])

(def transaction
  [:map {:closed true}
   [:transaction-id entity-id]
   [:task-id entity-id]
   [:payer-id entity-id]
   [:payee-id entity-id]
   [:amount amount]
   [:currency currency]
   [:marketplace-fee fee]
   [:status transaction-status]
   [:mp-preference-id [:maybe :string]]
   [:mp-payment-id [:maybe :string]]
   [:failure-reason [:maybe :string]]
   [:updated-at instant-string]])

(def create-checkout-command
  [:and
   [:map {:closed true}
    [:task-id entity-id]
    [:payer-id entity-id]
    [:payee-id entity-id]
    [:payee-collector-id [:string {:min 1 :max 64}]]
    [:amount amount]
    [:currency currency]
    [:marketplace-fee fee]]
   [:fn {:error/message "marketplace-fee no puede superar amount"}
    (fn [{:keys [amount marketplace-fee]}]
      (and (int? amount)
           (int? marketplace-fee)
           (<= marketplace-fee amount)))]])

(def gateway-preference-request
  [:map {:closed true}
   [:external-reference entity-id]
   [:title non-blank]
   [:amount amount]
   [:currency currency]
   [:marketplace-fee fee]
   [:collector-id [:string {:min 1 :max 64}]]])

(def gateway-payment
  [:map {:closed true}
   [:mp-payment-id [:string {:min 1 :max 64}]]
   [:status [:string {:min 1 :max 40}]]
   [:status-detail [:maybe [:string {:max 120}]]]
   [:transaction-amount amount]
   [:currency currency]])

(def release-escrow-command
  [:map {:closed true}
   [:task-id entity-id]])

(def dispute-outcome
  [:enum "REFUND_PAYER" "RELEASE_PAYEE"])

(def dispute-resolved-event
  [:map {:closed true}
   [:event-id entity-id]
   [:occurred-at instant-string]
   [:task-id entity-id]
   [:transaction-id entity-id]
   [:outcome dispute-outcome]
   [:actor-id entity-id]
   [:actor-role [:string {:min 1 :max 40}]]
   [:reason [:string {:min 1 :max 500}]]])

(def task-status-changed-event
  [:map {:closed true}
   [:event-id entity-id]
   [:occurred-at instant-string]
   [:task-id entity-id]
   [:task-name non-blank]
   [:previous-status task-status]
   [:task-status task-status]
   [:task-owner entity-id]
   [:task-assignee [:maybe entity-id]]
   [:category [:string {:min 1 :max 80}]]
   [:price amount]
   [:currency currency]])

(def payment-authorized-event
  [:map {:closed true}
   [:event-id entity-id]
   [:event-type [:= "PaymentAuthorizedEvent"]]
   [:occurred-at instant-string]
   [:transaction-id entity-id]
   [:task-id entity-id]
   [:payer-id entity-id]
   [:payee-id entity-id]
   [:amount amount]
   [:currency currency]
   [:mp-payment-id [:string {:min 1 :max 64}]]
   [:mp-preference-id [:maybe :string]]])

(def funds-released-event
  [:map {:closed true}
   [:event-id entity-id]
   [:event-type [:= "FundsReleasedEvent"]]
   [:occurred-at instant-string]
   [:transaction-id entity-id]
   [:task-id entity-id]
   [:payee-id entity-id]
   [:amount amount]
   [:currency currency]
   [:mp-payment-id [:string {:min 1 :max 64}]]])

(def payment-failed-event
  [:map {:closed true}
   [:event-id entity-id]
   [:event-type [:= "PaymentFailedEvent"]]
   [:occurred-at instant-string]
   [:transaction-id entity-id]
   [:task-id entity-id]
   [:payer-id entity-id]
   [:amount amount]
   [:currency currency]
   [:mp-payment-id [:maybe :string]]
   [:reason [:string {:min 1 :max 120}]]])

(def transaction-history-query
  [:map {:closed true}
   [:user-id entity-id]
   [:limit {:optional true} [:int {:min 1 :max 100}]]])

(def transaction-history-item
  [:map {:closed true}
   [:transaction-id entity-id]
   [:task-id entity-id]
   [:role [:enum "PAYER" "PAYEE"]]
   [:amount amount]
   [:currency currency]
   [:status transaction-status]
   [:occurred-at instant-string]])

(def default-history-limit 20)

(def webhook-idempotency-ttl-seconds (* 24 60 60))

(defn valid?
  [schema value]
  (m/validate schema value))

(defn explain
  "Devuelve errores legibles, o nil si el valor cumple el esquema."
  [schema value]
  (when-not (m/validate schema value)
    (me/humanize (m/explain schema value))))
