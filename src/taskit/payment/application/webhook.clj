(ns taskit.payment.application.webhook
  (:require [clojure.tools.logging :as log]
            [taskit.payment.application.support :as support]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.logic :as logic]
            [taskit.payment.domain.model :as model]
            [taskit.payment.ports.outbound :as out]))

(defn- gateway-view
  [payment]
  {:mp-payment-id (str (:mp-payment-id payment))
   :status (:status payment)
   :status-detail (:status-detail payment)
   :transaction-amount (:transaction-amount payment)
   :currency (:currency payment)})

(defn- retryable-error?
  [throwable]
  (or (not (errors/domain-error? throwable))
      (= errors/gateway-unavailable (:code (ex-data throwable)))))

(defn- persist!
  [deps ctx previous result]
  (case (:status result)
    :rejected (support/throw-rejected! result)
    :ignored result
    :applied
    (if (out/update-transaction-if-status!
         (:repo deps) ctx (:transaction result) (:status previous))
      (do
        (when-let [event (:event result)]
          (out/insert-outbox! (:repo deps) ctx event))
        result)
      {:status :ignored :reason :concurrent-update :transaction previous})
    result))

(defn process-mercado-pago-webhook
  [deps notification]
  (let [normalized (logic/normalize-webhook notification)]
    (if-not normalized
      {:status :ignored :reason :unsupported-topic}
      (let [payment-id (:mp-payment-id normalized)
            idem-key (logic/webhook-idempotency-key payment-id)
            decision (logic/webhook-delivery-decision
                      (out/acquire! (:idempotency deps)
                                    idem-key
                                    model/webhook-idempotency-ttl-seconds))]
        (if (= :duplicate (:status decision))
          decision
          (try
            (let [raw (out/fetch-payment (:gateway deps) payment-id)
                  external-reference (some-> (:external-reference raw) str)
                  _ (when-not (model/valid? model/entity-id external-reference)
                      (throw (errors/domain-exception
                              errors/transaction-not-found
                              "El pago no trae la transaccion en external_reference."
                              {:mp-payment-id payment-id})))
                  result (out/in-transaction (:tx-manager deps)
                           (fn [ctx]
                             (let [current (out/find-transaction-by-id
                                            (:repo deps) ctx external-reference)]
                               (when-not current
                                 (throw (errors/domain-exception
                                         errors/transaction-not-found
                                         "No existe la transaccion del pago."
                                         {:transaction-id external-reference})))
                               (persist! deps
                                         ctx
                                         current
                                         (logic/interpret-payment-update
                                          {:transaction current
                                           :payment (gateway-view raw)
                                           :now (support/now deps)
                                           :event-id (support/id deps)})))))]
              (when (and (= :ignored (:status result))
                         (= :awaiting-gateway (:reason result)))
                (out/release! (:idempotency deps) idem-key))
              (support/flush-outbox! deps)
              result)
            (catch Throwable t
              (when (retryable-error? t)
                (try
                  (out/release! (:idempotency deps) idem-key)
                  (catch Throwable release-error
                    (log/warn release-error "No se pudo liberar la llave de idempotencia"))))
              (throw t))))))))
