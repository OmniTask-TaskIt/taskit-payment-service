(ns taskit.payment.application.dispute
  (:require [taskit.payment.application.support :as support]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.logic :as logic]
            [taskit.payment.domain.model :as model]
            [taskit.payment.ports.outbound :as out]))

(defn process-hitl-dispute
  [deps resolution]
  (when-let [details (model/explain model/dispute-resolved-event resolution)]
    (support/throw-rejected!
     {:code errors/invalid-payload
      :message "Payload inválido"
      :details details}))
  (let [planned (out/in-transaction (:tx-manager deps)
                                    (fn [ctx]
                                      (let [tx (out/find-transaction-by-id
                                                (:repo deps) ctx (:transaction-id resolution))]
                                        (when-not tx
                                          (support/throw-rejected!
                                           {:code errors/transaction-not-found
                                            :message "No existe la transaccion de la disputa."
                                            :details {:transaction-id (:transaction-id resolution)}}))
                                        (let [result (logic/interpret-dispute
                                                      {:transaction tx
                                                       :resolution resolution
                                                       :now (support/now deps)
                                                       :event-id (support/id deps)})]
                                          (if (= :rejected (:status result))
                                            (support/throw-rejected! result)
                                            result)))))]
    (if (not= :applied (:status planned))
      planned
      (do
        (case (:action planned)
          :refund (out/refund (:gateway deps) (:transaction planned))
          :release (out/disburse (:gateway deps) (:transaction planned)))
        (let [saved (out/in-transaction (:tx-manager deps)
                                        (fn [ctx]
                                          (if (out/update-transaction-if-status!
                                               (:repo deps) ctx (:transaction planned) "RETAINED")
                                            (do
                                              (when-let [event (:event planned)]
                                                (out/insert-outbox! (:repo deps) ctx event))
                                              planned)
                                            {:status :ignored
                                             :reason :already-applied
                                             :transaction (:transaction planned)})))]
          (when-let [ff (:fraud-freeze deps)]
            (out/unfreeze-task! ff (:task-id resolution)))
          (support/flush-outbox! deps)
          saved)))))