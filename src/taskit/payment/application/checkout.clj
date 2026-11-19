(ns taskit.payment.application.checkout
  (:require [taskit.payment.application.support :as support]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.logic :as logic]
            [taskit.payment.domain.model :as model]
            [taskit.payment.ports.outbound :as out]))

(defn create-payment-checkout
  [deps command]
  (when-let [details (model/explain model/create-checkout-command command)]
    (support/throw-rejected!
     {:code errors/invalid-payload
      :message "Payload inválido"
      :details details}))
  (let [task (out/find-task-snapshot (:snapshots deps) (:task-id command))]
    (when-not task
      (support/throw-rejected!
       {:code errors/checkout-not-allowed
        :message "La tarea no esta habilitada para cobro."
        :details {:task-id (:task-id command)}}))
    (let [planned (out/in-transaction (:tx-manager deps)
                    (fn [ctx]
                      (let [existing (out/find-transaction-by-task-id
                                      (:repo deps) ctx (:task-id command))
                            result (logic/interpret-checkout
                                    task command existing
                                    {:transaction-id (support/id deps)
                                     :now (support/now deps)})]
                        (when (= :create (:status result))
                          (out/insert-transaction! (:repo deps) ctx (:transaction result)))
                        (if (= :rejected (:status result))
                          (support/throw-rejected! result)
                          result))))]
      (if (= :replay (:status planned))
        (assoc planned :init-point nil)
        (let [preference (out/create-preference (:gateway deps) (:preference planned))
              tx (logic/with-preference (:transaction planned)
                                        (:mp-preference-id preference)
                                        (support/now deps))]
          (out/in-transaction (:tx-manager deps)
            (fn [ctx]
              (out/update-transaction! (:repo deps) ctx tx)))
          {:status (:status planned)
           :transaction tx
           :init-point (:init-point preference)})))))
