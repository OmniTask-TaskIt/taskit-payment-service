(ns taskit.payment.application.release
  (:require [taskit.payment.application.support :as support]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.logic :as logic]
            [taskit.payment.ports.outbound :as out]))

(defn release-escrow-funds
  [deps command]
  (let [task (or (:task command)
                 (when-let [task-id (:task-id command)]
                   (out/find-task-snapshot (:snapshots deps) task-id)))]
    (when-not task
      (support/throw-rejected!
       {:code errors/transaction-not-found
        :message "No hay tarea para liberar el escrow."
        :details {:task-id (:task-id command)}}))
    (let [planned (out/in-transaction (:tx-manager deps)
                    (fn [ctx]
                      (let [tx (out/find-transaction-by-task-id
                                (:repo deps) ctx (:task-id task))
                            result (logic/interpret-release
                                    {:task task
                                     :transaction tx
                                     :now (support/now deps)
                                     :event-id (support/id deps)})]
                        (if (= :rejected (:status result))
                          (support/throw-rejected! result)
                          result))))]
      (if (not= :applied (:status planned))
        planned
        (do
          (out/disburse (:gateway deps) (:transaction planned))
          (let [saved (out/in-transaction (:tx-manager deps)
                        (fn [ctx]
                          (if (out/update-transaction-if-status!
                               (:repo deps) ctx (:transaction planned) "RETAINED")
                            (do
                              (when-let [event (:event planned)]
                                (out/insert-outbox! (:repo deps) ctx event))
                              planned)
                            {:status :ignored
                             :reason :already-released
                             :transaction (:transaction planned)})))]
            (support/flush-outbox! deps)
            saved))))))
