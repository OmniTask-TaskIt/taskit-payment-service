(ns taskit.payment.application.history
  (:require [taskit.payment.application.support :as support]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.logic :as logic]
            [taskit.payment.domain.model :as model]
            [taskit.payment.ports.outbound :as out]))

(defn get-transaction-history
  [deps query]
  (when-let [details (model/explain model/transaction-history-query query)]
    (support/throw-rejected!
     {:code errors/invalid-payload
      :message "Payload inválido"
      :details details}))
  (let [limit (or (:limit query) model/default-history-limit)
        rows (out/list-transactions-by-user
              (:repo deps) nil (:user-id query) limit)]
    {:items (logic/project-history rows (:user-id query))}))
