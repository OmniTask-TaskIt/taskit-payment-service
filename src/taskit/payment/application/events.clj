(ns taskit.payment.application.events
  (:require [taskit.payment.application.dispute :as dispute]
            [taskit.payment.application.release :as release]
            [taskit.payment.application.support :as support]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.logic :as logic]
            [taskit.payment.domain.model :as model]
            [taskit.payment.ports.outbound :as out]))

(defn on-task-status-changed
  [deps event]
  (when-let [details (model/explain model/task-status-changed-event event)]
    (support/throw-rejected!
     {:code errors/invalid-payload
      :message "Payload inválido"
      :details details}))
  (let [task (select-keys event [:task-id :task-name :task-status :task-owner
                                 :task-assignee :category :price :currency])]
    (out/save-task-snapshot! (:snapshots deps) task)
    (when (= :release-escrow (:effect (logic/on-task-status-changed event)))
      (release/release-escrow-funds deps {:task-id (:task-id task) :task task}))))

(defn dispatch!
  [deps event]
  (cond
    (contains? event :outcome)
    (dispute/process-hitl-dispute deps event)

    (contains? event :task-status)
    (on-task-status-changed deps event)

    :else
    (support/throw-rejected!
     {:code errors/invalid-payload
      :message "Evento no reconocido."
      :details nil})))
