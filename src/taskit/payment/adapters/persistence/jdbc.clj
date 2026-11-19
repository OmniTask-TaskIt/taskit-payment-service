(ns taskit.payment.adapters.persistence.jdbc
  (:require [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [taskit.payment.infra.json :as json]
            [taskit.payment.ports.outbound :as out]))

(def ^:private jdbc-opts
  {:builder-fn rs/as-unqualified-kebab-maps})

(def ^:private statements
  ["CREATE TABLE IF NOT EXISTS transactions (
      transaction_id UUID PRIMARY KEY,
      task_id UUID NOT NULL,
      payer_id UUID NOT NULL,
      payee_id UUID NOT NULL,
      amount BIGINT NOT NULL,
      currency TEXT NOT NULL,
      marketplace_fee BIGINT NOT NULL,
      status TEXT NOT NULL,
      mp_preference_id TEXT,
      mp_payment_id TEXT,
      failure_reason TEXT,
      updated_at TIMESTAMPTZ NOT NULL
    )"
   "CREATE INDEX IF NOT EXISTS transactions_task_id_idx ON transactions (task_id, updated_at DESC)"
   "CREATE INDEX IF NOT EXISTS transactions_mp_payment_id_idx ON transactions (mp_payment_id)"
   "CREATE INDEX IF NOT EXISTS transactions_payer_id_idx ON transactions (payer_id)"
   "CREATE INDEX IF NOT EXISTS transactions_payee_id_idx ON transactions (payee_id)"
   "CREATE TABLE IF NOT EXISTS task_snapshots (
      task_id UUID PRIMARY KEY,
      task_name TEXT NOT NULL,
      task_status TEXT NOT NULL,
      task_owner UUID NOT NULL,
      task_assignee UUID,
      category TEXT NOT NULL,
      price BIGINT NOT NULL,
      currency TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )"
   "CREATE TABLE IF NOT EXISTS outbox (
      event_id UUID PRIMARY KEY,
      event_type TEXT NOT NULL,
      payload TEXT NOT NULL,
      published_at TIMESTAMPTZ
    )"
   "CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (published_at)"])

(defn migrate!
  [datasource]
  (doseq [statement statements]
    (jdbc/execute! datasource [statement])))

(defn- uuid
  [value]
  (java.util.UUID/fromString (str value)))

(defn- ts
  [value]
  (java.sql.Timestamp/from (java.time.Instant/parse value)))

(defn- as-instant
  [value]
  (cond
    (nil? value) nil
    (instance? java.sql.Timestamp value) (str (.toInstant ^java.sql.Timestamp value))
    (instance? java.time.Instant value) (str value)
    (instance? java.time.OffsetDateTime value) (str (.toInstant ^java.time.OffsetDateTime value))
    :else (str value)))

(defn- as-long
  [value]
  (when (some? value)
    (long value)))

(defn- row->transaction
  [row]
  {:transaction-id (str (:transaction-id row))
   :task-id (str (:task-id row))
   :payer-id (str (:payer-id row))
   :payee-id (str (:payee-id row))
   :amount (as-long (:amount row))
   :currency (:currency row)
   :marketplace-fee (as-long (:marketplace-fee row))
   :status (:status row)
   :mp-preference-id (:mp-preference-id row)
   :mp-payment-id (:mp-payment-id row)
   :failure-reason (:failure-reason row)
   :updated-at (as-instant (:updated-at row))})

(defn- row->task
  [row]
  {:task-id (str (:task-id row))
   :task-name (:task-name row)
   :task-status (:task-status row)
   :task-owner (str (:task-owner row))
   :task-assignee (some-> (:task-assignee row) str)
   :category (:category row)
   :price (as-long (:price row))
   :currency (:currency row)})

(defn- one
  [connectable sqlmap]
  (jdbc/execute-one! connectable (sql/format sqlmap) jdbc-opts))

(defn- many
  [connectable sqlmap]
  (jdbc/execute! connectable (sql/format sqlmap) jdbc-opts))

(defn- transaction-columns
  [transaction]
  {:transaction_id (uuid (:transaction-id transaction))
   :task_id (uuid (:task-id transaction))
   :payer_id (uuid (:payer-id transaction))
   :payee_id (uuid (:payee-id transaction))
   :amount (:amount transaction)
   :currency (:currency transaction)
   :marketplace_fee (:marketplace-fee transaction)
   :status (:status transaction)
   :mp_preference_id (:mp-preference-id transaction)
   :mp_payment_id (:mp-payment-id transaction)
   :failure_reason (:failure-reason transaction)
   :updated_at (ts (:updated-at transaction))})

(defrecord JdbcStore [datasource]
  out/TransactionManagerPort
  (in-transaction [_ f]
    (jdbc/with-transaction [tx datasource {:isolation :read-committed}]
      (f tx)))

  out/TransactionRepositoryPort
  (insert-transaction! [_ ctx transaction]
    (one ctx {:insert-into :transactions
              :values [(transaction-columns transaction)]})
    transaction)

  (update-transaction! [_ ctx transaction]
    (one ctx {:update :transactions
              :set (dissoc (transaction-columns transaction) :transaction_id :task_id :payer_id :payee_id)
              :where [:= :transaction_id (uuid (:transaction-id transaction))]})
    transaction)

  (update-transaction-if-status! [_ ctx transaction expected-status]
    (let [written (one ctx {:update :transactions
                            :set (dissoc (transaction-columns transaction)
                                         :transaction_id :task_id :payer_id :payee_id)
                            :where [:and
                                    [:= :transaction_id (uuid (:transaction-id transaction))]
                                    [:= :status expected-status]]})]
      (= 1 (:next.jdbc/update-count written))))

  (find-transaction-by-id [_ ctx transaction-id]
    (some-> (one (or ctx datasource)
                 {:select [:*]
                  :from :transactions
                  :where [:= :transaction_id (uuid transaction-id)]})
            row->transaction))

  (find-transaction-by-task-id [_ ctx task-id]
    (some-> (one (or ctx datasource)
                 {:select [:*]
                  :from :transactions
                  :where [:= :task_id (uuid task-id)]
                  :order-by [[:updated_at :desc]]
                  :limit 1})
            row->transaction))

  (find-transaction-by-mp-payment-id [_ ctx mp-payment-id]
    (some-> (one (or ctx datasource)
                 {:select [:*]
                  :from :transactions
                  :where [:= :mp_payment_id mp-payment-id]
                  :order-by [[:updated_at :desc]]
                  :limit 1})
            row->transaction))

  (list-transactions-by-user [_ ctx user-id limit]
    (->> (many (or ctx datasource)
               {:select [:*]
                :from :transactions
                :where [:or
                        [:= :payer_id (uuid user-id)]
                        [:= :payee_id (uuid user-id)]]
                :order-by [[:updated_at :desc]]
                :limit limit})
         (mapv row->transaction)))

  (insert-outbox! [_ ctx event]
    (one ctx {:insert-into :outbox
              :values [{:event_id (uuid (:event-id event))
                        :event_type (:event-type event)
                        :payload (json/write-json event)}]})
    event)

  (unpublished-events [_]
    (->> (many datasource
               {:select [:payload]
                :from :outbox
                :where [:is :published_at nil]
                :order-by [:event_id]})
         (mapv #(json/read-json (:payload %)))))

  (mark-published! [_ event-id]
    (one datasource
         {:update :outbox
          :set {:published_at (java.sql.Timestamp/from (java.time.Instant/now))}
          :where [:= :event_id (uuid event-id)]})
    true))

(defrecord JdbcSnapshots [datasource]
  out/TaskSnapshotPort
  (save-task-snapshot! [_ task]
    (one datasource
         {:insert-into :task_snapshots
          :values [{:task_id (uuid (:task-id task))
                    :task_name (:task-name task)
                    :task_status (:task-status task)
                    :task_owner (uuid (:task-owner task))
                    :task_assignee (some-> (:task-assignee task) uuid)
                    :category (:category task)
                    :price (:price task)
                    :currency (:currency task)
                    :updated_at (java.sql.Timestamp/from (java.time.Instant/now))}]
          :on-conflict [:task_id]
          :do-update-set [:task_name :task_status :task_owner :task_assignee
                          :category :price :currency :updated_at]})
    task)

  (find-task-snapshot [_ task-id]
    (some-> (one datasource
                 {:select [:*]
                  :from :task_snapshots
                  :where [:= :task_id (uuid task-id)]})
            row->task)))

(defn store
  [datasource]
  (->JdbcStore datasource))

(defn snapshots
  [datasource]
  (->JdbcSnapshots datasource))
