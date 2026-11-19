(ns taskit.payment.domain.logic
  "Decisiones de negocio puras: checkout, webhook, escrow, disputa e historial.

  Ninguna funcion abre sockets ni transacciones. El caso de uso aplica
  el resultado sobre los puertos."
  (:require [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.model :as model]
            [malli.core :as m]
            [malli.error :as me]))

(def ^:private forbidden-card-keys
  #{"pan" "cvv" "cvc" "card-number" "card_number" "cardnumber"
    "security-code" "security_code" "card-token" "card_token"
    "expiration" "expiry"})

(defn- key-name
  [k]
  (cond
    (keyword? k) (name k)
    (string? k) k
    :else nil))

(defn card-data-present?
  "Recorre mapas y secuencias. Una clave de tarjeta alcanza para rechazar el comando."
  [payload]
  (letfn [(walk [node]
            (cond
              (map? node)
              (or (some forbidden-card-keys (keep key-name (keys node)))
                  (some walk (vals node)))

              (sequential? node)
              (some walk node)

              :else false))]
    (boolean (walk payload))))

(defn webhook-idempotency-key
  "SHA-256 de payment_id. El adaptador Redis guarda la clave con TTL de 24 horas."
  [payment-id]
  (let [digest (.digest (java.security.MessageDigest/getInstance "SHA-256")
                        (.getBytes (str "taskit.payment/mp-webhook:" payment-id)
                                   java.nio.charset.StandardCharsets/UTF_8))
        hex (apply str (map (fn [b]
                              (format "%02x" (bit-and 0xff (long b))))
                            digest))]
    (str "idempotency:mp:webhook:" hex)))

(defn webhook-delivery-decision
  "first-claim? lo responde IdempotencyPort. Un duplicado se descarta sin error."
  [first-claim?]
  (if first-claim?
    {:status :process}
    {:status :duplicate}))

(defn classify-gateway-status
  "Traduce el status de Mercado Pago a una decision interna.
  :approved retiene el escrow. :pending (incluido authorized) espera.
  :chargeback y :unknown no mueven dinero solos."
  [status]
  (case status
    "approved" :approved
    ("rejected" "cancelled") :failed
    ("pending" "in_process" "authorized") :pending
    "refunded" :refunded
    "charged_back" :chargeback
    :unknown))

(defn on-task-status-changed
  "SEMI_COMPLETED habilita el checkout. COMPLETED pide liberar el escrow.
  El resto de estados no mueve fondos."
  [event]
  (case (:task-status event)
    "SEMI_COMPLETED" {:effect :checkout-enabled :task-id (:task-id event)}
    "COMPLETED" {:effect :release-escrow :task-id (:task-id event)}
    {:effect :none :task-id (:task-id event)}))

(defn- reject
  [code message details]
  {:status :rejected :code code :message message :details details})

(defn- schema-error
  [schema value]
  (when-not (m/validate schema value)
    (reject errors/invalid-payload
            "Payload inválido"
            (me/humanize (m/explain schema value)))))

(defn- invariant!
  [schema value]
  (when-not (m/validate schema value)
    (throw (ex-info "Invariante de dominio violada"
                    {:explain (me/humanize (m/explain schema value))})))
  value)

(defn- preference-request
  [transaction task collector-id]
  {:external-reference (:transaction-id transaction)
   :title (:task-name task)
   :amount (:amount transaction)
   :currency (:currency transaction)
   :marketplace-fee (:marketplace-fee transaction)
   :collector-id collector-id})

(defn- pending-transaction
  [command transaction-id now]
  {:transaction-id transaction-id
   :task-id (:task-id command)
   :payer-id (:payer-id command)
   :payee-id (:payee-id command)
   :amount (:amount command)
   :currency (:currency command)
   :marketplace-fee (:marketplace-fee command)
   :status "PENDING"
   :mp-preference-id nil
   :mp-payment-id nil
   :failure-reason nil
   :updated-at now})

(defn- same-commercial-terms?
  [transaction command]
  (and (= (:payer-id transaction) (:payer-id command))
       (= (:payee-id transaction) (:payee-id command))
       (= (:amount transaction) (:amount command))
       (= (:currency transaction) (:currency command))
       (= (:marketplace-fee transaction) (:marketplace-fee command))))

(defn interpret-checkout
  "Decide si se crea una preferencia Split.

  `existing` es la transaccion vigente de la tarea, o nil.
  `ids` es {:transaction-id :now}. Un PENDING con la misma preferencia
  se reexpone; un REJECTED permite un intento nuevo."
  [task command existing {:keys [transaction-id now]}]
  (or (when (card-data-present? command)
        (reject errors/card-data-forbidden
                "El comando incluye datos de tarjeta. Task It no los acepta ni los persiste."
                nil))
      (schema-error model/task task)
      (schema-error model/create-checkout-command command)
      (when (some? existing)
        (schema-error model/transaction existing))
      (schema-error [:map {:closed true}
               [:transaction-id model/entity-id]
               [:now model/instant-string]]
              {:transaction-id transaction-id :now now})
      (cond
        (not= "SEMI_COMPLETED" (:task-status task))
        (reject errors/checkout-not-allowed
                "El checkout solo se habilita con la tarea en SEMI_COMPLETED."
                {:task-status (:task-status task)})

        (nil? (:task-assignee task))
        (reject errors/missing-payee
                "La tarea no tiene prestador asignado."
                {:task-id (:task-id task)})

        (not= (:task-id task) (:task-id command))
        (reject errors/task-mismatch
                "El comando no corresponde a la tarea."
                nil)

        (not= (:payer-id command) (:task-owner task))
        (reject errors/checkout-conflict
                "El pagador debe ser el dueño de la tarea."
                nil)

        (not= (:payee-id command) (:task-assignee task))
        (reject errors/checkout-conflict
                "El cobrador debe ser el prestador asignado."
                nil)

        (not= (:amount command) (:price task))
        (reject errors/amount-mismatch
                "El monto del checkout debe ser el precio de la tarea."
                {:price (:price task) :amount (:amount command)})

        (not= (:currency command) (:currency task))
        (reject errors/currency-mismatch
                "La moneda del checkout debe ser la moneda de la tarea."
                {:currency (:currency task)})

        (and existing (#{"RETAINED" "RELEASED" "REFUNDED"} (:status existing)))
        (reject errors/already-paid
                "La tarea ya tiene una transaccion que movio fondos."
                {:status (:status existing)})

        (and existing (= "PENDING" (:status existing)))
        (if-not (same-commercial-terms? existing command)
          (reject errors/checkout-conflict
                  "Ya existe un checkout pendiente con otros terminos."
                  {:transaction-id (:transaction-id existing)})
          (if (:mp-preference-id existing)
            {:status :replay :transaction existing}
            {:status :resume
             :transaction existing
             :preference (invariant! model/gateway-preference-request
                                     (preference-request existing task (:payee-collector-id command)))}))

        :else
        (let [tx (invariant! model/transaction
                             (pending-transaction command transaction-id now))]
          {:status :create
           :transaction tx
           :preference (invariant! model/gateway-preference-request
                                   (preference-request tx task (:payee-collector-id command)))}))))

(defn with-preference
  "Completa la transaccion PENDING con el id que devolvio Mercado Pago."
  [transaction preference-id now]
  (invariant! model/transaction
              (assoc transaction
                     :mp-preference-id preference-id
                     :updated-at now)))

(defn- payment-event
  [base]
  (invariant! model/payment-authorized-event
              (assoc base :event-type "PaymentAuthorizedEvent")))

(defn- failed-event
  [base]
  (invariant! model/payment-failed-event
              (assoc base :event-type "PaymentFailedEvent")))

(defn- released-event
  [base]
  (invariant! model/funds-released-event
              (assoc base :event-type "FundsReleasedEvent")))
(defn- refunded-event
  [base]
  (invariant! model/payment-refunded-event
              (assoc base :event-type "PaymentRefundedEvent")))

(defn- touch
  [transaction status now extra]
  (invariant! model/transaction
              (merge transaction extra {:status status :updated-at now})))

(defn interpret-payment-update
  "Concilia una transaccion con un pago ya consultado en Mercado Pago.
  El mapa `payment` no debe traer datos de tarjeta: el esquema es cerrado."
  [{:keys [transaction payment now event-id]}]
  (or (schema-error model/transaction transaction)
      (schema-error model/gateway-payment payment)
      (schema-error model/entity-id event-id)
      (schema-error model/instant-string now)
      (cond
        (and (:mp-payment-id transaction)
             (not= (:mp-payment-id transaction) (:mp-payment-id payment)))
        (reject errors/gateway-conflict
                "El pago no corresponde a la transaccion."
                {:mp-payment-id (:mp-payment-id transaction)})

        (not= (:transaction-amount payment) (:amount transaction))
        (reject errors/amount-mismatch
                "El monto cobrado no coincide con la transaccion."
                {:expected (:amount transaction)
                 :actual (:transaction-amount payment)})

        (not= (:currency payment) (:currency transaction))
        (reject errors/currency-mismatch
                "La moneda cobrada no coincide con la transaccion."
                nil)

        :else
        (let [kind (classify-gateway-status (:status payment))
              status (:status transaction)]
          (cond
            (and (= status "PENDING") (= kind :approved))
            (let [tx (touch transaction "RETAINED" now
                            {:mp-payment-id (:mp-payment-id payment)})]
              {:status :applied
               :action :retain
               :transaction tx
               :event (payment-event
                       {:event-id event-id
                        :occurred-at now
                        :transaction-id (:transaction-id tx)
                        :task-id (:task-id tx)
                        :payer-id (:payer-id tx)
                        :payee-id (:payee-id tx)
                        :amount (:amount tx)
                        :currency (:currency tx)
                        :mp-payment-id (:mp-payment-id payment)
                        :mp-preference-id (:mp-preference-id tx)})})

            (and (= status "PENDING") (= kind :failed))
            (let [reason (or (:status-detail payment) (:status payment))
                  tx (touch transaction "REJECTED" now
                            {:mp-payment-id (:mp-payment-id payment)
                             :failure-reason reason})]
              {:status :applied
               :action :reject
               :transaction tx
               :event (failed-event
                       {:event-id event-id
                        :occurred-at now
                        :transaction-id (:transaction-id tx)
                        :task-id (:task-id tx)
                        :payer-id (:payer-id tx)
                        :amount (:amount tx)
                        :currency (:currency tx)
                        :mp-payment-id (:mp-payment-id payment)
                        :reason reason})})

            (and (= status "PENDING") (= kind :pending))
            {:status :ignored :reason :awaiting-gateway}

            (and (= status "RETAINED") (= kind :approved))
            {:status :ignored :reason :already-retained}

            (and (= status "RETAINED") (= kind :refunded))
            {:status :applied
             :action :sync-refund
             :transaction (touch transaction "REFUNDED" now {})
             :event nil}

            (and (= status "REFUNDED") (= kind :refunded))
            {:status :ignored :reason :already-refunded}

            (and (= status "REJECTED") (= kind :failed))
            {:status :ignored :reason :already-rejected}

            (and (= status "RELEASED") (= kind :approved))
            {:status :ignored :reason :already-released}

            (and (= status "RELEASED") (= kind :refunded))
            (reject errors/gateway-conflict
                    "La pasarela informa un reembolso sobre fondos ya liberados."
                    {:status status})

            (and (= status "RETAINED") (= kind :failed))
            (reject errors/gateway-conflict
                    "La pasarela rechazó un cobro que ya estaba retenido."
                    {:status status})

            (#{:chargeback :unknown} kind)
            (reject errors/gateway-conflict
                    "Estado de Mercado Pago sin transicion automatica."
                    {:gateway-status (:status payment)})

            :else
            (reject errors/invalid-transition
                    "La transaccion no admite el estado informado por la pasarela."
                    {:status status :gateway-status (:status payment)}))))))

(defn interpret-release
  "COMPLETED + RETAINED libera fondos al prestador si no hay bloqueo por fraude."
  [{:keys [task transaction frozen-reason now event-id]}]
  (or (schema-error model/task task)
      (when-not transaction
        (reject errors/transaction-not-found
                "No hay transaccion para liberar."
                {:task-id (:task-id task)}))
      (schema-error model/transaction transaction)
      (schema-error model/entity-id event-id)
      (schema-error model/instant-string now)
      (cond
        (not= (:task-id task) (:task-id transaction))
        (reject errors/task-mismatch
                "La transaccion pertenece a otra tarea."
                nil)

        (not= "COMPLETED" (:task-status task))
        (reject errors/invalid-transition
                "El escrow se libera cuando la tarea esta COMPLETED."
                {:task-status (:task-status task)})

        (= "RELEASED" (:status transaction))
        {:status :ignored :reason :already-released :transaction transaction}

        (some? frozen-reason)
        (reject errors/invalid-transition
                "Fondos congelados por alerta critica de fraude (AITA). Requiere revision de administrador."
                {:task-id (:task-id task) :reason frozen-reason})

        (= "RETAINED" (:status transaction))
        (if-not (:mp-payment-id transaction)
          (reject errors/gateway-conflict
                  "No hay pago de Mercado Pago asociado al escrow."
                  {:transaction-id (:transaction-id transaction)})
          (let [tx (touch transaction "RELEASED" now {})]
            {:status :applied
             :action :release
             :transaction tx
             :event (released-event
                     {:event-id event-id
                      :occurred-at now
                      :transaction-id (:transaction-id tx)
                      :task-id (:task-id tx)
                      :payee-id (:payee-id tx)
                      :amount (:amount tx)
                      :currency (:currency tx)
                      :mp-payment-id (:mp-payment-id tx)})}))

        :else
        (reject errors/invalid-transition
                "Solo un escrow RETAINED puede liberarse."
                {:status (:status transaction)}))))

(defn interpret-dispute
  "Un ADMIN mueve un escrow RETAINED a reembolso o a liberacion.
  La repeticion del estado destino se ignora."
  [{:keys [transaction resolution now event-id]}]
  (or (when (card-data-present? resolution)
        (reject errors/card-data-forbidden
                "La resolucion incluye datos de tarjeta."
                nil))
      (schema-error model/transaction transaction)
      (schema-error model/dispute-resolved-event resolution)
      (schema-error model/entity-id event-id)
      (schema-error model/instant-string now)
      (cond
        (not= "ADMIN" (:actor-role resolution))
        (reject errors/unauthorized-hitl
                "Solo un administrador HITL puede resolver la disputa."
                {:actor-role (:actor-role resolution)})

        (not= (:task-id resolution) (:task-id transaction))
        (reject errors/task-mismatch
                "La disputa no corresponde a la transaccion."
                nil)

        (not= (:transaction-id resolution) (:transaction-id transaction))
        (reject errors/task-mismatch
                "La disputa apunta a otra transaccion."
                nil)

        :else
        (let [outcome (:outcome resolution)
              status (:status transaction)
              target (case outcome
                       "REFUND_PAYER" "REFUNDED"
                       "RELEASE_PAYEE" "RELEASED")]
          (cond
            (= status target)
            {:status :ignored :reason :already-applied :transaction transaction}

            (not= status "RETAINED")
            (reject errors/invalid-transition
                    "La disputa solo actua sobre fondos retenidos."
                    {:status status :outcome outcome})

            (= outcome "REFUND_PAYER")
            (let [tx (touch transaction "REFUNDED" now {})]
              {:status :applied
               :action :refund
               :transaction tx
               :event (refunded-event
                       {:event-id event-id
                        :occurred-at now
                        :transaction-id (:transaction-id tx)
                        :task-id (:task-id tx)
                        :payer-id (:payer-id tx)
                        :amount (:amount tx)
                        :currency (:currency tx)
                        :mp-payment-id (:mp-payment-id tx)
                        :reason (:reason resolution)})})

            (= outcome "RELEASE_PAYEE")
            (if-not (:mp-payment-id transaction)
              (reject errors/gateway-conflict
                      "No hay pago de Mercado Pago asociado al escrow."
                      {:transaction-id (:transaction-id transaction)})
              (let [tx (touch transaction "RELEASED" now {})]
                {:status :applied
                 :action :release
                 :transaction tx
                 :event (released-event
                         {:event-id event-id
                          :occurred-at now
                          :transaction-id (:transaction-id tx)
                          :task-id (:task-id tx)
                          :payee-id (:payee-id tx)
                          :amount (:amount tx)
                          :currency (:currency tx)
                          :mp-payment-id (:mp-payment-id tx)})})))))))

(defn history-item
  "Proyeccion sin preferencia, sin id de pasarela y sin datos de tarjeta."
  [transaction user-id]
  (let [role (cond
               (= user-id (:payer-id transaction)) "PAYER"
               (= user-id (:payee-id transaction)) "PAYEE")]
    (when role
      {:transaction-id (:transaction-id transaction)
       :task-id (:task-id transaction)
       :role role
       :amount (:amount transaction)
       :currency (:currency transaction)
       :status (:status transaction)
       :occurred-at (:updated-at transaction)})))

(defn project-history
  [transactions user-id]
  (->> transactions
       (keep #(history-item % user-id))
       (sort-by :occurred-at #(compare %2 %1))
       vec))

(defn normalize-webhook
  "Se queda con el id del pago. El cuerpo crudo de Mercado Pago no se persiste."
  [payload]
  (let [raw-id (or (get-in payload [:data :id])
                   (get-in payload ["data" "id"])
                   (:id payload)
                   (get payload "id"))
        topic (or (:type payload)
                  (get payload "type")
                  (:topic payload)
                  (get payload "topic"))]
    (when (and (some? raw-id) (= "payment" (str topic)))
      {:mp-payment-id (str raw-id)
       :topic "payment"
       :action (some-> (or (:action payload) (get payload "action")) str)})))

(defn interpret-fraud-alert
  "Solo congela escrow si AITA recomienda ALERT_ADMIN sobre una tarea con UUID válido."
  [event]
  (or (schema-error model/fraud-alert-detected-event event)
      (let [action (:recommended_action event)
            task-id (get-in event [:analysis :task_id])
            reason (or (get-in event [:analysis :reason])
                       "Alerta de fraude detectada por AITA")]
        (cond
          (not= "ALERT_ADMIN" action)
          {:status :ignored :reason :not-critical}

          (or (nil? task-id) (not (model/valid? model/entity-id task-id)))
          {:status :ignored :reason :missing-or-invalid-task-id}

          :else
          {:status :freeze
           :task-id task-id
           :risk-level (get-in event [:analysis :risk_level])
           :reason reason}))))