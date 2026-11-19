(ns taskit.payment.application.webhook-test
  (:require [clojure.test :refer [deftest is testing]]
            [taskit.payment.application.webhook :as webhook]
            [taskit.payment.domain.logic :as logic]
            [taskit.payment.domain.model :as model]
            [taskit.payment.ports.outbound :as out]))

(def task-id "11111111-1111-4111-8111-111111111111")
(def tx-id "8a1b0c2d-3e4f-4a5b-9c6d-7e8f90123456")
(def payer-id "22222222-2222-4222-8222-222222222222")
(def payee-id "33333333-3333-4333-8333-333333333333")
(def event-id "6f1c2a30-9b4e-4d1a-8c77-0a1b2c3d4e5f")
(def now "2026-11-16T18:00:00Z")
(def payment-id "987654321")

(defn- pending-tx
  []
  {:transaction-id tx-id
   :task-id task-id
   :payer-id payer-id
   :payee-id payee-id
   :amount 150000
   :currency "COP"
   :marketplace-fee 0
   :status "PENDING"
   :mp-preference-id "pref-1"
   :mp-payment-id nil
   :failure-reason nil
   :updated-at now})

(defn- gateway-payment
  [status]
  {:mp-payment-id payment-id
   :status status
   :status-detail (if (= status "approved") "accredited" "cc_rejected_insufficient_amount")
   :transaction-amount 150000
   :currency "COP"
   :external-reference tx-id})

(defn- notification
  []
  {:type "payment"
   :action "payment.updated"
   :data {:id payment-id}})

(defn- deps
  []
  {:idempotency ::redis
   :gateway ::mercadopago
   :repo ::db
   :tx-manager ::tx
   :publisher ::bus
   :now-fn (constantly now)
   :id-fn (constantly event-id)})

(defn- call-with-fakes
  [payment store gateway-calls db-writes published]
  (let [outbox (atom [])]
    (with-redefs [out/acquire! (fn [_ key ttl]
                                 (is (= model/webhook-idempotency-ttl-seconds ttl))
                                 (is (= (logic/webhook-idempotency-key payment-id) key))
                                 (if (contains? @store key)
                                   false
                                   (do (swap! store conj key) true)))
                  out/release! (fn [_ key]
                                 (swap! store disj key)
                                 true)
                  out/fetch-payment (fn [_ id]
                                      (swap! gateway-calls inc)
                                      (is (= payment-id id))
                                      payment)
                  out/in-transaction (fn [_ f] (f ::ctx))
                  out/find-transaction-by-id (fn [_ _ id]
                                               (is (= tx-id id))
                                               (pending-tx))
                  out/update-transaction-if-status! (fn [_ _ tx expected]
                                                      (swap! db-writes inc)
                                                      (is (= "PENDING" expected))
                                                      tx)
                  out/insert-outbox! (fn [_ _ event]
                                       (swap! outbox conj event)
                                       event)
                  out/unpublished-events (fn [_] @outbox)
                  out/publish! (fn [_ event]
                                 (swap! published conj event)
                                 :sent)
                  out/mark-published! (fn [_ id]
                                        (swap! outbox (fn [events]
                                                        (vec (remove #(= id (:event-id %)) events))))
                                        true)]
      (webhook/process-mercado-pago-webhook (deps) (notification)))))

(deftest approved-webhook-is-idempotent-in-redis
  (testing "la primera entrega retiene fondos y la copia no toca la pasarela ni la base"
    (let [store (atom #{})
          gateway-calls (atom 0)
          db-writes (atom 0)
          published (atom [])
          first-result (call-with-fakes (gateway-payment "approved")
                                        store gateway-calls db-writes published)
          calls-after-first @gateway-calls
          writes-after-first @db-writes
          second-result (call-with-fakes (gateway-payment "approved")
                                         store gateway-calls db-writes published)]
      (is (= :applied (:status first-result)))
      (is (= :retain (:action first-result)))
      (is (= "RETAINED" (:status (:transaction first-result))))
      (is (= payment-id (:mp-payment-id (:transaction first-result))))
      (is (= "PaymentAuthorizedEvent" (:event-type (first @published))))
      (is (= 1 (count @published)))
      (is (= 1 (count @store)))
      (is (= :duplicate (:status second-result)))
      (is (= calls-after-first @gateway-calls))
      (is (= writes-after-first @db-writes))
      (is (= 1 (count @published))))))

(deftest pending-webhook-releases-the-redis-key
  (testing "un pago aun pendiente no bloquea la notificacion approved posterior"
    (let [store (atom #{})
          gateway-calls (atom 0)
          db-writes (atom 0)
          published (atom [])
          pending-result (call-with-fakes (gateway-payment "pending")
                                          store gateway-calls db-writes published)]
      (is (= :ignored (:status pending-result)))
      (is (= :awaiting-gateway (:reason pending-result)))
      (is (empty? @store))
      (is (zero? @db-writes))
      (let [approved-result (call-with-fakes (gateway-payment "approved")
                                             store gateway-calls db-writes published)]
        (is (= :applied (:status approved-result)))
        (is (= 1 @db-writes))))))

(deftest rejected-webhook-publishes-failure-and-ignores-the-duplicate
  (let [store (atom #{})
        gateway-calls (atom 0)
        db-writes (atom 0)
        published (atom [])
        first-result (call-with-fakes (gateway-payment "rejected")
                                      store gateway-calls db-writes published)
        second-result (call-with-fakes (gateway-payment "rejected")
                                       store gateway-calls db-writes published)]
    (is (= :applied (:status first-result)))
    (is (= :reject (:action first-result)))
    (is (= "REJECTED" (:status (:transaction first-result))))
    (is (= "PaymentFailedEvent" (:event-type (first @published))))
    (is (= :duplicate (:status second-result)))
    (is (= 1 @gateway-calls))
    (is (= 1 @db-writes))))

(deftest gateway-failure-clears-the-redis-key
  (let [store (atom #{})]
    (with-redefs [out/acquire! (fn [_ key _]
                                 (swap! store conj key)
                                 true)
                  out/release! (fn [_ key]
                                 (swap! store disj key)
                                 true)
                  out/fetch-payment (fn [& _]
                                      (throw (java.io.IOException. "red caida")))]
      (is (thrown? java.io.IOException
                   (webhook/process-mercado-pago-webhook (deps) (notification))))
      (is (empty? @store)))))

(deftest unsupported-topic-does-not-touch-redis
  (let [calls (atom 0)]
    (with-redefs [out/acquire! (fn [& _]
                                 (swap! calls inc)
                                 true)]
      (is (= {:status :ignored :reason :unsupported-topic}
             (webhook/process-mercado-pago-webhook
              (deps)
              {:type "merchant_order" :data {:id "1"}})))
      (is (zero? @calls)))))
