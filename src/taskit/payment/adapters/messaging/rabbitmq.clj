(ns taskit.payment.adapters.messaging.rabbitmq
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.core :as rmq]
            [langohr.consumers :as lc]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [taskit.payment.application.events :as events]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.infra.json :as json]
            [taskit.payment.ports.outbound :as out]))

(defn- declare-topology!
  [channel {:keys [exchange dead-letter queue task-routing-key dispute-routing-key security-routing-key]}]
  (le/declare channel exchange "topic" {:durable true})
  (le/declare channel dead-letter "topic" {:durable true})
  (lq/declare channel (str queue ".dlq") {:durable true})
  (lq/bind channel (str queue ".dlq") dead-letter {:routing-key "#"})
  (lq/declare channel queue
              {:durable true
               :arguments {"x-dead-letter-exchange" dead-letter}})
  (lq/bind channel queue exchange {:routing-key task-routing-key})
  (lq/bind channel queue exchange {:routing-key dispute-routing-key})
  (lq/bind channel queue exchange {:routing-key (or security-routing-key "taskit.security.events")}))

(defn- permanent-domain-error?
  [throwable]
  (and (errors/domain-error? throwable)
       (not= errors/gateway-unavailable (:code (ex-data throwable)))))

(defn- consume!
  [channel usecases queue]
  (lb/qos channel 16)
  (lc/subscribe
   channel
   queue
   (fn [ch metadata ^bytes payload]
     (let [tag (:delivery-tag metadata)]
       (try
         (let [event (json/read-json payload)]
           (log/info "Evento recibido en" queue ":" (select-keys event [:event-id :task-id :task-status :outcome]))
           (events/dispatch! usecases event)
           (lb/ack ch tag))
         (catch Throwable thrown
           (if (permanent-domain-error? thrown)
             (do (log/warn thrown "Evento de pago descartado")
                 (lb/ack ch tag))
             (do (log/error thrown "Evento de pago enviado a la dead letter")
                 (lb/nack ch tag false false)))))))
   {:auto-ack false}))

(defrecord RabbitPublisher [enabled connection channel exchange routing-key queue]
  out/PaymentEventPublisherPort
  (publish! [_ event]
    (if-not enabled
      :skipped
      (locking channel
        (lb/publish channel exchange routing-key
                    (.getBytes (json/write-json event) "UTF-8")
                    {:content-type "application/json"
                     :persistent true
                     :message-id (:event-id event)
                     :type (:event-type event)})
        :sent))))

(defn start
  [{:keys [enabled url exchange dead-letter queue routing-key task-routing-key dispute-routing-key]}]
  (if-not enabled
    (do (log/info "RabbitMQ deshabilitado")
        (->RabbitPublisher false nil nil exchange routing-key queue))
    (let [connection (rmq/connect {:uri url :automatically-recover true})
          channel (lch/open connection)]
      (declare-topology! channel {:exchange exchange
                                  :dead-letter dead-letter
                                  :queue queue
                                  :task-routing-key task-routing-key
                                  :dispute-routing-key dispute-routing-key})
      (log/info "RabbitMQ conectado")
      (->RabbitPublisher true connection channel exchange routing-key queue))))

(defn listen!
  [publisher usecases]
  (when (:enabled publisher)
    (let [channel (lch/open (:connection publisher))]
      (consume! channel usecases (:queue publisher))
      (fn []
        (when-not (rmq/closed? channel)
          (rmq/close channel))))))

(defn stop!
  [publisher]
  (when-let [connection (:connection publisher)]
    (when-not (rmq/closed? connection)
      (rmq/close connection))))

