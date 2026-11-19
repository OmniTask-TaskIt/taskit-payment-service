(ns taskit.payment.adapters.gateway.mercadopago
  "Cliente HTTP de Mercado Pago. No persiste ni registra PAN, CVV ni tokens de tarjeta."
  (:require [clojure.tools.logging :as log]
            [hato.client :as http]
            [resilience4clj-circuitbreaker.core :as breaker]
            [resilience4clj-retry.core :as retry]
            [resilience4clj-timelimiter.core :as limiter]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.infra.json :as json]
            [taskit.payment.ports.outbound :as out])
  (:import (io.github.resilience4j.circuitbreaker CircuitBreaker CircuitBreakerOpenException)
           (io.github.resilience4j.retry Retry)
           (io.github.resilience4j.timelimiter TimeLimiter)
           (java.util.concurrent Callable Executors TimeUnit TimeoutException
                                 ExecutionException)))

(defn- unavailable!
  [message]
  (throw (errors/domain-exception errors/gateway-unavailable message nil)))

(defn- conflict!
  [message status]
  (throw (errors/domain-exception errors/gateway-conflict message {:status status})))

(defn- unwrap
  [throwable]
  (if (and (instance? ExecutionException throwable) (.getCause throwable))
    (unwrap (.getCause throwable))
    throwable))

(defn- long-amount
  [value]
  (long (Math/round (double value))))

(defn- preference-body
  [request notification-url]
  (cond-> {:items [{:title (:title request)
                    :quantity 1
                    :currency_id (:currency request)
                    :unit_price (:amount request)}]
           :marketplace_fee (:marketplace-fee request)
           :external_reference (:external-reference request)
           :binary_mode false
           :metadata {:collector_id (:collector-id request)}}
    (seq notification-url) (assoc :notification_url notification-url)))

(defn- payment-view
  [body]
  {:mp-payment-id (str (:id body))
   :status (str (:status body))
   :status-detail (some-> (:status_detail body) str)
   :transaction-amount (long-amount (:transaction_amount body))
   :currency (str (:currency_id body))
   :external-reference (some-> (:external_reference body) str)})

(defn- parse-body
  [response]
  (let [raw (:body response)]
    (cond
      (nil? raw) nil
      (string? raw) (try (json/read-json raw) (catch Throwable _ {:raw raw}))
      (map? raw) raw
      :else nil)))

(defn- invoke
  [gateway idempotency-key method path body]
  (let [call (reify Callable
               (call [_]
                 (let [response (http/request
                                 {:request-method method
                                  :url (str (:base-url gateway) path)
                                  :http-client (:client gateway)
                                  :throw-exceptions? false
                                  :headers (cond-> {"Authorization" (str "Bearer " (:access-token gateway))
                                                    "Accept" "application/json"}
                                             idempotency-key (assoc "X-Idempotency-Key" idempotency-key)
                                             body (assoc "Content-Type" "application/json"))
                                  :body (some-> body json/write-json)
                                  :as :string
                                  :timeout 700})
                       status (:status response)
                       parsed (parse-body response)]
                   (cond
                     (<= 200 status 299) parsed
                     (<= 500 status 599) (throw (ex-info "Mercado Pago no disponible"
                                                         {:type ::unavailable :status status}))
                     :else {:http-error status :body parsed}))))
        retried (Retry/decorateCallable (:retry gateway) call)
        guarded (CircuitBreaker/decorateCallable (:breaker gateway) retried)
        supplier (reify java.util.function.Supplier
                   (get [_]
                     (.submit (:executor gateway)
                              ^Callable guarded)))
        limited (TimeLimiter/decorateFutureSupplier (:limiter gateway) supplier)]
    (try
      (.call limited)
      (catch Throwable thrown
        (let [cause (unwrap thrown)]
          (cond
            (errors/domain-error? cause) (throw cause)
            (instance? CircuitBreakerOpenException cause)
            (unavailable! "Mercado Pago esta abierto en el circuit breaker.")

            (instance? TimeoutException cause)
            (unavailable! "Mercado Pago no respondio a tiempo.")

            (instance? java.io.IOException cause)
            (unavailable! "No hubo red con Mercado Pago.")

            (= ::unavailable (:type (ex-data cause)))
            (unavailable! "Mercado Pago respondio con error de servidor.")

            :else (throw cause)))))))

(defn- ensure-success
  [result message]
  (if (:http-error result)
    (conflict! message (:http-error result))
    result))

(defrecord MercadoPagoGateway
           [client access-token base-url notification-url breaker retry limiter executor]
  out/PaymentGatewayPort
  (create-preference [this request]
    (let [body (ensure-success
                (invoke this
                        (str "preference-" (:external-reference request))
                        :post
                        "/checkout/preferences"
                        (preference-body request notification-url))
                "Mercado Pago rechazo la preferencia.")]
      {:mp-preference-id (str (:id body))
       :init-point (or (:init_point body) (:sandbox_init_point body))}))

  (fetch-payment [this mp-payment-id]
    (let [body (ensure-success
                (invoke this
                        (str "fetch-" mp-payment-id)
                        :get
                        (str "/v1/payments/" mp-payment-id)
                        nil)
                "Mercado Pago no devolvio el pago.")]
      (payment-view body)))

  (disburse [this transaction]
    (let [payment-id (:mp-payment-id transaction)
          result (invoke this
                         (str "disburse-" (:transaction-id transaction))
                         :put
                         (str "/v1/payments/" payment-id)
                         {:capture true})]
      (if-not (:http-error result)
        (payment-view result)
        (let [current (out/fetch-payment this payment-id)]
          (if (= "approved" (:status current))
            current
            (conflict! "Mercado Pago no libero el escrow." (:http-error result)))))))

  (refund [this transaction]
    (let [payment-id (:mp-payment-id transaction)
          result (invoke this
                         (str "refund-" (:transaction-id transaction))
                         :post
                         (str "/v1/payments/" payment-id "/refunds")
                         {})]
      (if-not (:http-error result)
        result
        (let [current (out/fetch-payment this payment-id)]
          (if (= "refunded" (:status current))
            current
            (conflict! "Mercado Pago no reembolso el pago." (:http-error result))))))))

(defn start
  [{:keys [access-token base-url notification-url]}]
  (let [executor (Executors/newFixedThreadPool 4)]
    (log/info "Cliente Mercado Pago listo")
    (map->MercadoPagoGateway
     {:client (http/build-http-client {:connect-timeout 500
                                       :redirect-policy :never})
      :access-token access-token
      :base-url (or base-url "https://api.mercadopago.com")
      :notification-url notification-url
      :breaker (breaker/create "mercadopago"
                               {:failure-rate-threshold 50
                                :ring-buffer-size-in-closed-state 10
                                :ring-buffer-size-in-half-open-state 5
                                :wait-duration-in-open-state 10000})
      :retry (retry/create "mercadopago"
                           {:max-attempts 2
                            :wait-duration 150})
      :limiter (limiter/create {:timeout-duration 1600
                                :cancel-running-future? true})
      :executor executor})))

(defn stop!
  [gateway]
  (when-let [executor (:executor gateway)]
    (.shutdown executor)
    (.awaitTermination executor 2 TimeUnit/SECONDS)))