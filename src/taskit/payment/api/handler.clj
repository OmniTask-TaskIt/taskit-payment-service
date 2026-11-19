(ns taskit.payment.api.handler
  (:require [clojure.tools.logging :as log]
            [muuntaja.core :as muuntaja]
            [reitit.coercion.malli]
            [reitit.ring :as ring]
            [reitit.ring.coercion :as coercion]
            [reitit.ring.middleware.muuntaja :as muuntaja-mw]
            [reitit.ring.middleware.parameters :as parameters]
            [taskit.payment.domain.errors :as errors]
            [taskit.payment.domain.model :as model]
            [taskit.payment.infra.json :as json]
            [taskit.payment.ports.inbound :as inbound]))

(def ^:private status-by-code
  {errors/invalid-payload 400
   errors/unauthorized-hitl 403
   errors/transaction-not-found 404
   errors/checkout-conflict 409
   errors/already-paid 409
   errors/gateway-conflict 409
   errors/invalid-transition 409
   errors/task-mismatch 409
   errors/gateway-unavailable 503})

(defn- json-response
  [status body]
  {:status status
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (json/write-json body)})

(defn- http-status
  [code]
  (get status-by-code code 422))

(defn- coercion-error?
  [throwable]
  (and (instance? clojure.lang.ExceptionInfo throwable)
       (contains? #{:reitit.coercion/request-coercion
                    :reitit.coercion/response-coercion}
                  (:type (ex-data throwable)))))

(defn- wrap-errors
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Throwable thrown
        (cond
          (errors/domain-error? thrown)
          (let [data (ex-data thrown)]
            (json-response (http-status (:code data))
                           {:code (:code data)
                            :message (ex-message thrown)
                            :details (:details data)}))

          (coercion-error? thrown)
          (json-response 400
                         {:code errors/invalid-payload
                          :message "Payload inválido"
                          :details (or (:humanized (ex-data thrown))
                                       (:errors (ex-data thrown)))})

          :else
          (do (log/error thrown "Error no controlado")
              (json-response 500
                             {:code "INTERNAL_ERROR"
                              :message "Error interno"})))))))

(defn- outcome-name
  [value]
  (some-> value name))

(defn- present-checkout
  [{:keys [status transaction init-point]}]
  {:outcome (outcome-name status)
   :transaction-id (:transaction-id transaction)
   :task-id (:task-id transaction)
   :status (:status transaction)
   :amount (:amount transaction)
   :currency (:currency transaction)
   :mp-preference-id (:mp-preference-id transaction)
   :init-point init-point})

(defn- present-movement
  [{:keys [status action reason transaction]}]
  {:outcome (outcome-name status)
   :action (outcome-name action)
   :reason (outcome-name reason)
   :transaction-id (:transaction-id transaction)
   :status (:status transaction)})

(defn- present-webhook
  [result]
  {:status (outcome-name (:status result))
   :action (outcome-name (:action result))
   :reason (outcome-name (:reason result))})

(defn- webhook-payload
  [request]
  (let [body (:body-params request)
        query (:query-params request)
        topic (get query "topic")
        id (or (get query "id") (get query "data.id"))]
    (merge (cond-> {}
             topic (assoc :topic topic)
             id (assoc :data {:id id}))
           (when (map? body) body))))

(defn- routes
  [services]
  [["/health"
    {:get {:handler (fn [_] {:status 200 :body {:status "ok"}})}}]
   ["/payments/checkout"
    {:post {:parameters {:body model/create-checkout-command}
            :handler (fn [{:keys [parameters]}]
                       {:status 201
                        :body (present-checkout
                               (inbound/create-payment-checkout
                                (:checkout services)
                                (:body parameters)))})}}]
   ["/payments/escrow/release"
    {:post {:parameters {:body model/release-escrow-command}
            :handler (fn [{:keys [parameters]}]
                       {:status 200
                        :body (present-movement
                               (inbound/release-escrow-funds
                                (:release services)
                                (:body parameters)))})}}]
   ["/payments/disputes"
    {:post {:parameters {:body model/dispute-resolved-event}
            :handler (fn [{:keys [parameters]}]
                       {:status 200
                        :body (present-movement
                               (inbound/process-hitl-dispute
                                (:dispute services)
                                (:body parameters)))})}}]
   ["/payments/history"
    {:get {:parameters {:query model/transaction-history-query}
           :handler (fn [{:keys [parameters]}]
                      {:status 200
                       :body (inbound/get-transaction-history
                              (:history services)
                              (:query parameters))})}}]
   ["/webhooks/mercadopago"
    {:post {:handler (fn [request]
                       {:status 200
                        :body (present-webhook
                               (inbound/process-mercado-pago-webhook
                                (:webhook services)
                                (webhook-payload request)))})}}]])

(defn handler
  [services]
  (wrap-errors
   (ring/ring-handler
    (ring/router
     (routes services)
     {:data {:muuntaja muuntaja/instance
             :coercion reitit.coercion.malli/coercion
             :middleware [parameters/parameters-middleware
                          muuntaja-mw/format-middleware
                          coercion/coerce-exceptions-middleware
                          coercion/coerce-request-middleware
                          coercion/coerce-response-middleware]}})
    (ring/create-default-handler
     {:not-found (fn [_]
                   (json-response 404 {:code "NOT_FOUND"
                                       :message "Ruta no encontrada"}))}))))
