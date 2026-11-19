(ns taskit.payment.application.services
  (:require [taskit.payment.application.checkout :as checkout]
            [taskit.payment.application.dispute :as dispute]
            [taskit.payment.application.history :as history]
            [taskit.payment.application.release :as release]
            [taskit.payment.application.webhook :as webhook]
            [taskit.payment.ports.inbound :as inbound]))

(defrecord CheckoutService [deps]
  inbound/CreatePaymentCheckoutUseCase
  (create-payment-checkout [_ command]
    (checkout/create-payment-checkout deps command)))

(defrecord WebhookService [deps]
  inbound/ProcessMercadoPagoWebhookUseCase
  (process-mercado-pago-webhook [_ notification]
    (webhook/process-mercado-pago-webhook deps notification)))

(defrecord ReleaseService [deps]
  inbound/ReleaseEscrowFundsUseCase
  (release-escrow-funds [_ command]
    (release/release-escrow-funds deps command)))

(defrecord DisputeService [deps]
  inbound/ProcessHitlDisputeUseCase
  (process-hitl-dispute [_ resolution]
    (dispute/process-hitl-dispute deps resolution)))

(defrecord HistoryService [deps]
  inbound/GetTransactionHistoryUseCase
  (get-transaction-history [_ query]
    (history/get-transaction-history deps query)))

(defn usecases
  [deps]
  {:checkout (->CheckoutService deps)
   :webhook (->WebhookService deps)
   :release (->ReleaseService deps)
   :dispute (->DisputeService deps)
   :history (->HistoryService deps)
   :deps deps})
