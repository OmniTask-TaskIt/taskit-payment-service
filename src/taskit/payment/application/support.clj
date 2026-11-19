(ns taskit.payment.application.support
  (:require [taskit.payment.domain.errors :as errors]
            [taskit.payment.ports.outbound :as out]))

(defn throw-rejected!
  [{:keys [code message details]}]
  (throw (errors/domain-exception code message details)))

(defn now-string []
  (str (java.time.Instant/now)))

(defn new-id []
  (str (random-uuid)))

(defn now
  [deps]
  ((or (:now-fn deps) now-string)))

(defn id
  [deps]
  ((or (:id-fn deps) new-id)))

(defn flush-outbox!
  "Publica lo que quedo confirmado en PostgreSQL. Si el broker responde,
  marca el evento. Un fallo de red deja la fila para el proximo intento."
  [{:keys [repo publisher]}]
  (doseq [event (out/unpublished-events repo)]
    (when (= :sent (out/publish! publisher event))
      (out/mark-published! repo (:event-id event)))))
