(ns taskit.payment.system
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [ring.adapter.jetty :as jetty]
            [taskit.payment.adapters.cache.redis :as redis]
            [taskit.payment.adapters.gateway.mercadopago :as mp]
            [taskit.payment.adapters.messaging.rabbitmq :as rabbit]
            [taskit.payment.adapters.persistence.jdbc :as store]
            [taskit.payment.api.handler :as api]
            [taskit.payment.application.services :as services]
            [taskit.payment.application.support :as support]))

(defn- setting
  [value name]
  (when (or (nil? value) (= "" (str value)))
    (throw (ex-info (str "Falta la variable " name) {:name name})))
  value)

(defn- enabled?
  [value]
  (contains? #{"true" "1" "TRUE" "True"} (str value)))

(defn- unquote-dotenv
  [raw]
  (cond
    (and (>= (count raw) 2)
         (str/starts-with? raw "\"")
         (str/ends-with? raw "\""))
    (subs raw 1 (dec (count raw)))

    (and (>= (count raw) 2)
         (str/starts-with? raw "'")
         (str/ends-with? raw "'"))
    (subs raw 1 (dec (count raw)))

    :else raw))

(defn load-dotenv!
  "Copia .env a propiedades del sistema para que Aero las vea.
  Una variable ya presente en el entorno o en la JVM no se pisa."
  []
  (let [file (io/file ".env")]
    (when (.isFile file)
      (with-open [reader (io/reader file)]
        (doseq [line (line-seq reader)]
          (let [trimmed (str/trim line)]
            (when (and (seq trimmed)
                       (not (str/starts-with? trimmed "#"))
                       (str/includes? trimmed "="))
              (let [idx (str/index-of trimmed "=")
                    k (str/trim (subs trimmed 0 idx))
                    v (unquote-dotenv (str/trim (subs trimmed (inc idx))))]
                (when (and (seq k)
                           (nil? (System/getenv k))
                           (nil? (System/getProperty k)))
                  (System/setProperty k v))))))))))

(defn config
  []
  (load-dotenv!)
  (let [cfg (aero/read-config (io/resource "config.edn"))]
    {::datasource {:jdbc-url (setting (:database-url cfg) "DATABASE_URL")}
     ::schema {:datasource (ig/ref ::datasource)}
     ::store {:datasource (ig/ref ::schema)}
     ::snapshots {:datasource (ig/ref ::schema)}
     ::redis {:uri (setting (:redis-url cfg) "REDIS_URL")}
     ::gateway {:access-token (:mp-access-token cfg)
                :base-url (:mp-base-url cfg)
                :notification-url (:mp-notification-url cfg)}
     ::publisher {:enabled (enabled? (:rabbitmq-enabled cfg))
                  :url (:rabbitmq-url cfg)
                  :exchange (:rabbitmq-exchange cfg)
                  :dead-letter (:rabbitmq-dead-letter cfg)
                  :queue (:rabbitmq-queue cfg)
                  :routing-key (:rabbitmq-routing-key cfg)
                  :task-routing-key (:rabbitmq-task-routing-key cfg)
                  :dispute-routing-key (:rabbitmq-dispute-routing-key cfg)}
     ::usecases {:repo (ig/ref ::store)
                 :tx-manager (ig/ref ::store)
                 :snapshots (ig/ref ::snapshots)
                 :idempotency (ig/ref ::redis)
                 :gateway (ig/ref ::gateway)
                 :publisher (ig/ref ::publisher)}
     ::listener {:broker (ig/ref ::publisher)
                 :usecases (ig/ref ::usecases)}
     ::handler {:usecases (ig/ref ::usecases)}
     ::server {:handler (ig/ref ::handler)
               :port (Long/parseLong (str (:port cfg)))}}))

(defmethod ig/init-key ::datasource [_ {:keys [jdbc-url]}]
  (jdbc/get-datasource {:jdbcUrl jdbc-url}))

(defmethod ig/init-key ::schema [_ {:keys [datasource]}]
  (store/migrate! datasource)
  datasource)

(defmethod ig/init-key ::store [_ {:keys [datasource]}]
  (store/store datasource))

(defmethod ig/init-key ::snapshots [_ {:keys [datasource]}]
  (store/snapshots datasource))

(defmethod ig/init-key ::redis [_ {:keys [uri]}]
  (redis/start uri))

(defmethod ig/init-key ::gateway [_ options]
  (mp/start options))

(defmethod ig/halt-key! ::gateway [_ gateway]
  (mp/stop! gateway))

(defmethod ig/init-key ::publisher [_ options]
  (when (and (:enabled options) (= "" (str (:url options))))
    (throw (ex-info "Falta RABBITMQ_URL" {})))
  (rabbit/start options))

(defmethod ig/halt-key! ::publisher [_ publisher]
  (rabbit/stop! publisher))

(defmethod ig/init-key ::usecases [_ deps]
  (future
    (try
      (support/flush-outbox! deps)
      (catch Throwable thrown
        (log/warn thrown "No se pudo vaciar la outbox al arrancar"))))
  (services/usecases deps))

(defmethod ig/init-key ::listener [_ {:keys [broker usecases]}]
  (rabbit/listen! broker (:deps usecases)))

(defmethod ig/halt-key! ::listener [_ stop]
  (when stop
    (stop)))

(defmethod ig/init-key ::handler [_ {:keys [usecases]}]
  (api/handler usecases))

(defmethod ig/init-key ::server [_ {:keys [handler port]}]
  (log/info (str "Payment service escuchando en el puerto " port))
  (jetty/run-jetty handler {:port port :join? false}))

(defmethod ig/halt-key! ::server [_ server]
  (.stop server))
