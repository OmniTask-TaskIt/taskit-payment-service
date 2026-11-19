(ns taskit.payment.core
  (:gen-class)
  (:require [integrant.core :as ig]
            [taskit.payment.system :as system]))

(defn -main
  [& _]
  (let [running (ig/init (system/config))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. #(ig/halt! running)))
    (.join (::system/server running))))
