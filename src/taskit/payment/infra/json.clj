(ns taskit.payment.infra.json
  (:require [jsonista.core :as json]))

(def mapper json/keyword-keys-object-mapper)

(defn write-json
  [value]
  (json/write-value-as-string value mapper))

(defn read-json
  [value]
  (json/read-value value mapper))
