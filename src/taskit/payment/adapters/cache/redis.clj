(ns taskit.payment.adapters.cache.redis
  (:require [taoensso.carmine :as car]
            [taskit.payment.ports.outbound :as out]))

(defn- claimed?
  [reply]
  (or (= "OK" reply) (= :OK reply) (true? reply)))

(defrecord RedisIdempotency [spec]
  out/IdempotencyPort
  (acquire! [_ idempotency-key ttl-seconds]
    (claimed? (car/wcar spec
                        (car/set idempotency-key "1" :nx :ex ttl-seconds))))
  (release! [_ idempotency-key]
    (car/wcar spec (car/del idempotency-key))
    true))

(defn start
  [uri]
  (->RedisIdempotency {:uri uri}))
