(ns taskit.payment.adapters.cache.redis
  (:require [taoensso.carmine :as car]
            [taskit.payment.ports.outbound :as out]))

(defn- claimed?
  [reply]
  (or (= "OK" reply) (= :OK reply) (true? reply)))

(defrecord RedisIdempotency [conn-opts]
  out/IdempotencyPort
  (acquire! [_ idempotency-key ttl-seconds]
    (claimed? (car/wcar conn-opts
                        (car/set idempotency-key "1" :nx :ex ttl-seconds))))
  (release! [_ idempotency-key]
    (car/wcar conn-opts (car/del idempotency-key))
    true)

  out/FraudFreezePort
  (freeze-task! [_ task-id reason]
    (car/wcar conn-opts
              (car/set (str "fraud:freeze:" task-id) reason))
    true)
  (frozen-task [_ task-id]
    (car/wcar conn-opts
              (car/get (str "fraud:freeze:" task-id))))
  (unfreeze-task! [_ task-id]
    (car/wcar conn-opts
              (car/del (str "fraud:freeze:" task-id)))
    true))

(defn start
  [uri]
  (->RedisIdempotency {:spec {:uri uri}}))