(ns ceres-collector.polymorph)

(defprotocol Database
  (init [this])
  (transact [this entry])
  (transact-and-return-id [this entry])
  (retrieve-id [this query]))

(defrecord DbEntry [type value])

(defrecord DbQuery [type query])
