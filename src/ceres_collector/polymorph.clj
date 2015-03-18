(ns ceres-collector.polymorph)

(defprotocol Database
  (init [this])
  (transact [this entry])
  (transact-and-return-id [this entry]))

(defrecord DbEntry [type value])
