(ns ceres-collector.protocols
  (:require [ceres-collector.db :refer [db]]
            [monger.collection :as mc]))


(defprotocol Entity
  (store [entity] "Stores entity in database"))


(defrecord Unknown [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "unknown" (assoc entity :ts (t/now)))))


(defrecord Reply [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "replies" (assoc entity :ts (t/now)))))

(defrecord Retweet [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "unknown" (assoc entity :ts (t/now)))))

(defrecord Unknown [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "unknown" (assoc entity :ts (t/now)))))

(defrecord Unknown [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "unknown" (assoc entity :ts (t/now)))))

(defrecord Unknown [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "unknown" (assoc entity :ts (t/now)))))
