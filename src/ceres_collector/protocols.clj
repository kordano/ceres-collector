(ns ceres-collector.protocols
  (:require [ceres-collector.db :refer [db]]
            [clj-time.core :as t]
            [monger.conversion :refer [from-db-object]]
            [monger.collection :as mc]))


(defprotocol Entity
  (store [entity] "Stores entity in database"
    [entity conn] "Stores entity in database that requires connection"))


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
    (mc/insert-and-return @db "retweets" (assoc entity :ts (t/now)))))

(defrecord Share [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "shares" (assoc entity :ts (t/now)))))

(defrecord UrlRef [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "urlrefs" (assoc entity :ts (t/now)))))

(defrecord TagRef [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "tagrefs" (assoc entity :ts (t/now)))))

(defrecord Mention [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "mentions" (assoc entity :ts (t/now)))))

(defrecord Pub [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "pubs" (assoc entity :ts (t/now)))))

(defrecord Source [source target]
  Entity
  (store [entity]
    (mc/insert-and-return @db "sources" (assoc entity :ts (t/now)))))

(defrecord User [name id created_at]
  Entity
  (store [entity]
    (-> (mc/insert-and-return @db "users" (assoc entity :ts (t/now)))
        (from-db-object true)
        :_id)))

(defrecord Url [path]
  Entity
  (store [entity]
    (-> (mc/insert-and-return @db "urls" (assoc entity :ts (t/now)))
        (from-db-object true)
        :_id)))

(defrecord Message [text source tweet]
  Entity
  (store [entity]
    (-> (mc/insert-and-return @db "messages" (assoc entity :ts (t/now)))
        (from-db-object true)
        :_id)))

(defrecord Html [url url-id]
  Entity
  (store [entity]
    (mc/insert-and-return @db "htmls" (assoc entity :ts (t/now)))))
