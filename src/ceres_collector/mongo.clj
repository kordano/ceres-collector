(ns ceres-collector.mongo
  (:require [ceres-collector.polymorph :refer [Database transact]]
            [clj-time.core :as t]
            [monger.core :as mg]
            [monger.conversion :refer [from-db-object]]
            [taoensso.timbre :as timbre]
            [monger.collection :as mc])
  (:import [ceres_collector.polymorph DbEntry DbQuery]))


(timbre/refer-timbre)

(defn create-index
  "Define mongodb indices on first start"
  [db]
  (do
    (mc/ensure-index db "urls" (array-map :url 1))
    (mc/ensure-index db "urls" (array-map :ts 1))
    (mc/ensure-index db "mentions" (array-map :source 1))
    (mc/ensure-index db "mentions" (array-map :target 1))
    (mc/ensure-index db "mentions" (array-map :ts 1))
    (mc/ensure-index db "sources" (array-map :source 1))
    (mc/ensure-index db "sources" (array-map :target 1))
    (mc/ensure-index db "sources" (array-map :ts 1))
    (mc/ensure-index db "replies" (array-map :source 1))
    (mc/ensure-index db "replies" (array-map :target 1))
    (mc/ensure-index db "replies" (array-map :ts 1))
    (mc/ensure-index db "retweets" (array-map :source 1))
    (mc/ensure-index db "retweets" (array-map :target 1))
    (mc/ensure-index db "retweets" (array-map :ts 1))
    (mc/ensure-index db "pubs" (array-map :source 1))
    (mc/ensure-index db "pubs" (array-map :target 1))
    (mc/ensure-index db "pubs" (array-map :ts 1))
    (mc/ensure-index db "shares" (array-map :source 1))
    (mc/ensure-index db "shares" (array-map :target 1))
    (mc/ensure-index db "shares" (array-map :ts 1))
    (mc/ensure-index db "urlrefs" (array-map :source 1))
    (mc/ensure-index db "urlrefs" (array-map :target 1))
    (mc/ensure-index db "urlrefs" (array-map :ts 1))
    (mc/ensure-index db "tagrefs" (array-map :source 1))
    (mc/ensure-index db "tagrefs" (array-map :target 1))
    (mc/ensure-index db "tagrefs" (array-map :ts 1))
    (mc/ensure-index db "unknown" (array-map :source 1))
    (mc/ensure-index db "unknown" (array-map :target 1))
    (mc/ensure-index db "unknown" (array-map :ts 1))
    (mc/ensure-index db "tags" (array-map :text 1))
    (mc/ensure-index db "tags" (array-map :ts 1))
    (mc/ensure-index db "messages" (array-map :ts 1))
    (mc/ensure-index db "messages" (array-map :text 1))
    (mc/ensure-index db "messages" (array-map :tweet 1))
    (mc/ensure-index db "messages" (array-map :tid 1))
    (mc/ensure-index db "messages" (array-map :ts 1))
    (mc/ensure-index db "htmls" (array-map :ts 1))
    (mc/ensure-index db "users" (array-map :id 1))
    (mc/ensure-index db "users" (array-map :ts 1))
    (mc/ensure-index db "tweets" (array-map :user.screen_name 1))
    (mc/ensure-index db "tweets" (array-map :id 1))
    (mc/ensure-index db "tweets" (array-map :retweeted_status.id 1))
    (mc/ensure-index db "tweets" (array-map :in_reply_to_status_id 1))
    (mc/ensure-index db "tweets" (array-map :created_at 1))))


(defn type->coll [type]
  (case type
    :pub "pubs"
    :reply "replies"
    :retweet "retweets"
    :share "shares"
    :mention "mentions"
    :urlref "urlrefs"
    :tagref "tagrefs"
    :source "sources"
    :user "users"
    :tag "tags"
    :html "htmls"
    :tweet "tweets"
    :message "messages"
    :url "urls"
    :unknown "unknown"
    "unrelated"))


(defn store [db db-entry]
  (mc/insert-and-return db (type->coll (:type db-entry)) (:data db-entry)))


(defn find-id [db query]
  (:_id (mc/find-one-as-map db (type->coll (:type query)) (:query query))))


(defrecord MongoDB [db name opts sa]
  Database
  (transact [this entry] (store (:db this) entry))
  (transact-and-return-id [this entry] (-> (transact this entry)
                                       (from-db-object true)
                                       :_id))
  (retrieve-id [this query] (find-id (:db this) query)))


(defn init [& {:keys [name server-address] :as opts} ]
  "config must have keys server-address and name"
  (info "MongoDB - creating connection ...")
  (when-not opts
    (info "MongoDB - using default configuration"))
  (let [
        ^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300) ;; TODO: better options handling
        ^ServerAddress sa  (mg/server-address (or server-address  (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)
        name (or name "juno")
        db (mg/get-db (mg/connect sa opts) name)]
    (info "MongoDb - connected! ")
    (MongoDB. db name opts sa)))


(comment




  )
