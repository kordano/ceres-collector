(ns ceres-collector.mongo
  (:require [ceres-collector.polymorph :refer [Database transact]]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.shell :refer [sh]]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]]
            [monger.joda-time]
            [taoensso.timbre :as timbre])
  (:import [ceres_collector.polymorph DbEntry DbQuery]
           [com.mongodb MongoOptions ServerAddress]))


(timbre/refer-timbre)

(defn create-index
  "Define mongodb indices on first start"
  [db]
  (info "MONGODB - Creating index ...")
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
    (mc/ensure-index db "tweets" (array-map :created_at 1)))
  (info "MONGODB - Indexing done!"))


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
  (mc/insert-and-return db (type->coll (:type db-entry)) (assoc (:value db-entry) :ts (t/now))))


(defn find-id [db query]
  (:_id (mc/find-one-as-map db (type->coll (:type query)) (:query query))))

(defn find-entry [db query]
  (mc/find-maps db (type->coll (:type query)) (:query query)))

(defrecord MongoDB [db name opts sa]
  Database
  (transact [this entry] (store (:db this) entry))
  (transact-and-return-id [this entry] (-> (transact this entry)
                                           (from-db-object true)
                                           :_id))
  (retrieve-entry [this query] (find-entry (:db this) query))
  (retrieve-id [this query] (find-id (:db this) query)))


(defn init [& {:keys [name server-address] :as opts} ]
  "config must have keys server-address and name"
  (info "MONGODB - creating connection ...")
  (when-not opts
    (info "MongoDB - using default configuration"))
  (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300}) ;; TODO: better options handling
        ^ServerAddress sa  (mg/server-address (or server-address (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)
        name (or name "juno")
        db (mg/get-db (mg/connect sa opts) name)]
    (info "MONGODB - connected to" name "!")
    (MongoDB. db name opts sa)))


(defn init-log-db [name]
  (let [db (mg/get-db (mg/connect) name)]
    (info "MONGODB - " name " connected!")
    db))


;; --- MONGO DATA EXPORT/IMPORT ---
(defn backup
  "Write backup from given date of a specific collection to a given folder"
  [date database coll folder-path]
  (let [day-after (t/plus date (t/days 1))
        m (str (t/month date))
        d (str (t/day date))
        file-path (str folder-path
                       "/" coll
                       "-" (t/year date)
                       "-" (if (< (count m) 2) (str 0 m) m)
                       "-" (if (< (count d) 2) (str 0 d) d)
                       ".json")]
    (sh "mongoexport"
        "--port" "27017"
        "--host" (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1")
        "--db" database
        "--collection" coll
        "--query" (str "{" (if (= coll "tweets") "created_at" "ts") " : {$gte : new Date(" (c/to-long date) "), $lt : new Date(" (c/to-long day-after) ")}}")
        "--out" file-path)))


(defn backup-yesterday
  "Write last day's collection to specific folder"
  [database coll folder-path]
  (backup (t/minus (t/today) (t/days 1)) database coll folder-path))


(comment

  (def db (:db (init :name "juno")))

  (def log-db (init-log-db "saturn"))

  (create-index db)

  (let [ctimes (->> (mc/find-maps log-db "ctimes")
              (map :time))]
    (reduce (comp float +) (map #(/ % (count ctimes)) ctimes)))

  )
