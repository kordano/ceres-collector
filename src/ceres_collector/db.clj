(ns ceres-collector.db
  (:refer-clojure :exclude [sort find])
  (:require [clojure.java.shell :refer [sh]]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]]
            [monger.query :refer :all]
            [monger.joda-time]
            [aprint.core :refer [aprint]]
            [net.cgrand.enlive-html :as enlive]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.periodic :as p]
            [taoensso.timbre :as timbre])
  (:import org.bson.types.ObjectId))


(timbre/refer-timbre)

(def db
  (atom
   (let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300)
         ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
     (mg/get-db (mg/connect sa opts) "athena"))))

(def time-interval {$gt (t/date-time 2014 8 1) $lt (t/date-time 2014 9 1)})

(defn set-db [name]
  (let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300)
        ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
    (reset! db (mg/get-db (mg/connect sa opts) name))))

(def custom-formatter (f/formatter "E MMM dd HH:mm:ss Z YYYY"))

(defn init-mongo
  "Define mongodb indices on first start"
  []
  (do
    (mc/ensure-index @db "urls" (array-map :url 1))
    (mc/ensure-index @db "urls" (array-map :ts 1))
    (mc/ensure-index @db "refs" (array-map :source 1))
    (mc/ensure-index @db "refs" (array-map :target 1))
    (mc/ensure-index @db "refs" (array-map :type 1))
    (mc/ensure-index @db "refs" (array-map :ts 1))
    (mc/ensure-index @db "tags" (array-map :text 1))
    (mc/ensure-index @db "tags" (array-map :ts 1))
    (mc/ensure-index @db "message" (array-map :ts 1))
    (mc/ensure-index @db "message" (array-map :text 1))
    (mc/ensure-index @db "message" (array-map :tweet 1))
    (mc/ensure-index @db "message" (array-map :tid 1))
    (mc/ensure-index @db "message" (array-map :ts 1))
    (mc/ensure-index @db "htmls" (array-map :ts 1))
    (mc/ensure-index @db "users" (array-map :id 1))
    (mc/ensure-index @db "users" (array-map :ts 1))
    (mc/ensure-index @db "tweets" (array-map :user.screen_name 1))
    (mc/ensure-index @db "tweets" (array-map :id 1))
    (mc/ensure-index @db "tweets" (array-map :retweeted_status.id 1))
    (mc/ensure-index @db "tweets" (array-map :in_reply_to_status_id 1))
    (mc/ensure-index @db "tweets" (array-map :created_at 1))))


(defn store-reference
  "Store reference between tweet and another entity"
  [source target type]
  (mc/insert-and-return
   @db
   "refs"
   {:source source
    :target target
    :type type
    :ts (t/now)}))


(defn store-hashtag [text]
  (-> (mc/insert-and-return
       @db
       "tags"
       {:text text
        :ts (t/now)})
      (from-db-object true)
      :_id))


(defn store-author [{:keys [screen_name id created_at]}]
  (let [date (f/parse custom-formatter created_at)]
    (-> (mc/insert-and-return
         @db
         "users"
         {:id id
          :name screen_name
          :created_at date
          :ts (t/now)})
        (from-db-object true)
        :_id)))


(defn store-url [url]
  (-> (mc/insert-and-return
       @db
       "urls"
       {:path url
        :ts (t/now)})
      (from-db-object true)
      :_id))


(defn store-message [text source tid]
  (-> (mc/insert-and-return
       @db
       "messages"
       {:text text
        :source source
        :ts (t/now)
        :tweet tid})
      (from-db-object true)
      :_id))


(defn store-html
  "Fetch html document and store raw binary in database"
  [url url-id]
  (let [raw-html (if (= url :not-available) nil (slurp url))]
    (mc/insert-and-return
     @db
     "htmls"
     {:raw raw-html
      :ts (t/now)
      :url url-id})))


(defn store-tweet
  "Stores tweet, parses date"
  [status]
  (let [new-status (update-in status [:created_at] (fn [x] (f/parse custom-formatter x)))]
    (from-db-object
     (mc/insert-and-return @db "tweets" new-status)
     true)))



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
