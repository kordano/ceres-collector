(ns ceres-collector.migrator
  (:refer-clojure :exclude [sort find])
  (:require [clojure.java.shell :refer [sh]]
            [ceres-collector.db :refer [db news-accounts custom-formatter time-interval set-db]]
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


(defn store-reference
  "Store reference between tweet and another entity"
  [source target type db]
  (mc/insert-and-return
   db
   "references"
   {:source source
    :target target
    :type type
    :ts (t/now)}))


(defn store-hashtag [text db]
  (mc/insert-and-return
   db
   "hashtags-2"
   {:text text
    :ts (t/now)}))


(defn store-author [{:keys [screen_name id created_at]} db]
  (let [date (f/parse custom-formatter created_at)]
    (mc/insert-and-return
     db
     "authors"
     {:id id
      :name screen_name
      :created_at date
      :ts (t/now)})))


(defn store-url [url db]
  (mc/insert-and-return
   db
   "urls-2"
   {:path url
    :ts (t/now)}))


(defn store-message
  [text source tid db]
  (mc/insert-and-return
   db
   "messages"
   {:text text
    :source source
    :ts (t/now) :tweet tid}))



(comment

  (set-db "athena")

  ;; migration db
  (def db2 (let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300)
                 ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
             (mg/get-db (mg/connect sa opts) "demeter")))


  ;; migrate users
  (->> (mc/find-maps @db "publications")
       (pmap (fn [{:keys [url tweet user hashtags type]}]
               (let [text (:text (mc/find-map-by-id @db "tweets" tweet))
                     mid (-> (store-message text (= type "source") tweet db2)
                             from-db-object
                             :_id)]
                 (map #(store-reference mid % "tag" db2) hashtags)
                 (store-reference mid user "pub" db2)
                 (when url
                   (store-reference mid url "url" db2)))
                 ))))

  (->> (mc/find-maps @db "publications")
       (take 100)
       (map :type)
       (into #{}))
  )
