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
  [source target type]
  (mc/insert-and-return
   @db
   "references"
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
  [{:keys [url content-type] :as expanded-url} url-id]
  (let [raw-html (if (= url :not-available)
                   nil
                   (slurp url))]
    (mc/insert-and-return
     @db
     "htmls"
     {:raw raw-html
      :ts (t/now)
      :url url-id})))


(comment

  (set-db "demeter")

  ;; migration db
  (def db2 (let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300)
                 ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
             (mg/get-db (mg/connect sa opts) "athena")))


  ;; migrate
  (->> (mc/find-maps db2 "publications")
       (pmap (fn [{:keys [url tweet user hashtags type]}]
               (let [text (:text (mc/find-map-by-id @db "tweets" tweet))
                     mid (-> (store-message text (= type "source") tweet db2)
                             from-db-object
                             :_id)]
                 (map #(store-reference mid % "tag" db2) hashtags)
                 (store-reference mid user "pub" db2)
                 (when url (store-reference mid url ))))))

  (->> (mc/find-maps @db "reactions")
       (pmap
        (fn [{:keys [source publication]}]
          (let [{stid :tweet} (mc/find-map-by-id @db "publications" source)
                {tid :tweet t :type} (mc/find-map-by-id @db "publications" publication)
                smid (mc/find-one-as-map db2 "messages" {:tweet stid})
                mid (mc/find-one-as-map db2 "messages" {:tweet tid})]
            (store-reference mid smid t db2)))))

  )
