(ns ceres-collector.migrator
  (:refer-clojure :exclude [sort find])
  (:require [clojure.java.shell :refer [sh]]
            [ceres-collector.db :refer [db news-accounts custom-formatter time-interval]]
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
  (mc/insert-and-return
   @db
   "hashtags-2"
   {:text text
    :ts (t/now)}))


(defn store-author [{:keys [screen_name id created_at]}]
  (let [date (f/parse custom-formatter created_at)]
    (mc/insert-and-return
     @db
     "authors"
     {:id id
      :name screen_name
      :created_at date
      :ts (t/now)})))


(defn store-url [url]
  (mc/insert-and-return
   @db
   "urls-2"
   {:path url
    :ts (t/now)}))
