(ns ceres-collector.migration
  (:refer-clojure :exclude [sort find])
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]]
            [monger.query :refer :all]
            [monger.joda-time]
            [net.cgrand.enlive-html :as enlive]
            [aprint.core :refer [aprint]]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.periodic :as p]
            [ceres-collector.db :refer [db custom-formatter] :as d]
            [taoensso.timbre :as timbre]))



(defn store-ref [{:keys [source target ts]} coll]
  (mc/insert
   @db
   coll
   {:source source
    :target target
    :ts ts}))


(comment

  (let [refs (mc/find-maps @db "refs")
        refs->coll {"mention" "mentions"
                    "pub" "pubs"
                    "reply" "replies"
                    "retweet" "retweets"
                    "share" "shares"
                    "source" "sources"
                    "tag" "tagrefs"
                    "url" "urlrefs"
                    "unrelated" "unknown"}]
    (doall
     (pmap
      (fn [{:keys [type] :as entry}]
        (store-ref entry (refs->coll type)))
      refs)))

  )
