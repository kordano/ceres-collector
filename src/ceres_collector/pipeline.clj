(ns ceres-collector.pipeline
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
            [taoensso.timbre :as timbre])
  (:import org.bson.types.ObjectId))

(timbre/refer-timbre)

(def news-accounts #{"FAZ_NET" "dpa" "tagesschau" "SPIEGELONLINE" "SZ" "BILD" "DerWesten" "ntvde" "tazgezwitscher" "welt" "ZDFheute" "N24_de" "sternde" "focusonline"} )

(defn expand-url
  "Expands shortened url strings, thanks to http://www.philippeadjiman.cod/blog/2009/09/07/the-trick-to-write-a-fast-universal-java-url-expander/"
  [url-str]
  (let [url (java.net.URL. url-str)
        conn (try (.openConnection url)
                  (catch Exception e (do (error (str e))
                                         false)))]
    (if conn
      (do (.setInstanceFollowRedirects conn false)
          (try
            (do
              (.connect conn)
              (let [expanded-url (.getHeaderField conn "Location")
                    content-type (.getContentType conn)]
                (try
                  (do (.close (.getInputStream conn))
                      {:url expanded-url
                       :content-type content-type})
                  (catch Exception e (do (error (str e))
                                         {:url :not-available
                                          :content-type content-type})))))
            (catch Exception e (do (error (str e))
                                   nil))))
      nil)))

(defn get-author-id
  "Find user if exists, else store new user if it's not a mention"
  [{:keys [screen_name id mention] :as user}]
  (if-let [uid (:_id (mc/find-one-as-map @db "users" {:id id}))]
    uid
    (if-not mention
      (d/store-author user)
      nil)))


(defn get-hashtag-id
  "Find hashtag if if exists, else store new hashtag"
  [text]
  (if-let [hid (:_id (mc/find-one-as-map @db "tags" {:text text}))]
    hid
    (d/store-hashtag text)))


(defn get-url-id
  "Get url id if exists otherwise store url"
  [url news?]
  (if-let [url-id (:_id (mc/find-one-as-map @db "urls" {:path url}))]
    url-id
    (if-let [expanded-url (expand-url url)]
      (if-let [x-url-id (:_id (mc/find-one-as-map @db "urls" {:path (:url expanded-url)}))]
        x-url-id
        (let [new-url-id (d/store-url (:url expanded-url))]
          (when news?
            (d/store-html (:url expanded-url) new-url-id))
          new-url-id))
      (let [new-url-id (d/store-url url)]
          (when news?
            (d/store-html url new-url-id))
          new-url-id))))


(defn get-type
  "Dispatches tweet type"
  [{:keys [in_reply_to_status_id retweeted_status entities]}]
  (if in_reply_to_status_id
    :reply
    (if retweeted_status
      :retweet
      (if-not (empty? (:urls entities))
        :share
        :unrelated))))


(defn start
  "Start the pipeline storing everything and everyone"
  [status]
  (let [{:keys [id user entities text id _id] :as tweet} (d/store-tweet status)
        news? (news-accounts (:screen_name user))
        mid (d/store-message text _id id)
        aid (get-author-id user)
        _ (d/store-reference aid mid "pub")
        hids (doall (map (fn [{:keys [text]}] (get-hashtag-id text)) (:hashtags entities)))
        url-ids (doall (map #(get-url-id (:expanded_url %) news?) (:urls entities)))
        me-ids (doall (map #(get-author-id (assoc % :mention true)) (:user_mentions entities)))
        type (get-type tweet)]
    (when news?
      (doall (map #(d/store-reference % mid "source") url-ids)))
    (doall (map #(d/store-reference mid % "mention") me-ids))
    (doall (map #(d/store-reference mid % "url") url-ids))
    (doall (map #(d/store-reference mid % "tag") hids))
    (case type
      :reply (let [sid (mc/find-one-as-map @db "messages" {:tid (:in_reply_to_status_id tweet)})]
               (d/store-reference mid sid "reply"))
      :retweet (let [sid (mc/find-one-as-map @db "messages" {:tid (get-in tweet [:retweeted_status :id])})]
                 (d/store-reference mid sid "retweet"))
      :share (if news?
               nil
               (let [sids (doall (map #(:_id (mc/find-one-as-map @db "refs" {:source %})) url-ids))]
                 (doall (map #(d/store-reference mid % "share") sids))))
      :unrelated (d/store-reference mid nil "unrelated"))))
