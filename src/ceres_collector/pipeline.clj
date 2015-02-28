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
            [ceres-collector.db :refer [db custom-formatter news-accounts]]
            [ceres-collector.migrator :as d]
            [taoensso.timbre :as timbre])
  (:import org.bson.types.ObjectId))

(timbre/refer-timbre)

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
  (if-let [uid (:_id (mc/find-one-as-map @db "users" {:id (:id user)}))]
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


(defn get-source-id
  "Find source message id of a retweet or reply"
  [source-tid db]
  (if-let [source (mc/find-one-as-map db "messages" {:tid source-tid})]
    (:_id source)
    nil))



(defn get-url-id
  "Get url id if exists otherwise store url"
  [url uid tid ts source?]
  (if-let [url-id (:_id (mc/find-one-as-map @db "urls" {:url url}))]
    url-id
    (if-let [expanded-url (expand-url url)]
      (if-let [x-url-id (:_id (mc/find-one-as-map @db "urls" {:url (:url expanded-url)}))]
        x-url-id
        (if source?
          (let [new-url-id (d/store-url (:url expanded-url))]
            (do
              (d/store-html expanded-url new-url-id)
              new-url-id))
          nil))
      nil)))


(defn get-type
  "Dispatches tweet type"
  [{:keys [in_reply_to_status_id retweeted_status entities]}]
  (if in_reply_to_status_id
    :reply
    (if retweeted_status
      :retweet
      (if-not (empty? (:urls entities))
        :source-or-share
        :unrelated))))


(defn start [status]
  (let [oid (ObjectId.)
        doc (update-in status [:created_at] (fn [x] (f/parse custom-formatter x)))
        {:keys [id user entities retweeted_status in_reply_to_status_id created_at _id text]
         :as record} (from-db-object (mc/insert-and-return @db "tweets" (merge doc {:_id oid})) true)
        mid (d/store-message text _id id db)
        aid (get-author-id (:user record))
        hids (doall (map (fn [{:keys [text]}] (get-hashtag-id text db)) (:hashtags entities)))
        url-ids (doall (map get-url-id (:urls entities)))
        type (get-type record)
        source? (news-accounts (:screen_name user))]
    record))
