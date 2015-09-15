(ns ceres-collector.processor
  (:refer-clojure :exclude [sort find])
  (:require [net.cgrand.enlive-html :as enlive]
            [ceres-collector.polymorph :as poly]
            [aprint.core :refer [aprint]]
            [clj-time.format :as f]
            [taoensso.timbre :as timbre])
  (:import [ceres_collector.polymorph DbQuery DbEntry]))

(timbre/refer-timbre)

(def news-accounts #{"FAZ_NET" "dpa" "tagesschau" "SPIEGELONLINE" "SZ" "BILD" "DerWesten" "ntvde" "tazgezwitscher" "welt" "ZDFheute" "N24" "sternde" "focusonline"} )

(def news-ids #{114508061 18016521 5734902 40227292 2834511 9204502 15071293 19232587 15243812 8720562 1101354170 15738602 18774524 5494392})
(def custom-formatter (f/formatter "E MMM dd HH:mm:ss Z YYYY"))

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
  [db {:keys [screen_name id mention] :as user}]
  (if-let [uid (poly/retrieve-id db (DbQuery. :user {:id id}))]
    uid
    (if-not mention
      (poly/transact-and-return-id db (DbEntry. :user user))
      nil)))


(defn get-hashtag-id
  "Find hashtag if if exists, else store new hashtag"
  [db text]
  (if-let [hid (poly/retrieve-id db (DbQuery. :tag {:text text}))]
    hid
    (poly/transact-and-return-id db (DbEntry. :tag {:text text}))))


(defn get-url-id
  "Get url id if exists otherwise store url"
  [db url news?]
  (if-let [url-id (poly/retrieve-id db (DbQuery. :url {:path url}))]
    url-id
    (if-let [expanded-url (expand-url url)]
      (if-let [x-url-id (poly/retrieve-id db (DbQuery. :url {:path (:url expanded-url)}))]
        x-url-id
        (let [new-url-id (poly/transact-and-return-id db (DbEntry. :url {:path (:url expanded-url)}))]
          (when news?
            (let [raw-html (if (= (:url expanded-url) :not-available)
                             nil
                             (slurp (:url expanded-url)))]
              (poly/transact db (DbEntry. :html {:raw raw-html :url new-url-id}))))
          new-url-id))
      (let [new-url-id (poly/transact-and-return-id db (DbEntry. :url {:path url}))]
        (when news?
          (let [raw-html (if (= url :not-available) nil (slurp url))]
              (poly/transact db (DbEntry. :html {:raw raw-html :url new-url-id}))))
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


(defn process [db status]
  (let [{:keys [id user entities text] :as tweet} (update-in status [:created_at] (fn [x] (f/parse custom-formatter x)))
        _id (poly/transact-and-return-id db (DbEntry. :tweet tweet))
        news? (news-ids (:id user))
        mid (poly/transact-and-return-id db (DbEntry. :message {:text text :tweet _id :tid id}))
        aid (get-author-id db user)
        _ (poly/transact db (DbEntry. :pub {:source aid :target mid}))
        hids (doall (map (fn [{:keys [text]}] (get-hashtag-id db text)) (:hashtags entities)))
        url-ids (doall (map #(get-url-id db (:expanded_url %) news?) (:urls entities)))
        me-ids (doall (map #(get-author-id db (assoc % :mention true)) (:user_mentions entities)))
        type (get-type tweet)]
    (when news?
      (doall (map #(poly/transact db (DbEntry. :source {:source % :target mid})) url-ids)))
    (doall (map #(poly/transact db (DbEntry. :mention {:source mid :target %})) me-ids))
    (doall (map #(poly/transact db (DbEntry. :urlref {:source mid :target %})) url-ids))
    (doall (map #(poly/transact db (DbEntry. :tagref {:source mid :target %})) hids))
    (case type
      :reply (let [sid  (poly/retrieve-id db (DbQuery. :message {:tid (:in_reply_to_status_id tweet)}))]
               (poly/transact db (DbEntry. :reply {:source mid :target sid})))
      :retweet (let [sid (poly/retrieve-id db (DbQuery. :message {:tid (get-in tweet [:retweeted_status :id])}))]
                 (poly/transact db (DbEntry. :retweet {:source mid :target sid})))
      :share (if news?
               nil
               (let [sids (doall (map #(:target (first (poly/retrieve-entry db (DbQuery. :source {:source %})))) url-ids))]
                 (doall (map #(poly/transact db (DbEntry. :share {:source mid :target %})) sids))))
      :unrelated (poly/transact db (DbEntry. :unknown {:source mid})))))
