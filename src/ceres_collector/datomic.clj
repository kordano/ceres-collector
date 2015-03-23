(ns ceres-collector.datomic
  (:require [datomic.api :as d]
            [taoensso.timbre :as timbre :refer [debug info error]]
            [ceres-collector.polymorph :refer [Database transact]])
  (:import [ceres_collector.polymorph DbEntry DbQuery]))

(timbre/refer-timbre)

(defrecord Datomic [uri conn schema]
  Database
  (transact [this entry]))

#_(defn init-schema [conn path]
  (d/transact conn (-> path slurp read-string)))


#_(defn init [& {:keys [uri schema] :as opts}]
  (info "DATOMIC - creating connection ...")
  (let [db-uri (or uri (str "datomic:mem://" (d/squuid)))
        _ (when-not uri
            (d/delete-database db-uri)
            (info "DATOMIC - using in-mem database"))
        _ (d/create-database db-uri)
        conn (d/connect db-uri)
        _ (when-not uri
            (info "DATOMIC - transacting schema " schema)
            (d/transact conn (-> schema slurp read-string)))]
    (info "DATOMIC - connected!")
    (Datomic. db-uri conn schema)))
