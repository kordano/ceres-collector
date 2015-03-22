(ns ceres-collector.core
  (:gen-class :main true)
  (:require [ceres-collector.processor :as proc]
            [ceres-collector.mongo :as mongo]
            [ceres-collector.geschichte :as geschichte]
            [ceres-collector.scheduler :as scheduler]
            [gezwitscher.core :refer [start-filter-stream gezwitscher]]
            [clojure.java.io :as io]
            [clojure.core.async :refer [close! put! timeout sub chan <!! >!! <! >! go go-loop] :as async]
            [taoensso.timbre :refer [info debug error warn] :as timbre]))

(timbre/refer-timbre)

(def server-state (atom nil))

(defn initialize
  "Initialize the server state using a given config file"
  [state path]
  (do (reset! state (-> path slurp read-string))
      (swap! state assoc :geschichte (geschichte/init :user "kordano@topiq.es" :repo "tweet collection"))
      (debug @state)))


(defn -main [config-path & args]
  (initialize server-state config-path)
  (timbre/set-config! [:appenders :spit :enabled?] true)
  (timbre/set-config! [:shared-appender-config :spit-filename] (:logfile @server-state))
  (info "Starting twitter collector...")
  (when (:init? @server-state)
    (mongo/create-index))
  (let [{{:keys [follow track credentials]} :app} @server-state
        db (mongo/init :name "juno")]
    (start-filter-stream follow track (fn [status] (proc/process db status)) credentials))
  (when (:backup? @server-state)
    (scheduler/start (:backup-folder @server-state))))



(comment

  (initialize server-state "opt/test-config.edn")

  (def geschichte-stream
    (let [{{:keys [follow track credentials]} :app} @server-state]
      (start-filter-stream
       follow
       track
       (fn [status] (geschichte/transact-status server-state status))
       credentials)))


  (do
    (geschichte-stream)
    (geschichte/stop-peer server-state))

  (-> (geschichte/get-current-state server-state)
      deref
      (get-in [:data])
      count)


  (def stop-stream
    (let [{{:keys [follow track credentials]} :app} @server-state
          db (mongo/init :name "juno")]
      (debug @server-state)
      (debug db)
      (start-filter-stream
       follow
       track
       (fn [status]
         (debug "STATUS - " (str "@" (get-in status [:user :screen_name])) ":"  (:text status))
         (proc/process db status))
       credentials)))


  (stop-stream)

  )
