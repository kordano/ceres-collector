(ns ceres-collector.core
  (:gen-class :main true)
  (:require [ceres-collector.processor :as proc]
            [ceres-collector.mongo :as mongo]
            [ceres-collector.geschichte :as geschichte]
            [ceres-collector.scheduler :as scheduler]
            [gezwitscher.core :refer [start-filter-stream gezwitscher]]
            [konserve.protocols :refer [-get-in]]
            [clojure.java.io :as io]
            [clojure.core.async :refer [close! put! timeout sub chan <!! >!! <! >! go go-loop] :as async]
            [taoensso.timbre :refer [info debug error warn] :as timbre]))

(timbre/refer-timbre)


(defn initialize-state
  "Initialize the server state using a given config file"
  [path]
  (let [config (-> path slurp read-string)
        _ nil #_(assoc :geschichte (geschichte/init :user "kordano@topiq.es" :repo "tweet collection" :fs-store "/data/ceres/konserve")) ;; todo: into config file
        _ nil #_(assoc :log-db (mongo/init-log-db "saturn"))]
    (debug "CORE - STATE:" config)
    (atom config)))


(defn -main [config-path & args]
  (info "CORE - Warming up...")
  (let [state (initialize-state config-path)]
    (timbre/set-config! [:appenders :spit :enabled?] true)
    (timbre/set-config! [:shared-appender-config :spit-filename] (:logfile @state))
    (let [{{:keys [follow track credentials]} :app} @state
          db (mongo/init :name "juno")]
      (do
        (when (:init? @state)
          (mongo/create-index (:db db)))
        (start-filter-stream follow track (fn [status] (proc/process db status)) credentials)))
    (when (:backup? @state)
      (scheduler/start-jobs (:backup-folder @state)))
    (info "CORE - server started!")))



(comment

  (def state (initialize-state "opt/test-config.edn"))

  (scheduler/start-jobs (:backup-folder @state))

  (def stop-stream
    (let [{{:keys [follow track credentials]} :app} @state
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
