(ns ceres-collector.core
  (:gen-class :main true)
  (:require [ceres-collector.processor :as proc]
            [ceres-collector.mongo :as mongo]
            [ceres-collector.scheduler :as scheduler]
            [gezwitscher.core :refer [start-filter-stream gezwitscher]]
            [clojure.java.io :as io]
            [taoensso.timbre :refer [info debug error warn] :as timbre]))

(timbre/refer-timbre)


(defn initialize-state
  "Initialize the server state using a given config file"
  [path]
  (let [config (-> path slurp read-string)]
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
        (when (:init? @state) (mongo/create-index (:db db)))
        (start-filter-stream follow track (fn [status] (proc/process db status)) credentials)))
    (when (:backup? @state)
      (scheduler/start-jobs (:backup-folder @state)))
    (info "CORE - server started!")))



(comment

  (def state (initialize-state "opt/test-config.edn"))

  (def stop-stream
    (let [{{:keys [follow track credentials]} :app} @state
          db (mongo/init :name "juno")]
      (start-filter-stream
       follow
       track
       (fn [status]
         (proc/process db status))
       credentials)))

  (stop-stream)

  )
