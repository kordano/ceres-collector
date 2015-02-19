(ns ceres-collector.core
  (:gen-class :main true)
  (:require [ceres-collector.db :refer [store-raw-tweet set-db init-mongo]]
            [ceres-collector.scheduler :refer [start-scheduler]]
            [gezwitscher.core :refer [start-filter-stream gezwitscher]]
            [clojure.java.io :as io]
            [clojure.core.async :refer [close! put! timeout sub chan <!! >!! <! >! go go-loop] :as async]
            [taoensso.timbre :refer [info debug error warn] :as timbre]))

(timbre/refer-timbre)

(def server-state (atom nil))


(defn initialize
  "Initialize the server state using a given config file"
  [state path]
  (do
    (reset!
     state
     (-> path slurp read-string
         (assoc-in [:app :out-chans] [])
         (assoc-in [:app :recent-tweets] [])
         (assoc-in [:app :recent-articles] [])))
    (set-db (-> @state :app :db))))


(defn start-stream [state]
  (let [{:keys [follow track credentials]} (:app @state)
        [in out] (gezwitscher credentials)]
    (>!! in {:topic :start-stream :track track :follow follow})
    (let [output (<!! out)]
      (go-loop [status (<! (:status-ch output))]
        (when status
          #_(println (:text status))
          (store-raw-tweet status)
          (recur (<! (:status-ch output))))))
    [in out]))


(defn -main [config-path & args]
  (initialize server-state config-path)
  (timbre/set-config! [:appenders :spit :enabled?] true)
  (timbre/set-config! [:shared-appender-config :spit-filename] (:logfile @server-state))
  (info "Starting twitter collector...")
  (when (:init? @server-state) (init-mongo))
  (info @server-state)
  (let [{{:keys [follow track credentials]} :app} @server-state]
    (start-filter-stream follow track store-raw-tweet credentials))
  (when (:backup? @server-state)
    (start-scheduler (:backup-folder @server-state))))

(comment

  (initialize server-state "opt/test-config.edn")

  (def stop-stream
    (let [{{:keys [follow track credentials]} :app} @server-state]
      (start-filter-stream follow track store-raw-tweet credentials)))

  (stop-stream)

  (def g (start-stream server-state))

  (>!! (first g) {:topic :stop-stream})

)
