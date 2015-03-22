(ns ceres-collector.core
  (:gen-class :main true)
  (:require [ceres-collector.db :refer [set-db init-mongo]]
            [ceres-collector.pipeline :as pipeline]
            [ceres-collector.scheduler :refer [start-scheduler]]
            [ceres-collector.processor :as proc]
            [ceres-collector.geschichte :as geschichte]
            [ceres-collector.mongo :as mongo]
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
    (set-db (-> @state :app :db))
    #_(swap! state assoc :geschichte (geschichte/init :user "kordano@topiq.es" :repo "tweet collection"))
    (debug @state)))



(defn start-stream [state]
  (let [{:keys [follow track credentials]} (:app @state)
        [in out] (gezwitscher credentials)]
    (>!! in {:topic :start-stream :track track :follow follow})
    (let [output (<!! out)]
      (go-loop [status (<! (:status-ch output))]
        (when status
          #_(println (:text status))
          (pipeline/start status)
          (recur (<! (:status-ch output))))))
    [in out]))


(defn -main [config-path & args]
  (initialize server-state config-path)
  (timbre/set-config! [:appenders :spit :enabled?] true)
  (timbre/set-config! [:shared-appender-config :spit-filename] (:logfile @server-state))
  (info "Starting twitter collector...")
  (when (:init? @server-state) (init-mongo))
  (let [{{:keys [follow track credentials]} :app} @server-state
        db (mongo/init :name "juno")]
    (start-filter-stream
     follow
     track
     (fn [status] (proc/process db status))
     credentials))
  (when (:backup? @server-state)
    (start-scheduler (:backup-folder @server-state))))


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
      last
      :text
      )


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
