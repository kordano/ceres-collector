(ns ceres-collector.scheduler
  (:require [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j]
            [clojurewerkz.quartzite.conversion :as qc]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.schedule.daily-interval :refer [schedule time-of-day every-day starting-daily-at ending-daily-at]]
            [ceres-collector.mongo :as mongo]
            [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

(defmacro create-backup-job
  "Create a defjob for scheduling."
  [coll]
  `(defjob ~(symbol (str coll "Backup")) [ctx#]
     (info "- BACKUP -" ~coll)
     (let [path# (get (qc/from-job-data ctx#) "folder-path")]
       (mongo/backup-yesterday "juno" ~(clojure.string/lower-case coll) path#))))

(defn backup-schedule
  "Create schedule for a given collection, storing path, collection offset and job type."
  [coll path offset jtype]
  (let [job (j/build
               (j/of-type jtype)
               (j/using-job-data {"folder-path" path})
               (j/with-identity (j/key (str "jobs." (clojure.string/lower-case coll) ".backup.1"))))
         tk (t/key (str (clojure.string/lower-case coll)".triggers." offset))
         trigger (t/build
                   (t/with-identity tk)
                   (t/start-now)
                   (t/with-schedule
                     (schedule
                      (every-day)
                      (starting-daily-at (time-of-day 3 (* 3 offset) 0))
                      (ending-daily-at (time-of-day 3 (* 3 offset) 1)))))]
     (qs/schedule job trigger)))


(defn start
  "Run the schedules"
  [path]
  (let [jobs [["Tweets" (create-backup-job "Tweets")]
              ["Users" (create-backup-job "Users")]
              ["Htmls" (create-backup-job "Htmls")]
              ["Urls" (create-backup-job "Urls")]
              ["Tags" (create-backup-job "Tags")]
              ["Messages" (create-backup-job "Messages")]
              ["Mentions" (create-backup-job "Mentions")]
              ["Sources" (create-backup-job "Sources")]
              ["Retweets" (create-backup-job "Retweets")]
              ["Replies" (create-backup-job "Replies")]
              ["Pubs" (create-backup-job "Pubs")]
              ["Shares" (create-backup-job "Shares")]
              ["Urlrefs" (create-backup-job "Urlrefs")]
              ["Tagrefs" (create-backup-job "Tagrefs")]
              ["Unknown" (create-backup-job "Unknown")]]]
    jobs
    (qs/initialize)
    (qs/start)
    (doall
     (map #(backup-schedule (first (get jobs %)) path % (second (get jobs %))) (range (count jobs))))))
