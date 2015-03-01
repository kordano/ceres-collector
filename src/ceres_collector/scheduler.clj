(ns ceres-collector.scheduler
  (:require [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j]
            [clojurewerkz.quartzite.conversion :as qc]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.schedule.daily-interval :refer [schedule time-of-day every-day starting-daily-at ending-daily-at]]
            [ceres-collector.db :as db]
            [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

(defjob HtmlsBackup [ctx]
  (let [path (get (qc/from-job-data ctx) "folder-path")]
      (info "Writing htmls backup...")
    (db/backup-yesterday "demeter" "htmls" path)))

(defjob TweetBackup [ctx]
  (let [path (get (qc/from-job-data ctx) "folder-path")]
      (info "Writing tweets backup...")
      (db/backup-yesterday "demeter" "tweets" path)))

(defjob RefsBackup [ctx]
  (let [path (get (qc/from-job-data ctx) "folder-path")]
      (info "Writing refs backup...")
    (db/backup-yesterday "demeter" "refs" path)))

(defjob UrlsBackup [ctx]
  (let [path (get (qc/from-job-data ctx) "folder-path")]
      (info "Writing urls backup...")
      (db/backup-yesterday "demeter" "urls" path)))

(defjob UsersBackup [ctx]
  (let [path (get (qc/from-job-data ctx) "folder-path")]
      (info "Writing users backup...")
    (db/backup-yesterday "demeter" "users" path)))

(defjob MessagesBackup [ctx]
  (let [path (get (qc/from-job-data ctx) "folder-path")]
      (info "Writing messages backup...")
      (db/backup-yesterday "demeter" "messages" path)))

(defjob HashtagsBackup [ctx]
  (let [path (get (qc/from-job-data ctx) "folder-path")]
      (info "Writing tags backup...")
      (db/backup-yesterday "demeter" "tags" path)))


;; --- Schedules ---

(defn tweets-backup-schedule
  "Create a schedule to backup the tweets at 3 am"
  [path]
  (let [job (j/build
             (j/of-type TweetBackup)
             (j/using-job-data {"folder-path" path})
             (j/with-identity (j/key "jobs.tweetsbackup.1")))
        tk (t/key "triggers.1")
        trigger (t/build
                 (t/with-identity tk)
                 (t/start-now)
                 (t/with-schedule
                   (schedule
                    (every-day)
                    (starting-daily-at (time-of-day 3 00 00))
                    (ending-daily-at (time-of-day 3 00 01)))))]
    (qs/schedule job trigger)))


(defn htmls-backup-schedule
  "Create a schedule to backup the htmls at 3.05 am"
  [path]
  (let [job (j/build
             (j/of-type HtmlsBackup)
             (j/using-job-data {"folder-path" path})
             (j/with-identity (j/key "jobs.htmlsbackup.1")))
        tk (t/key "triggers.2")
        trigger (t/build
                 (t/with-identity tk)
                 (t/start-now)
                 (t/with-schedule
                   (schedule
                    (every-day)
                    (starting-daily-at (time-of-day 3 05 00))
                    (ending-daily-at (time-of-day 3 05 01)))))]
    (qs/schedule job trigger)))


(defn refs-backup-schedule
  "Create a schedule to backup the refs at 3.10 am"
  [path]
  (let [job (j/build
             (j/of-type RefsBackup)
             (j/using-job-data {"folder-path" path})
             (j/with-identity (j/key "jobs.refsbackup.1")))
        tk (t/key "triggers.3")
        trigger (t/build
                 (t/with-identity tk)
                 (t/start-now)
                 (t/with-schedule
                   (schedule
                    (every-day)
                    (starting-daily-at (time-of-day 3 10 00))
                    (ending-daily-at (time-of-day 3 10 01)))))]
    (qs/schedule job trigger)))



(defn urls-backup-schedule
  "Create a schedule to backup the urls at 3.20 am"
  [path]
  (let [job (j/build
             (j/of-type UrlsBackup)
             (j/using-job-data {"folder-path" path})
             (j/with-identity (j/key "jobs.urlsbackup.1")))
        tk (t/key "triggers.5")
        trigger (t/build
                 (t/with-identity tk)
                 (t/start-now)
                 (t/with-schedule
                   (schedule
                    (every-day)
                    (starting-daily-at (time-of-day 3 20 00))
                    (ending-daily-at (time-of-day 3 20 01)))))]
    (qs/schedule job trigger)))


(defn users-backup-schedule
  "Create a schedule to backup the users at 3.25 am"
  [path]
  (let [job (j/build
             (j/of-type UsersBackup)
             (j/using-job-data {"folder-path" path})
             (j/with-identity (j/key "jobs.usersbackup.1")))
        tk (t/key "triggers.6")
        trigger (t/build
                 (t/with-identity tk)
                 (t/start-now)
                 (t/with-schedule
                   (schedule
                    (every-day)
                    (starting-daily-at (time-of-day 3 25 00))
                    (ending-daily-at (time-of-day 3 25 01)))))]
    (qs/schedule job trigger)))


(defn messages-backup-schedule
  "Create a schedule to backup the messages at 3.30 am"
  [path]
  (let [job (j/build
             (j/of-type MessagesBackup)
             (j/using-job-data {"folder-path" path})
             (j/with-identity (j/key "jobs.messagesbackup.1")))
        tk (t/key "triggers.7")
        trigger (t/build
                 (t/with-identity tk)
                 (t/start-now)
                 (t/with-schedule
                   (schedule
                    (every-day)
                    (starting-daily-at (time-of-day 3 30 00))
                    (ending-daily-at (time-of-day 3 30 01)))))]
    (qs/schedule job trigger)))


(defn hashtags-backup-schedule
  "Create a schedule to backup the hashtags at 3.35 am"
  [path]
  (let [job (j/build
             (j/of-type HashtagsBackup)
             (j/using-job-data {"folder-path" path})
             (j/with-identity (j/key "jobs.hashtagsbackup.1")))
        tk (t/key "triggers.8")
        trigger (t/build
                 (t/with-identity tk)
                 (t/start-now)
                 (t/with-schedule
                   (schedule
                    (every-day)
                    (starting-daily-at (time-of-day 3 35 00))
                    (ending-daily-at (time-of-day 3 35 01)))))]
    (qs/schedule job trigger)))


(defn start-scheduler
  "Run the schedules"
  [path]
  (qs/initialize)
  (qs/start)
  (tweets-backup-schedule path)
  (htmls-backup-schedule path)
  (refs-backup-schedule path)
  (urls-backup-schedule path)
  (users-backup-schedule path)
  (messages-backup-schedule path)
  (hashtags-backup-schedule path))
