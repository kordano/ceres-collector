(ns ceres-collector.geschichte
  (:require [hasch.core :refer [uuid]]
            [konserve.store :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store]]
            [clj-time.core :as t]
            [konserve.protocols :refer [-get-in -assoc-in]]
            #_[geschichte.sync :refer [server-peer client-peer]]
            #_[geschichte.stage :as s]
            #_[geschichte.p2p.fetch :refer [fetch]]
            #_[geschichte.p2p.hash :refer [ensure-hash]]
            #_[geschichte.realize :refer [commit-value]]
            #_[geschichte.p2p.block-detector :refer [block-detector]]
            #_[geschichte.platform :refer [create-http-kit-handler! <!? start stop]]
            [monger.collection :as mc]
            [monger.joda-time]
            [clojure.core.async :refer [>!!]]
            [aprint.core :refer [aprint]]
            [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

#_(defn stop-peer [state]
  (stop (get-in @state [:geschichte :peer])))

(def eval-map
  {'(fn init [_ name]
      {:store name
       :data []})
   (fn [_ name]
     {:store name
      :data []})
   '(fn transact-entry [old entry]
      (update-in old [:data] concat entry))
   (fn [old entry]
     (update-in old [:data] concat entry))})


(defn mapped-eval [code]
  (if (eval-map code)
    (eval-map code)
    (do (debug "eval-map didn't match:" code)
        (fn [old _] old))))

(defn find-fn [name]
  (first (filter (fn [[_ fn-name]]
                   (= name fn-name))
                 (keys eval-map))))

#_(defn init [ & {:keys [user socket repo-name fs-store]}]
  (let [user (or user "kordano@topiq.es")
        socket (or socket "ws://127.0.0.1:31744")
        store (<!? (if fs-store
                     (new-fs-store fs-store)
                     (new-mem-store)))
        peer-server (server-peer (create-http-kit-handler! socket)
                                 store
                                 (comp (partial block-detector :peer-core)
                                       (partial fetch store)
                                       ensure-hash
                                       (partial block-detector :p2p-surface)))
        _ (start peer-server)
        stage (<!? (s/create-stage! user peer-server eval))
        _ (<!? (s/connect! stage socket))
        r-id (<!? (s/create-repo! stage (or repo-name "tweets collection")))]
    (<!? (s/transact stage [user r-id "master"] [[(find-fn 'init) "mem"]]))
    (<!? (s/commit! stage {user {r-id #{"master"}}})) ;; master branch
    {:store store
     :peer peer-server
     :stage stage
     :repo r-id
     :user user}))


#_(defn transact-status
  "Transact incoming status to geschichte and commit"
  [state status]
  (let [{:keys [store peer stage repo user]} (get-in @state [:geschichte])
        db (get-in @state [:log-db])]
    (<!? (s/transact stage [user repo "master"] [[(find-fn 'transact-entry) [status]]]))
    (let [pre-time (System/currentTimeMillis)]
      (<!? (s/commit! stage {user {repo #{"master"}}}))
      (mc/insert db "ctimes" {:time (- (System/currentTimeMillis) pre-time)
                              :type "commit"
                              :ts (t/now)}))
   state))


#_(defn get-current-state
  "Realize head of current geschichte master branch"
  [state]
  (let [{:keys [store peer stage repo user]} (get-in @state [:geschichte])
        causal-order (get-in @stage [user repo :state :causal-order])
        master-head (first (get-in @stage [user repo :state :branches "master"]))]
    (<!? (commit-value store mapped-eval causal-order master-head))))


(comment

  (def store (<!? (new-mem-store)))

  (def peer (client-peer "archimedes" store (comp (partial fetch store) ensure-hash)))

  (def stage (<!? (s/create-stage! "kordano@topiq.es" peer eval)))

  (<!? (s/connect! stage "ws://127.0.0.1:31744"))


  (def user "kordano@topiq.es")

  (def repo #uuid "ac87a592-1d10-471e-87d3-2f3ad9518129")


  (<!? (s/subscribe-repos! stage {user {repo #{"master"}}}))

  (let [causal-order (get-in @stage [user repo :state :causal-order])
        master-head (first (get-in @stage [user repo :state :branches "master"]))]
    (-> (<!? (commit-value store mapped-eval causal-order master-head))
        (get-in [:data])
        count
        time))


  )
