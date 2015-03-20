(ns ceres-collector.geschichte
  (:require [hasch.core :refer [uuid]]
            [konserve.store :refer [new-mem-store]]
            [konserve.protocols :refer [-get-in -assoc-in]]
            [geschichte.sync :refer [server-peer client-peer]]
            [geschichte.stage :as s]
            [geschichte.p2p.fetch :refer [fetch]]
            [geschichte.p2p.hash :refer [ensure-hash]]
            [geschichte.realize :refer [commit-value]]
            [geschichte.p2p.block-detector :refer [block-detector]]
            [geschichte.platform :refer [create-http-kit-handler! <!? start stop]]
            [clojure.core.async :refer [>!!]]
            [aprint.core :refer [aprint]]
            [taoensso.timbre :as timbre]))

(timbre/refer-timbre)


(def eval-map
  {'(fn init [_ name]
      (atom {:store name
             :data []}))
   (fn [_ name]
     (atom {:store name
             :data []}))
   '(fn transact-entry [old entry]
      (swap! old #(update-in % [:data] concat entry))
      old)
   (fn [old entry]
     (swap! old #(update-in % [:data] concat entry))
     old)})


(defn mapped-eval [code]
  (if (eval-map code)
    (eval-map code)
    (do (debug "eval-map didn't match:" code)
        (throw code))))

(defn find-fn [name]
  (first (filter (fn [[_ fn-name]]
                   (= name fn-name))
                 (keys eval-map))))

(defn init [ & {:keys [user socket repo-name]}]
  (let [user (or user "kordano@topiq.es")
        socket (or socket "ws://127.0.0.1:31744")
        store (<!? (new-mem-store))
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


(defn transact-status
  "Transact incoming status to geschichte and commit"
  [state status]
  (let [{:keys [store peer stage repo user]} (get-in @state :geschichte)]
    (<!? (s/transact stage [user repo "master"]
                     [[(find-fn 'transact-entry)
                       [status]]]))
    (<!? (s/commit! stage {user {repo #{"master"}}}))
   state))

(defn get-current-state
  "Realize head of current geschichte master branch"
  [state]
  (let [{:keys [store peer stage repo user]} (get-in @state :geschichte)]
    (<!? (commit-value store mapped-eval (get-in @stage [user repo :state :causal-order])    (first (get-in @stage [user repo :state :branches "master"]))))
    state))

(comment

  (def store (<!? (new-mem-store)))

  (def peer-server (server-peer (create-http-kit-handler! "ws://127.0.0.1:31744") ;; TODO client-peer?
                                 store
                                 (comp (partial block-detector :peer-core)
                                       (partial fetch store)
                                       ensure-hash
                                       (partial block-detector :p2p-surface))))

  (start peer-server)

  (stop peer-server)


  (def stage (<!? (s/create-stage! "kordano@topiq.es" peer-server eval)))

  (<!? (s/connect! stage "ws://127.0.0.1:31744"))

  (def r-id (<!? (s/create-repo! stage "tweets collection")))

  (<!? (s/transact stage ["kordano@topiq.es" r-id "master"]
                   [[(find-fn 'init)
                     "mem"]]))

  (<!? (s/commit! stage {"kordano@topiq.es" {r-id #{"master"}}}))

  (<!? (s/transact stage ["kordano@topiq.es" r-id "master"]
                   [[(find-fn 'transact-entry)
                     [{:user "eve" :message "earth"}]]]))

  (<!? (s/commit! stage {"kordano@topiq.es" {r-id #{"master"}}}))

  (aprint (<!? (commit-value store mapped-eval (get-in @stage ["kordano@topiq.es" r-id :state :causal-order])    (first (get-in @stage ["kordano@topiq.es" r-id :state :branches "master"])))))

  (<!? (s/transact stage ["kordano@topiq.es" r-id "master"]
                   [[(find-fn 'transact-entry)
                     [{:user "adam" :message "wind"}
                      {:user "snake" :message "fire"}]]]))

  (<!? (s/commit! stage {"kordano@topiq.es" {r-id #{"master"}}}))

  (aprint (<!? (commit-value store mapped-eval (get-in @stage ["kordano@topiq.es" r-id :state :causal-order])    (first (get-in @stage ["kordano@topiq.es" r-id :state :branches "master"])))))

  )
