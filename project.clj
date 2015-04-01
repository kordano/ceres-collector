(defproject ceres-collector "0.1.0-SNAPSHOT"

  :description "FIXME: write description"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [gezwitscher "0.1.1-SNAPSHOT"]

                 [com.novemberain/monger "2.1.0"]

                 [clj-time "0.9.0"]
                 [cheshire "5.4.0"]
                 [enlive "1.1.5"]
                 [clojurewerkz/quartzite "2.0.0"]

                 [aprint "0.1.3"]
                 [com.taoensso/timbre "3.4.0"]]

  :main ceres-collector.core

  :min-lein-version "2.0.0"

  :uberjar-name "ceres-collector-standalone.jar"

  )
