(defproject ceres-collector "0.1.0-SNAPSHOT"

  :description "FIXME: write description"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.json "0.2.4"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [clj-time "0.7.0"]
                 [cheshire "5.3.1"]
                 [enlive "1.1.5"]
                 [gezwitscher "0.1.1-SNAPSHOT"]
                 [com.novemberain/monger "2.0.0-rc1"]
                 [clojurewerkz/quartzite "1.3.0"]
                 [aprint "0.1.0"]
                 [com.taoensso/timbre "3.2.1"]]

  :main ceres-collector.core

  :min-lein-version "2.0.0"

  :uberjar-name "ceres-collector-standalone.jar"

  )
