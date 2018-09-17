(defproject urn-parser "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [org.clojure/clojure "1.9.0"]
                 [com.novemberain/pantomime "2.10.0"]
                 [clojurewerkz/elastisch "6.0.0-SNAPSHOT"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [com.taoensso/timbre "4.10.0"]
                 [clj-http "3.9.1"]
                ]
  :main ^:skip-aot urn-parser.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
