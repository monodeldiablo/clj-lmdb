(defproject clj-lmdb "0.3.2"
  :description "Clojure bindings for lmdb"
  :url "http://github.com/shriphani/clj-lmdb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [byte-streams "0.2.2"]
                 [gloss "0.2.6"]
                 [org.deephacks.lmdbjni/lmdbjni "0.4.6"]
                 [org.deephacks.lmdbjni/lmdbjni-linux64 "0.4.6"]
                 [org.deephacks.lmdbjni/lmdbjni-win64 "0.4.6"]
                 [org.deephacks.lmdbjni/lmdbjni-osx64 "0.4.6"]])
