(ns clj-lmdb.core-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.core :refer :all]))

(deftest simple-init
  (testing "Open an environment with the default options"
    (let [e (env "/tmp")]
      (prn (-> e :db (.stat)))
      (prn (.getEntries (-> e :db (.stat))))
      (is (= 0 (-> e :db (.stat) (.getEntries)))))))
