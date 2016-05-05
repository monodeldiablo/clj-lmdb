(ns clj-lmdb.core-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.core :refer :all]
            [clojure.java.io :as io]))

(deftest init
  (testing "Open an environment with the default options"
    (let [e (env "/tmp")]
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      (is (= 10485760 (-> e :_env (.info) (.getMapSize))))
      (io/delete-file "/tmp/data.mdb")
      (io/delete-file "/tmp/lock.mdb")))
  (testing "Open an environment with multiple DBs"
    (let [e (env "/tmp"
                 :max-size (* 33 1024 1024)
                 :dbs {:test1 [:create]
                       :test2 [:create]
                       :test3 [:create]})]
      (is (= 3 (-> e (keys) (count) (dec))))
      (is (= 0 (-> e :test1 (.stat) (.ms_entries))))
      (is (= 0 (-> e :test2 (.stat) (.ms_entries))))
      (is (= 0 (-> e :test3 (.stat) (.ms_entries))))
      (is (= 34603008 (-> e :_env (.info) (.getMapSize))))
      (io/delete-file "/tmp/data.mdb")
      (io/delete-file "/tmp/lock.mdb")))
  (testing "Fill up a DB until it fails"
    (let [e (env "/tmp")]
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      ;; FIXME: attempt to write 10 MB worth of stuff in a txn
      (with-txn [txn (write-txn e)]
        (dotimes [n (* 5 1024 1024)]
          (put! env :db txn (str n) (str n))))
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      ;; FIXME: ensure the write fails
      ;; FIXME: count the entries (should still be 0)
      )))
