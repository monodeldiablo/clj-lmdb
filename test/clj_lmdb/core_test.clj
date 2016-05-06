(ns clj-lmdb.core-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.core :refer :all]
            [clojure.java.io :as io]))

;; FIXME: write test fixtures for simple and complex DB setup/teardown

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
      (is (= 3 (-> e (keys) (set) (disj :_env :_marshal-fn :_unmarshal-fn) (count))))
      (is (= 0 (-> e :test1 (.stat) (.ms_entries))))
      (is (= 0 (-> e :test2 (.stat) (.ms_entries))))
      (is (= 0 (-> e :test3 (.stat) (.ms_entries))))
      (is (= 34603008 (-> e :_env (.info) (.getMapSize))))
      (io/delete-file "/tmp/data.mdb")
      (io/delete-file "/tmp/lock.mdb")))
  (testing "Fill up a DB until it fails"
    (let [e (env "/tmp")]
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      ;; attempt to write >10 MB worth of stuff in a txn
      (is (thrown? org.fusesource.lmdbjni.LMDBException
                   (with-txn [txn (write-txn e)]
                     (dotimes [n (* 5 1024 1024)]
                       (put! e :db txn (str n) (str n))))))
      ;; number of entries should still be 0, since the txn failed
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      (io/delete-file "/tmp/data.mdb")
      (io/delete-file "/tmp/lock.mdb"))))
