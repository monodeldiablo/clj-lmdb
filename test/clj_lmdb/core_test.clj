(ns clj-lmdb.core-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.core :refer :all]
            [clojure.java.io :as io])
  (:import [org.fusesource.lmdbjni Constants]))

;; FIXME: write test fixtures for simple and complex DB setup/teardown
;; FIXME: test duplicate key support

(defn cleanup []
  (io/delete-file "/tmp/data.mdb")
  (io/delete-file "/tmp/lock.mdb"))

(deftest init
  (testing "Open an environment with the default options"
    (let [e (env "/tmp")]
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      (is (= 10485760 (-> e :_env (.info) (.getMapSize))))
      (cleanup)))

  (testing "Fill up a DB until it fails"
    (let [e (env "/tmp")
          b #(Constants/bytes %)]
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      ;; attempt to write >10 MB worth of stuff in a txn
      (is (thrown? org.fusesource.lmdbjni.LMDBException
                   (with-txn [txn (write-txn e)]
                     (dotimes [n (* 5 1024 1024)]
                       (put! e :db txn (b (str n)) (b (str n)))))))
      ;; number of entries should still be 0, since the txn failed
      (is (= 0 (-> e :db (.stat) (.ms_entries))))
      (cleanup)))

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
      (cleanup)))

  ;; NOTE: Looks like something under the covers isn't properly
  ;; supporting sort-order flags. The :integer-key and :reverse-key
  ;; tests break.
  (testing "Compare sorting strategies"
    (let [e (env "/tmp"
                 :dbs {:str-test [:create]
                       :int-test [:integer-key :create]
                       :rev-test [:reverse-key :create]})
          in #(let [buf (java.nio.ByteBuffer/allocate Long/BYTES)]
                (.putLong buf 0 %)
                (.array buf))
          out #(let [buf (java.nio.ByteBuffer/wrap %)]
                 (.getLong buf))
          nums [-123 0 123]]
      (doseq [n nums]
        (put! e :str-test (in n) (in n))
        (put! e :int-test (in n) (in n))
        (put! e :rev-test (in n) (in n)))
      (is (= -123 (->> (in -123) (get! e :str-test) (out))))
      (is (= -123 (->> (in -123) (get! e :int-test) (out))))
      (is (= -123 (->> (in -123) (get! e :rev-test) (out))))
      (with-txn [txn (read-txn e)]
        (is (= [0 123 -123] (->> (items e :str-test txn)
                                 (map #(out (first %)))
                                 (vec))))
        (is (= [-123 0 123] (->> (items e :int-test txn)
                                 (map #(out (first %)))
                                 (vec))))
        (is (= [0 -123 123] (->> (items e :rev-test txn)
                                 (map #(out (first %)))
                                 (vec)))))
      (cleanup)))

  (testing "Fetching duplicate keys is supported"
    (let [e (env "/tmp"
                 :dbs {:control [:create]
                       :dup-test [:create :dup-sort]})
          in #(Constants/bytes %)
          out #(Constants/string %)]
      (dotimes [n 10]
        (put! e :control (in "foo") (in (str n)))
        (put! e :dup-test (in "foo") (in (str n))))
      (is (= "9" (->> (in "foo") (get! e :control) (out))))
      (is (= "0" (->> (in "foo") (get! e :dup-test) (out))))
      (is (= 1 (count (get-many e :control (in "foo")))))
      (is (= 10 (count (get-many e :dup-test (in "foo")))))
      (cleanup)))

  (testing "Ranges are supported"
    (let [e (env "/tmp"
                 :dbs {:no-dups [:create]
                       :dups [:create :dup-sort]})
          in #(let [buf (java.nio.ByteBuffer/allocate Long/BYTES)]
                (.putLong buf 0 %)
                (.array buf))
          out #(let [buf (java.nio.ByteBuffer/wrap %)]
                 (.getLong buf))
          keys [0 1 1 1 2 2 3 3 4 5 6 7 7 8 8 8 9]
          vals [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]]
      (doseq [pair (map vector keys vals)]
        (put! e :no-dups (in (first pair)) (in (second pair)))
        (put! e :dups (in (first pair)) (in (second pair))))
      (is (= [3 5 7 8 9 10 12 15]
             (->> (range! e :no-dups (in 1) (in 9))
                  (map #(out (second %))))))
      (is (= (range 1 16)
             (->> (range! e :dups (in 1) (in 9))
                  (map #(out (second %))))))
      (cleanup)))

  (testing "Fetching the first and last values in a DB"
    (let [e (env "/tmp")
          in #(let [buf (java.nio.ByteBuffer/allocate Long/BYTES)]
                (.putLong buf 0 %)
                (.array buf))
          out #(let [buf (java.nio.ByteBuffer/wrap %)]
                 (.getLong buf))
          keys [0 1 1 1 2 2 3 3 4 5 6 7 7 8 8 8 9]
          vals [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]]
      (doseq [pair (map vector keys vals)]
        (put! e :db (in (first pair)) (in (second pair))))
      (is (= 0 (-> (first! e :db) (second) (out))))
      (is (= 16 (-> (last! e :db) (second) (out))))
      (cleanup))))
