(ns clj-lmdb.core-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.core :refer :all]))

(deftest non-txn-test
  (testing "Put + get without using a txn"
    (let [db (make-db "/tmp")]
      (put! db
            "foo"
            "bar")
      (is
       (= (get! db
                "foo")
          "bar"))

      (delete! db "foo")

      (is
       (nil?
        (get! db "foo"))))))

(deftest with-txn-test
  (testing "Results with a txn"
    (let [db (make-db "/tmp")]
      (with-write-txn db
        (put! "foo"
              "bar")
        (put! "foo1"
              "bar1"))

      (with-read-txn db
        (is (= (get! "foo")
               "bar"))

        (is (= (get! "foo1")
               "bar1"))))))
