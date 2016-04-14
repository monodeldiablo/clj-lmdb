(ns clj-lmdb.core-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.core :refer :all]))

(deftest non-txn-test
  (testing "Put get without using a txn"
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
