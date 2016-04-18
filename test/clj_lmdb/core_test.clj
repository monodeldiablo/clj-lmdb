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
               "bar1")))

      (delete! db "foo")
      (delete! db "foo1"))))

(deftest iteration-test
  (testing "Iteration"
    (let [db (make-db "/tmp")]
      (with-write-txn db
        (doall
         (map
          (fn [i]
            (put! (str i) (str i)))
          (range 1000))))

      (with-read-txn db
        (let [num-items (count
                         (doall
                          (map
                           (fn [[k v]]
                             (is (= k v))
                             [k v])
                           (items db))))]
          (is (= num-items 1000))))

      (with-read-txn db
        (let [num-items (count
                         (doall
                          (map
                           (fn [[k v]]
                             (is (= k v))
                             [k v])
                           (items-from db "500"))))]
          (is (= num-items 553)))) ; items are sorted in alphabetical order - not numerical

      (with-write-txn db
        (doall
         (map
          #(-> %
               str
               delete!)
          (range 1000)))

        (is (= (count (items-from db "400"))
               0))))))
