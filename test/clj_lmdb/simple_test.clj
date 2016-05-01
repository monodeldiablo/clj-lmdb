(ns clj-lmdb.simple-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.simple :refer :all]))

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
      (with-txn [txn (write-txn db)]
        (put! db
              txn
              "foo"
              "bar")
        (put! db
              txn
              "foo1"
              "bar1"))

      (with-txn [txn (read-txn db)]
        (is (= (get! db
                     txn
                     "foo")
               "bar"))

        (is (= (get! db
                     txn
                     "foo1")
               "bar1")))

      (delete! db "foo")
      (delete! db "foo1"))))

(deftest iteration-test
  (testing "Iteration"
    (let [db (make-db "/tmp")]
      (with-txn [txn (write-txn db)]
        (doall
         (map
          (fn [i]
            (put! db txn (str i) (str i)))
          (range 1000))))

      (with-txn [txn (read-txn db)]
        (let [num-items (count
                         (doall
                          (map
                           (fn [[k v]]
                             (is (= k v))
                             [k v])
                           (items db txn))))]
          (is (= num-items 1000))))

      (with-txn [txn (read-txn db)]
        (let [num-items (count
                         (doall
                          (map
                           (fn [[k v]]
                             (is (= k v))
                             [k v])
                           (items-from db txn "500"))))]
          (is (= num-items 553)))) ; items are sorted in alphabetical order - not numerical

      (with-txn [txn (write-txn db)]
        (doall
         (map
          #(->> %
                str
                (delete! db txn))
          (range 1000)))

        (is (= (count (items-from db txn "400"))
               0))))))
