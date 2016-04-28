(ns clj-lmdb.core
  (:require [clojure.core :as core]
            [clj-lmdb.specs :refer :all])
  (:import [clj_lmdb.specs Txn DB]
           [org.fusesource.lmdbjni Database Env]
           [org.fusesource.lmdbjni Constants]))

(defn make-db
  [dir-path]
  (let [env (Env. dir-path)
        db  (.openDatabase env)]
   (DB. env db)))

(defn read-txn
  [db-record]
  (let [env (get-env db-record)
        txn (.createReadTransaction env)]
    (Txn. txn :read)))

(defn write-txn
  [db-record]
  (let [env (get-env db-record)
        txn (.createWriteTransaction env)]
    (Txn. txn :write)))

(defmacro with-txn
  [bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-txn ~(subvec bindings 2)
                                  ~@body)
                                (finally
                                  (if (-> ~(bindings 0)
                                          :type
                                          (= :read))
                                    (-> ~(bindings 0)
                                        :txn
                                        (.abort))
                                    (-> ~(bindings 0)
                                        :txn
                                        (.commit))))))
    :else (throw (IllegalArgumentException.
                  "with-open only allows Symbols in bindings"))))

(defn put!
  ([db-record txn k v]
   (let [db (get-db db-record)]
     (.put db
           (:txn txn)
           (Constants/bytes k)
           (Constants/bytes v))))

  ([db-record k v]
   (let [db (get-db db-record)]
     (.put db
           (Constants/bytes k)
           (Constants/bytes v)))))

(defn get!
  ([db-record txn k]
   (let [db (get-db db-record)]
     (Constants/string
      (.get db
            (:txn txn)
            (Constants/bytes k)))))
  
  ([db-record k]
   (let [db (get-db db-record)]
     (Constants/string
      (.get db
            (Constants/bytes k))))))

(defn delete!
  ([db-record txn k]
   (let [db (get-db db-record)]
     (.delete db
              (:txn txn)
              (Constants/bytes k))))

  ([db-record k]
   (let [db (get-db db-record)]
    (.delete db
             (Constants/bytes k)))))
