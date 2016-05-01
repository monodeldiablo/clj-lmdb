(ns clj-lmdb.simple
  (:require [clj-lmdb.core :as core])
  (:import [org.fusesource.lmdbjni Constants]))

(def make-db core/make-db)
(def read-txn core/read-txn)
(def write-txn core/write-txn)

(defmacro with-txn
  [& args]
  `(core/with-txn ~@args))

(defn put!
  ([db-record txn k v]
   (core/put! db-record txn (Constants/bytes k) (Constants/bytes v)))
  ([db-record k v]
   (core/put! db-record (Constants/bytes k) (Constants/bytes v))))

(defn get!
  ([db-record txn k]
   (->> (Constants/bytes k)
        (core/get! db-record txn)
        (Constants/string)))
  ([db-record k]
   (->> (Constants/bytes k)
        (core/get! db-record)
        (Constants/string))))

(defn delete!
  ([db-record txn k]
   (core/delete! db-record txn (Constants/bytes k)))
  ([db-record k]
   (core/delete! db-record (Constants/bytes k))))

(defn items
  [db-record txn]
  (->> (core/items db-record txn)
       (map (fn [[k v]] [(Constants/string k) (Constants/string v)]))))

(defn items-from
  [db-record txn from]
  (->> (Constants/bytes from)
       (core/items-from db-record txn)
       (map (fn [[k v]] [(Constants/string k) (Constants/string v)]))))
 
