(ns clj-lmdb.simple
  (:require [clj-lmdb.core :as core])
  (:import [org.fusesource.lmdbjni Constants]))

(def read-txn core/read-txn)
(def write-txn core/write-txn)

(defn in
  [s]
  (Constants/bytes s))

(defn out
  [b]
  (Constants/string b))

(defn make-db
  [path]
  (core/env path))

(defmacro with-txn
  [& args]
  `(core/with-txn ~@args))

(defn put!
  ([env txn k v]
   (core/put! env :db txn (in k) (in v)))
  ([env k v]
   (core/put! env :db (in k) (in v))))

(defn get!
  ([env txn k]
   (out (core/get! env :db txn (in k))))
  ([env k]
   (out (core/get! env :db (in k)))))

(defn delete!
  ([env txn k]
   (core/delete! env :db txn (in k)))
  ([env k]
   (core/delete! env :db (in k))))

(defn items
  [env txn]
  (->> (core/items env :db txn)
       (map (fn [[k v]]
              [(out k) (out v)]))))

(defn items-from
  [env txn from]
  (->> (core/items-from env :db txn (in from))
       (map (fn [[k v]]
              [(out k) (out v)]))))
