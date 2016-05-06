(ns clj-lmdb.simple
  (:require [clj-lmdb.core :as core]))

(def read-txn core/read-txn)
(def write-txn core/write-txn)

(defn make-db
  [path]
  (core/env path))

(defmacro with-txn
  [& args]
  `(core/with-txn ~@args))

(defn put!
  ([env txn k v]
   (core/put! env :db txn (str k) (str v)))
  ([env k v]
   (core/put! env :db (str k) (str v))))

(defn get!
  ([env txn k]
   (core/get! env :db txn (str k)))
  ([env k]
   (core/get! env :db (str k))))

(defn delete!
  ([env txn k]
   (core/delete! env :db txn (str k)))
  ([env k]
   (core/delete! env :db (str k))))

(defn items
  [env txn]
  (core/items env :db txn))

(defn items-from
  [env txn from]
  (core/items-from env :db txn (str from)))
