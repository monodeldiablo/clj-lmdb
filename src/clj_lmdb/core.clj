(ns clj-lmdb.core
  (:refer-clojure :exclude [get]) ; suppress the shadowing warning
  (:require [clojure.core :as core])
  (:import [org.fusesource.lmdbjni Database Env]
           [org.fusesource.lmdbjni Constants]))

(declare ^:dynamic *txn*)

(defmacro with-write-txn
  [tx db-record & body]
  `(let [db-record# ~db-record
         env# (:env db-record#)
         db#  (:db db-record#)

         txn# (.createWriteTransaction env#)]
     (binding [*txn* txn#]
      ~@body)
     (.commit txn#)))

(defmacro with-read-txn
  [db-record & body]
  `(let [db-record# ~db-record
         env# (:env db-record#)
         db#  (:db db-record#)

         txn# (.createReadTransaction env#)]
     (binding [*txn* txn#]
       ~@body)
     (.commit txn#)))

(defrecord DB [env db])

(defn make-db
  [dir-path]
  (let [env (Env. dir-path)
        db  (.openDatabase env)]
   (DB. env db)))

(defn put!
  ([k v]
   (.put *txn*
         (Constants/bytes k)
         (Constants/bytes k)))

  ([db-record k v]
   (let [db (:db db-record)]
     (.put db
           (Constants/bytes k)
           (Constants/bytes v)))))

(defn get!
  ([k]
   (Constants/string
    (.get *txn*
          (Constants/bytes k))))
  
  ([db-record k]
   (let [db (:db db-record)]
     (Constants/string
      (.get db
            (Constants/bytes k))))))

