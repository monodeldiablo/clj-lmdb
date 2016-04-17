(ns clj-lmdb.core
  (:refer-clojure :exclude [get]) ; suppress the shadowing warning
  (:require [clojure.core :as core])
  (:import [org.fusesource.lmdbjni Database Env]
           [org.fusesource.lmdbjni Constants]))

(declare ^:dynamic *txn*)
(declare ^:dynamic *db*)

(defprotocol LMDB
  "A simple protocol for getters and setters
  w/ LBDB"
  (get-env [this])
  (get-db [this])
  #_(items
    ([this])
    ([this from])))

(deftype DB [env db]

  LMDB
  (get-env [this] env)
  (get-db [this] db)
  #_(items
    ([this]
     (let [entries (-> db
                       (.iterate *txn*)
                       iterator-seq)]
       (map
        (fn [e]
          [(.first e)
           (.second e)])
        entries)))

    ([this from]
     (let [entries (-> (.seek this *txn* from)
                       iterator-seq)]
       (map
        (fn [e]
          [(.first e)
           (.second e)])
        entries)))))

(defn make-db
  [dir-path]
  (let [env (Env. dir-path)
        db  (.openDatabase env)]
   (DB. env db)))

(defmacro with-write-txn
  [db-record & body]
  `(let [db-record# ~db-record
         env# (get-env db-record#)
         db#  (get-db db-record#)

         txn# (.createWriteTransaction env#)]
     (binding [*db*  db#
               *txn* txn#]
      ~@body)
     (.commit txn#)))

(defmacro with-read-txn
  [db-record & body]
  `(let [db-record# ~db-record
         env# (get-env db-record#)
         db#  (get-db db-record#)

         txn# (.createReadTransaction env#)]
     (binding [*txn* txn#
               *db*  db#]
       ~@body)
     (.abort txn#)))

(defn put!
  ([k v]
   (.put *db*
         *txn*
         (Constants/bytes k)
         (Constants/bytes v)))

  ([db-record k v]
   (let [db (get-db db-record)]
     (.put db
           (Constants/bytes k)
           (Constants/bytes v)))))

(defn get!
  ([k]
   (Constants/string
    (.get *db*
          *txn*
          (Constants/bytes k))))
  
  ([db-record k]
   (let [db (get-db db-record)]
     (Constants/string
      (.get db
            (Constants/bytes k))))))

(defn delete!
  ([k]
   (.delete *txn*
            (Constants/bytes k)))

  ([db-record k]
   (let [db (get-db db-record)]
    (.delete db
             (Constants/bytes k)))))
