(ns clj-lmdb.core
  (:import [org.fusesource.lmdbjni Database Env]
           [org.fusesource.lmdbjni Constants]))

(defrecord DB [env db])

(defrecord Txn [txn type])

(defn make-db
  "Initialize a new database, optionally specifying *max-size*, in
  bytes. By default, the maximum size of the memory map (and thus the
  database) is 10485760 bytes."
  ([dir-path max-size]
   (let [env (doto (Env. dir-path)
               (.setMapSize max-size))
         db  (.openDatabase env)]
     (DB. env db)))
  ([dir-path]
   (make-db dir-path 10485760)))

(defn read-txn
  [db-record]
  (let [env (:env db-record)
        txn (.createReadTransaction env)]
    (Txn. txn :read)))

(defn write-txn
  [db-record]
  (let [env (:env db-record)
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
   (let [db (:db db-record)]
     (.put db
           (:txn txn)
           (Constants/bytes k)
           (Constants/bytes v))))

  ([db-record k v]
   (let [db (:db db-record)]
     (.put db
           (Constants/bytes k)
           (Constants/bytes v)))))

(defn get!
  ([db-record txn k]
   (let [db (:db db-record)]
     (Constants/string
      (.get db
            (:txn txn)
            (Constants/bytes k)))))
  
  ([db-record k]
   (let [db (:db db-record)]
     (Constants/string
      (.get db
            (Constants/bytes k))))))

(defn delete!
  ([db-record txn k]
   (let [db (:db db-record)]
     (.delete db
              (:txn txn)
              (Constants/bytes k))))

  ([db-record k]
   (let [db (:db db-record)]
    (.delete db
             (Constants/bytes k)))))


(defn items
  [db-record txn]
  (let [db   (:db db-record)
        txn* (:txn txn)
        
        entries (-> db
                    (.iterate txn*)
                    iterator-seq)]
    (map
     (fn [e]
       (let [k (.getKey e)
             v (.getValue e)]
         [(Constants/string k)
          (Constants/string v)]))
     entries)))

(defn items-from
  [db-record txn from]
  (let [db   (:db db-record)
        txn* (:txn txn)
        
        entries (-> db
                    (.seek txn*
                           (Constants/bytes from))
                    iterator-seq)]
    (map
     (fn [e]
       (let [k (.getKey e)
             v (.getValue e)]
         [(Constants/string k)
          (Constants/string v)]))
     entries)))
 
