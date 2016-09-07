(ns clj-lmdb.core
  (:require [byte-streams :as bs])
  (:import [org.fusesource.lmdbjni Database Env Constants SeekOp GetOp]))

(defrecord Txn [txn type])

(def env-flags
  {:read-only Constants/RDONLY})

(def db-flags
  {:create Constants/CREATE
   :reverse-key Constants/REVERSEKEY
   :integer-key Constants/INTEGERKEY
   :dup-sort Constants/DUPSORT
   :dup-fixed Constants/DUPFIXED
   :integer-data Constants/INTEGERDUP
   :reverse-data Constants/REVERSEDUP})

(defn- mk-db
  [env db-name config]
  (.openDatabase env
                 (name db-name)
                 (->> (map db-flags config)
                      (apply bit-or 0 0))))

(defn env
  "Initialize a new environment, optionally specifying the following
  parameters:

  - `:dbs` - a map of database configurations, of the form `{<db name>
    <vector of flags>}`. The following database config keys are supported:
    - `:create` - create this database if it doesn't already exist
    - `:reverse-key` - keys are treated as binary strings and compared
      in reverse order, from the end of the string to the beginning
    - `:integer-key` - keys are treated as binary integers in native
      byte order (note: this setting requires all keys in the database
      to be the same size)
    - `:dup-sort` - allow duplicate keys in the database
    - `:dup-fixed` - indicate that all values will be a fixed size,
      enabling performance optimizations (note: this may only be used in
      conjunction with `:dup-sort`)
    - `:integer-data` - values are treated as binary integers in native
      byte order (note: this may only be used in conjunction with
      `:dup-sort`)
    - `:reverse-data` - values are treated as binary strings and
      compared in reverse order, from the end of the string to the
      beginning (note: this may only be used in conjunction with
      `:dup-sort`)
  - `:max-size` - the maximum size, in bytes, of the memory map (and,
    thus, the total size of all the databases within this
    environment. The default is 10485760 bytes.
  - `:flags` - A vector of special options for the environment. Currently, only
    `:read-only` is supported."
  [dir-path & {:keys [max-size dbs flags]
               :or {max-size 10485760
                    dbs {}
                    flags []}}]
  (let [num-dbs (long (count dbs))
        env (doto (Env.)
              (.setMaxDbs num-dbs)
              (.setMapSize max-size)
              (.open dir-path
                     (->> (map env-flags flags)
                          (apply bit-or 0 0))))
        env-map {:_env env
                 :_config dbs}]
    (if (> num-dbs 0)
      (do
        (transient
         (reduce (fn [e [db-name config]]
                   (assoc e
                          db-name
                          (mk-db env db-name config)))
                 env-map
                 dbs)))
      (transient (assoc env-map :db (.openDatabase env))))))

(defn read-txn
  [env]
  (let [txn (.createReadTransaction (:_env env))]
    (Txn. txn :read)))

(defn write-txn
  [env]
  (let [txn (.createWriteTransaction (:_env env))]
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

;; NOTE: There seems to be an issue with the 'delete' option in LMDB's
;; 'drop' command. As a result, dropped databases continue to use up
;; RAM, but are no longer reachable. Disaster. So I just wipe all the
;; data in a give DB instead.
(defn drop!
  ([env db txn]
   (let [cursor (.openCursor (get env db) (:txn txn))]
     (while (.get cursor GetOp/FIRST)
       (.delete cursor))
     (.close cursor))   )
  ([env db]
   (with-txn [txn (write-txn env)]
     (drop! env db txn))))

(defn put!
  ([env db txn k v]
   (.put (get env db)
         (:txn txn)
         k
         v))
  ([env db k v]
   (.put (get env db)
         k
         v)))

(defn delete!
  ([env db txn k]
   (.delete (get env db)
            (:txn txn)
            k))
  ([env db k]
   (.delete (get env db)
            k)))

;; Remove a row from a dup-supporting DB. This has terrible
;; performance for rows with lots of dups.
(defn delete-row!
  ([env db txn k v]
   (let [cursor (.openCursor (get env db) (:txn txn))]
     (when-let [row-key (.seek cursor SeekOp/KEY k)]
       (loop [this row-key]
         (when this
           (if (bs/bytes= v (.getValue this))
             (.delete cursor)
             (recur (.get cursor GetOp/NEXT_DUP))))))
     (.close cursor)))
  ([env db k v]
   (with-txn [txn (write-txn env)]
     (delete-row! env db txn k v))))

(defn get!
  ([env db txn k]
   (.get (get env db)
         (:txn txn)
         k))
  ([env db k]
   (.get (get env db)
         k)))

(defn first!
  ([env db txn]
   (-> (get env db)
       (.iterate (:txn txn))
       (iterator-seq)
       (first)
       (#(vector (.getKey %) (.getValue %)))))
  ([env db]
   (with-txn [txn (read-txn env)]
     (first! env db txn))))

(defn last!
  ([env db txn]
   (-> (get env db)
       (.iterateBackward (:txn txn))
       (iterator-seq)
       (first)
       (#(vector (.getKey %) (.getValue %)))))
  ([env db]
   (with-txn [txn (read-txn env)]
     (last! env db txn))))

(defn last-dup
  ([env db txn k]
   (let [cursor (.openCursor (get env db) (:txn txn))
         this (.seek cursor SeekOp/KEY k)
         v (when this
             (.getValue (.get cursor GetOp/LAST_DUP)))]
     (.close cursor)
     v))
  ([env db k]
   (with-txn [txn (read-txn env)]
     (last-dup env db txn k))))

;; FIXME: Rename this fn. It's got a terrible name.
;; TODO: If the DB was created with :dup-fixed, then use GET_MULTIPLE
;; and NEXT_MULTIPLE GetOps to speed things up.
;; TODO: Wrap this in a lazy-seq.
(defn get-many
  "Get all the values corresponding to key `k`. If `db` does not
  support duplicates, it will return a vector containing a single
  value if there is an entry for `k`."
  ([env db txn k]
   (let [cursor (.openCursor (get env db) (:txn txn))
         this (.seek cursor SeekOp/KEY k)
         entries (if this
                   (loop [vals [(.getValue this)]]
                     (if-let [n (.get cursor GetOp/NEXT_DUP)]
                       (recur (conj vals (.getValue n)))
                       vals))
                   [])]
     (.close cursor)
     entries))
  ([env db k]
   (with-txn [txn (read-txn env)]
     (get-many env db txn k))))

(defn get-n
  "Get `n` values corresponding to key `k`. If `db` does not support
  duplicates, it will return a vector containing a single value if
  there is an entry for `k`. If `n` is negative or omitted, it will
  fetch all the values."
  ([env db txn k n]
   (let [cursor (.openCursor (get env db) (:txn txn))
         this (.seek cursor SeekOp/KEY k)
         entries (if this
                   (loop [count 0
                          vals [(.getValue this)]]
                     (if (and n
                              (> n 0)
                              (> count n))
                       (drop-last vals)
                       (if-let [next (.get cursor GetOp/NEXT_DUP)]
                         (recur (inc count)
                                (conj vals (.getValue next)))
                         vals)))
                   [])]
     (.close cursor)
     entries))
  ([env db k n]
   (with-txn [txn (read-txn env)]
     (get-n env db txn k n)))
  ([env db k]
   (get-n env db k -1)))

(defn get-count
  "Get the number of duplicate values corresponding to key `k`. If
  `db` does not support duplicates, it will return 1 if there is an
  entry for `k`. If there is no corresponding key, then this will
  return 0."
  ([env db txn k]
   (let [cursor (.openCursor (get env db) (:txn txn))
         this (.seek cursor SeekOp/KEY k)
         c (.count cursor)]
     (.close cursor)
     c))
  ([env db k]
   (with-txn [txn (read-txn env)]
     (get-count env db txn k))))

(defn page
  "Retrieve at most `n` items from the given database, starting after
  `start-key`. If `start-key` is nil, then start at the first item."
  ([env db txn start-key n]
   (let [cursor (.openCursor (get env db) (:txn txn))
         start (if start-key
                 (do
                   (.seek cursor SeekOp/KEY start-key)
                   ;; skip one past start-key
                   (.get cursor GetOp/NEXT_NODUP))
                 (.get cursor GetOp/FIRST))
         buffer (if start
                  (loop [count 1
                         ;; FIXME: make this an object array for speed
                         buf [[(.getKey start)
                               (.getValue start)]]]
                    ;; FIXME: There should be a :dup and :nodup mode!
                    (let [next (.get cursor GetOp/NEXT_NODUP)]
                      (if (and next
                               (< count n))
                        (recur (inc count)
                               (conj buf [(.getKey next)
                                          (.getValue next)]))
                        buf)))
                  [])]
     (.close cursor)
     buffer))
  ([env db start-key n]
   (with-txn [txn (read-txn env)]
     (page env db txn start-key n))))

(defn items
  ([env db txn]
   (let [txn* (:txn txn)
         d (get env db)
         entries (->> (.iterate d txn*)
                      (iterator-seq)
                      (map
                       (fn [e]
                         [(.getKey e) (.getValue e)])))]
     entries))
  ([env db]
   (with-txn [txn (read-txn env)]
     (items env db txn))))

(defn keys!
  ([env db txn]
   (let [txn* (:txn txn)
         entries (-> (get env db)
                     (.iterate txn*)
                     (iterator-seq))]
     (->> entries
          (map #(.getKey %)))))
  ([env db]
   (with-txn [txn (read-txn env)]
     (keys! env db txn))))

(defn items-from
  ([env db txn from]
   (let [txn* (:txn txn)
         entries (-> (get env db)
                     (.seek txn*
                            from)
                     iterator-seq)]
     (map
      (fn [e]
        [(.getKey e) (.getValue e)])
      entries)))
  ([env db from]
   (with-txn [txn (read-txn env)]
     (items-from env db txn from))))

;; NOTE: compare-bytes uses big-endian comparison, whilst LMDB uses
;; native byte order (usually little-endian). This'd be quicker if we
;; used a comparator that could do native order instead of flipping
;; everything.
;; FIXME: This is slooooooooooow.
;; FIXME: make this aware of sort-order directives (e.g. :integer-key)
(defn range!
  "Returns all the [key, value] pairs in the interval [start, end). If
  `db` supports duplicates, they are included in sorted order."
  ([env db txn start end]
   (let [buf (doto (java.nio.ByteBuffer/wrap end)
               (.order (java.nio.ByteOrder/nativeOrder)))
         end (.array buf)]
     (->> (items-from env db txn start)
          (take-while #(-> %
                           (first)
                           (java.nio.ByteBuffer/wrap)
                           (.order (java.nio.ByteOrder/nativeOrder))
                           (.array)
                           (bs/compare-bytes end)
                           (< 0))))))
  ([env db start end]
   (with-txn [txn (read-txn env)]
     ;; FIXME: This really doesn't like being lazy in its current
     ;; form. Transactions are timing out. We should really
     ;; lazy-cat a series of chunked transactions together, but
     ;; that'll have to wait.
     (doall (range! env db txn start end)))))
