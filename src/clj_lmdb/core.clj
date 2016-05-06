(ns clj-lmdb.core
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

;; FIXME: remove any marshaling here. wrong layer.
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
        env-map {:_env env}]
    (if (> num-dbs 0)
      (do
        (reduce (fn [e [db-name config]]
                  (assoc e
                         db-name
                         (.openDatabase env
                                        (name db-name)
                                        (->> (map db-flags config)
                                             (apply bit-or 0 0)))))
                env-map
                dbs))
      (assoc env-map :db (.openDatabase env)))))

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

(defn put!
  ([env db txn k v]
   (.put (get-in env [db])
         (:txn txn)
         k
         v))
  ([env db k v]
   (.put (get-in env [db])
         k
         v)))

(defn get!
  ([env db txn k]
   (.get (get-in env [db])
         (:txn txn)
         k))
  ([env db k]
   (.get (get-in env [db])
         k)))

;; TODO: If the DB was created with :dup-fixed, then use GET_MULTIPLE
;; and NEXT_MULTIPLE GetOps to speed things up.
;; TODO: Wrap this in a lazy-seq.
(defn get-many
  ([env db txn k]
   (let [cursor (.openCursor (get-in env [db]) (:txn txn))
         this (.seek cursor SeekOp/KEY k)
         entries (loop [vals [[(.getKey this) (.getValue this)]]]
                   (if-let [n (.get cursor GetOp/NEXT_DUP)]
                     (recur (conj vals [(.getKey n) (.getValue n)]))
                     vals))]
     (.close cursor)
     entries))
  ([env db k]
   (with-txn [txn (read-txn env)]
     (get-many env db txn k))))

;; FIXME: fetch all k/v within a range, including dups

(defn delete!
  ([env db txn k]
   (.delete (get-in env [db])
            (:txn txn)
            k))
  ([env db k]
   (.delete (get-in env [db])
            k)))

(defn items
  [env db txn]
  (let [txn* (:txn txn)
        entries (-> (get-in env [db])
                    (.iterate txn*)
                    iterator-seq)]
    (map
     (fn [e]
       [(.getKey e) (.getValue e)])
     entries)))

(defn items-from
  [env db txn from]
  (let [txn* (:txn txn)
        entries (-> (get-in env [db])
                    (.seek txn*
                           from)
                    iterator-seq)]
    (map
     (fn [e]
       [(.getKey e) (.getValue e)])
     entries)))
