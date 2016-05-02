(ns clj-lmdb.core
  (:import [org.fusesource.lmdbjni Database Env Constants]))

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

;; FIXME: add support for custom marshal/unmarshal functions
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
                    dbs []
                    flags []}}]
  (let [env (doto (Env.)
              (.open dir-path
                     (->> (map env-flags flags)
                          (apply bit-or 0 0)))
              (.setMapSize max-size))
        env-map (:_env env)]
    (if (> (count dbs) 0)
      (do
        (.setMaxDbs (count dbs))
        (reduce (fn [env-map [db-name config]]
                  (assoc env-map
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
  ([db txn k v]
   (.put db
         (:txn txn)
         k
         v))
  ([db k v]
   (.put db
         k
         v)))

(defn get!
  ([db txn k]
   (.get db
         (:txn txn)
         k))
  ([db k]
   (.get db
         k)))

(defn delete!
  ([db txn k]
   (.delete db
            (:txn txn)
            k))
  ([db k]
   (.delete db
            k)))

(defn items
  [db txn]
  (let [txn* (:txn txn)
        entries (-> db
                    (.iterate txn*)
                    iterator-seq)]
    (map
     (fn [e]
       [(.getKey e) (.getValue e)])
     entries)))

(defn items-from
  [db txn from]
  (let [txn* (:txn txn)
        entries (-> db
                    (.seek txn*
                           from)
                    iterator-seq)]
    (map
     (fn [e]
       [(.getKey e) (.getValue e)])
     entries)))
