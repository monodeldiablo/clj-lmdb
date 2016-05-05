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
  - `:marshal-fn` - a custom serializer to convert input into a byte
    string (optional; default serializer only supports strings)
  - `:unmarshal-fn` - a custom deserializer to convert a byte string back
    into useful output (optional; default deserializer only supports strings)
  - `:flags` - A vector of special options for the environment. Currently, only
    `:read-only` is supported."
  [dir-path & {:keys [max-size dbs flags]
               :or {max-size 10485760
                    dbs {}
                    flags []
                    marshal-fn Constants/bytes
                    unmarshal-fn Constants/string}}]
  (let [num-dbs (long (count dbs))
        env (doto (Env.)
              (.setMaxDbs num-dbs)
              (.setMapSize max-size)
              (.open dir-path
                     (->> (map env-flags flags)
                          (apply bit-or 0 0))))
        env-map {:_env env
                 :_marshal-fn marshal-fn
                 :_unmarshal-fn unmarshal-fn}]
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
   (.put (env db)
         (:txn txn)
         ((:_marshal-fn env) k)
         ((:_marshal-fn env) v)))
  ([env db k v]
   (.put (env db)
         ((:_marshal-fn env) k)
         ((:_marshal-fn env) v))))

(defn get!
  ([env db txn k]
   ((:_unmarshal-fn env)
    (.get (env db)
          (:txn txn)
          ((:_marshal-fn env) k))))
  ([env db k]
   ((:_unmarshal-fn env)
    (.get (env db)
          ((:_marshal-fn env) k)))))

(defn delete!
  ([env db txn k]
   (.delete (env db)
            (:txn txn)
            ((:_marshal-fn env) k)))
  ([env db k]
   (.delete (env db)
            ((:_marshal-fn env) k))))

(defn items
  [env db txn]
  (let [txn* (:txn txn)
        entries (-> (env db)
                    (.iterate txn*)
                    iterator-seq)
        uf (:_unmarshal-fn env)]
    (map
     (fn [e]
       [(uf (.getKey e))
        (uf (.getValue e))])
     entries)))

(defn items-from
  [env db txn from]
  (let [txn* (:txn txn)
        entries (-> (env db)
                    (.seek txn*
                           ((:_marshal-fn env) from))
                    iterator-seq)
        uf (:_unmarshal-fn env)]
    (map
     (fn [e]
       [(uf (.getKey e))
        (uf (.getValue e))])
     entries)))
