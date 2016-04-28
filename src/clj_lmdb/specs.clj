(ns clj-lmdb.specs
  "This ns contains records and protocol definitions")

(defrecord DB [env db])

(defrecord Txn [txn type])
