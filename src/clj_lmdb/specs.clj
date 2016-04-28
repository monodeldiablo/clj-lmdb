(ns clj-lmdb.specs
  "This ns contains records and protocol definitions")

(defprotocol LMDB
  "A simple protocol for getters and setters
  w/ LBDB"
  (get-env [this])
  (get-db [this])
;  (items [this])
;  (items-from [this from])
  )

(deftype DB [env db]

  LMDB
  (get-env [this] env)
  
  (get-db [this] db))

(defrecord Txn [txn type])
