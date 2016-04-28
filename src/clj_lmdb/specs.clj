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
  
  (get-db [this] db)
  
  ;; (items [this]
  ;;   (let [entries (-> db
  ;;                     (.iterate *txn*)
  ;;                     iterator-seq)]
  ;;     (map
  ;;      (fn [e]
  ;;        (let [k (.getKey e)
  ;;              v (.getValue e)]
  ;;          [(Constants/string k)
  ;;           (Constants/string v)]))
  ;;      entries)))
  
  ;; (items-from [this from]
  ;;   (let [entries (-> *db*
  ;;                     (.seek *txn*
  ;;                            (Constants/bytes from))
  ;;                     iterator-seq)]
  ;;     (map
  ;;      (fn [e]
  ;;        (let [k (.getKey e)
  ;;              v (.getValue e)]
  ;;          [(Constants/string k)
  ;;           (Constants/string v)]))
  ;;      entries)))
  )

(defrecord Txn [txn type])
