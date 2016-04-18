# clj-lmdb

Clojure wrappers for lmdb - the best no-nonsense, no-surprise, fast key-value store.

## Usage

TODO: ADD LEIN COORDINATES

### Creating a database

`make-db`

	```clojure
	(use 'clj-lmdb.core :reload)
	nil
	user> (def db (make-db "/tmp"))
    #'user/db
	```

### Inserting/Retrieving Values

`get!` and `put!`

	```clojure
	user> (put! db "foo" "bar")
	nil
	user> (get! db "foo")
	"bar"
	user> 
	```

### Deleting Values

`delete!`

```clojure
user> (delete! db "foo")
true
user> (get! db "foo")
nil
user>
```

### Transactions:

`with-read-txn` for read-only transactions

`with-write-txn` for transactions that update the db

This inserts a couple of entries:

	```clojure
	(with-write-txn db
      (put! "foo"
            "bar")
      (put! "foo1"
            "bar1"))
	```

This retrieves them

	```clojure
	(with-read-txn db
      (= (get! "foo")
         "bar") ; true

      (= (get! "foo1")
         "bar1")) ; true
	```

### Iterating through entries:

Inside a read-transaction you can use `items` or `items-from`
to iterate over the entries or to iterate from a particular key onwards.

```clojure
(with-read-txn db
 (count
  (doall
   (map
    (fn [[k v]]
      ...)
    (items db)))))
```

```clojure
(with-read-txn db
 (count
  (doall
   (map
    (fn [[k v]]
      ...)
    (items-from db "foo")))))
```

### LICENSE

Copyright © 2016 Shriphani Palakodety

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
