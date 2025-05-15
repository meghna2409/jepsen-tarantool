(ns tarantool.register
  "Run register tests."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [client :as client]
                    [checker :as checker]
                    [core :as jepsen]
                    [control :as c]
                    [independent :as independent]
                    [generator :as gen]
                    [util :refer [timeout meh]]]
            [next.jdbc :as j]
            [next.jdbc.sql :as sql]
            [knossos.model :as model]
            [jepsen.checker.timeline :as timeline]
            [tarantool [client :as cl]
                       [db :as db]]))

(def table-name "register")

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client [conn]
  client/Client

  (open! [this test node]
    (let [conn (cl/open node test)]
      (assert conn)
      (assoc this :conn conn :node node)))

  (setup! [this test node]
    (info node "Setting up register client")
    (let [this (client/open! this test node)
          conn (:conn this)]
            (Thread/sleep 10000)

      (when (= node (first (db/primaries test)))
        (info node "Creating register table")
        (try
          (j/execute! conn
            [(format "CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, value INT)"
                    table-name)])
          (catch Exception e
            (warn node "Error creating table:" (.getMessage e)))))

      (Thread/sleep 5000)
      (when (= node (first (db/primaries test)))
        (info node "Making register table synchronous")
        (let [tbl (str/upper-case table-name)]
          (try
            (j/execute! conn
              [(format "SELECT LUA('local s = box.space.%s; if s then s:alter{is_sync=true} end')" tbl)])
            (catch Exception e
              (warn node "Error making table synchronous:" (.getMessage e))))))

      this))

(invoke! [this test op]
  (try
    (let [[k value] (:value op)]
      (case (:f op)
        :read (let [result (sql/query (:conn this) [(str "SELECT value FROM " table-name " WHERE id = " k)])
                    v (:value (first result))]
                (info "Read successful:" k "value:" v)
                (assoc op :type :ok, :value (independent/tuple k v)))

        :write (do (let [con (cl/open (first (db/primaries test)) test)
                         table (clojure.string/upper-case table-name)]
                     (info "Attempting write" k "value:" value)
                     (j/execute! con
                       [(str "SELECT _UPSERT(" k ", " value ", '" table "')")])
                     (info "Write successful:" k "value:" value)
                     (assoc op :type :ok :value (independent/tuple k value))))

        :cas (do (let [[old new] value
                   con (cl/open (first (db/primaries test)) test)
                   table (clojure.string/upper-case table-name)
                   _ (info "Attempting CAS" k "from:" old "to:" new)
                   r (->> (j/execute! con
                            [(str "SELECT _CAS(" k ", " old ", " new ", '" table "')")])
                          (first)
                          (vals)
                          (first))]
                  (info "CAS result for" k ":" r)
                  (if r
                    (assoc op :type :ok :value (independent/tuple k value))
                    (assoc op :type :fail))))))
    (catch Exception e
      (warn "Operation failed:" (.getMessage e))
      (if (.getMessage e)
        (let [msg (.getMessage e)]
          (cond
            (.contains msg "Quorum collection for a synchronous transaction is timed out")
            (assoc op :type :info :error msg)
            
            (.contains msg "A rollback for a synchronous transaction is received")
            (assoc op :type :info :error msg)
            
            :else
            (assoc op :type :fail :error msg)))
        (assoc op :type :fail :error :connection-error)))))

  (teardown! [this test]
    (info (:node this) "Tearing down register client")
    (let [{:keys [conn node]} this
          primary  (first (db/primaries test))]
      (when (and conn
                 (not (:leave-db-running? test))
                 (= node primary))
        (info node "Dropping table")
        (try
          (cl/with-conn-failure-retry conn
            (j/execute! conn
              [(format "DROP TABLE IF EXISTS %s" table-name)]))
          (catch Exception e
            (warn node "Error dropping table:" (.getMessage e)))))))

(close! [this test]
  (info (:node this) "Closing register client")
  (when-let [conn (:conn this)]
    (try
      (when (satisfies? java.io.Closeable conn)
        (.close conn))
      (catch Exception e
        (warn (:node this) "Error closing connection:" (.getMessage e)))))))

(defn workload
  [opts]
  {:client      (Client. nil)
   :generator   (independent/concurrent-generator
                  10
                  (range)
                  (fn [k]
                    (->> (gen/mix [w cas])
                         (gen/reserve 5 r)
                         (gen/limit 100))))
   :checker     (independent/checker
                  (checker/compose
                    {:timeline     (timeline/html)
                     :linearizable (checker/linearizable {:model (model/cas-register 0)
                                                          :algorithm :linear})}))})