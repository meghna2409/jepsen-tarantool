(ns tarantool.bank
  "Simulates transfers between bank accounts."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.core.reducers :as r]
            [jepsen [cli :as cli]
                    [client :as client]
                    [checker :as checker]
                    [control :as c]
                    [generator :as gen]]
            [jepsen.tests.bank :as bank]
            [next.jdbc :as j]
            [next.jdbc.sql :as sql]
            [knossos.op :as op]
            [jepsen.checker.timeline :as timeline]
            [tarantool [client :as cl]
                       [db :as db]]))
(def bank-initialized? (atom false))
(def table-name "accounts")
(def err-unable-to-distribute-balances (str
  "Unable to distribute initial balances uniformly "
  "for given total-amount and accounts"))

(defrecord BankClientWithLua [conn]
  client/Client

  (open! [this test node]
    (let [conn (cl/open node test)]
      (assoc this :conn conn :node node)))

  (setup! [this test node]
    (locking BankClientWithLua
      (let [this (client/open! this test node)
            conn (:conn this)
            initial-balance-per-account (/ (:total-amount test)
                                          (count (:accounts test)))]
        (assert (integer? initial-balance-per-account)
                err-unable-to-distribute-balances)
        (Thread/sleep 10000) 
        
        (when (= node (first (db/primaries test)))
          (cl/with-conn-failure-retry conn
            (info node "Creating table" table-name)
            (j/execute! conn [(str "CREATE TABLE IF NOT EXISTS " table-name
                              "(id INT NOT NULL PRIMARY KEY,
                              balance INT NOT NULL)")])
            (Thread/sleep 2000)
            (let [tbl (clojure.string/upper-case table-name)]
              (j/execute! conn
                [(str "SELECT LUA('local s = box.space." tbl
                      "; if s then s:alter{is_sync=true}; return true else return false; end')")]))
            
            (doseq [a (:accounts test)]
              (info node "Populating account" a)
              (sql/insert! conn table-name
                {:id a
                 :balance initial-balance-per-account}))))
        
        (Thread/sleep 5000)
        this)))

(invoke! [this test op]
  (try
    (case (:f op)
      :read (try
              (->> (sql/query (:conn this) [(str "SELECT * FROM " table-name)])
                   (map (juxt :ID :BALANCE))
                   (into (sorted-map))
                   (assoc op :type :ok, :value))
              (catch Exception e
                (assoc op :type :info, :error :connection-failed)))

      :transfer
      (try
        (let [{:keys [from to amount]} (:value op)
              con (try 
                    (cl/open (first (db/primaries test)) test)
                    (catch Exception _ nil))
              table (clojure.string/upper-case table-name)]
          (if con
            (let [r (-> con
                       (sql/query [(str "SELECT _WITHDRAW('" table "'," from "," to "," amount ")")])
                       first
                       :COLUMN_1)]
              (if (false? r)
                (assoc op :type :fail, :value {:from from :to to :amount amount})
                (assoc op :type :ok)))
            (assoc op :type :info, :error :connection-failed)))
        (catch Exception e
          (assoc op :type :info, :error :connection-failed))))
    
    (catch Exception e
      (assoc op :type :info, :error (if (.getMessage e)
                                     (.getMessage e)
                                     :connection-error)))))
      
      (catch Exception e
        (if (.getMessage e) 
          (assoc op :type :fail, :error (.getMessage e))
          (assoc op :type :fail, :error :connection-error))))

  (teardown! [this test]
    (info (:node this) "Tearing down bank client")
    (when-let [conn (:conn this)]
      (when (and (not (:leave-db-running? test))
                 (= (:node this) (first (db/primaries test))))
        (info (:node this) "Dropping table" table-name)
        (try
          (cl/with-conn-failure-retry conn
            (j/execute! conn [(str "DROP TABLE IF EXISTS " table-name)]))
          (catch Exception e
            (warn (:node this) "Error dropping table:" (.getMessage e)))))))

  (close! [this test]
    (info (:node this) "Closing bank client")
    (when-let [conn (:conn this)]
      (try
        (when (and (instance? java.io.Closeable conn)
                   (.getClass conn))
          (.close conn))
        (catch Exception e
          (warn (:node this) "Error closing connection:" (.getMessage e))))))

(defrecord MultiBankClientWithLua [conn tbl-created?]
  client/Client
  
  (open! [this test node]
    (assoc this :conn (cl/open node test)))

  (setup! [this test node]
    (locking tbl-created?
      (let [this (client/open! this test node)
            conn (:conn this)
            initial-balance-per-account (/ (:total-amount test)
                                          (count (:accounts test)))]
        (assert (integer? initial-balance-per-account)
                err-unable-to-distribute-balances)
        (Thread/sleep 10000) 
        
        (when (= node (first (db/primaries test)))
          (when (compare-and-set! tbl-created? false true)
            (cl/with-conn-failure-retry conn
              (doseq [a (:accounts test)]
                (info node "Creating table" table-name a)
                (j/execute! conn [(str "CREATE TABLE IF NOT EXISTS " table-name a
                                       "(id             INT NOT NULL PRIMARY KEY,"
                                       "account_id      INT NOT NULL,"
                                       "balance INT NOT NULL)")])
                (j/execute! conn [(str "SELECT LUA('local s = box.space."
                                       (clojure.string/upper-case (str table-name a))
                                       "; if s then s:alter{is_sync=true}; return true; else return false; end')")])
                (info node "Populating account" a)
                (sql/insert! conn (str table-name a)
                  {:id 0
                   :account_id a
                   :balance initial-balance-per-account})))))
        
        (Thread/sleep 5000)
        this)))

  (invoke! [this test op]
    (try
      (case (:f op)
        :read
        (let [union-queries (map #(str "SELECT ACCOUNT_ID, BALANCE FROM " 
                                      (str (clojure.string/upper-case table-name) %))
                                 (:accounts test))
              query-str (str/join " UNION " union-queries)]
          (->> (sql/query (:conn this) [query-str])
               (map (juxt :ACCOUNT_ID :BALANCE))
               (into (sorted-map))
               (assoc op :type :ok, :value)))

        :transfer
        (let [{:keys [from to amount]} (:value op)
              table_from    (str (clojure.string/upper-case table-name) from)
              table_to      (str (clojure.string/upper-case table-name) to)
              con           (cl/open (first (db/primaries test)) test)
              r (-> con
                    (sql/query [(str "SELECT _WITHDRAW_MULTITABLE('" table_from "','" table_to "'," amount ")")])
                    first
                    :COLUMN_1)]
          (if (false? r)
            (assoc op :type :fail)
            (assoc op :type :ok))))
      
      (catch Exception e
        (cond
          (and (.getMessage e) 
               (.contains (.getMessage e) "Couldn't initiate connection"))
          (do
            (try
              (let [new-conn (cl/open (first (db/primaries test)) test)]
                (info (:node this) "Reconnected after connection failure")
                (assoc this :conn new-conn))
              (catch Exception _
                (assoc op :type :fail :error (.getMessage e))))
            (assoc op :type :fail :error (.getMessage e)))
          
          (.getMessage e)
          (assoc op :type :fail :error (.getMessage e))
          
          :else
          (assoc op :type :fail :error :connection-error)))))

  (teardown! [this test]
    (when-not (:leave-db-running? test)
      (when-let [conn (:conn this)]
        (try
          (cl/with-conn-failure-retry conn
            (doseq [a (:accounts test)]
              (info (:node this) "Drop table" table-name a)
              (try
                (j/execute! conn [(str "DROP TABLE IF EXISTS " table-name a)])
                (catch Exception e
                  (warn (:node this) "Error dropping table" table-name a ":" (.getMessage e))))))
          (catch Exception e
            (warn (:node this) "Error dropping tables:" (.getMessage e)))))))

  (close! [this test]
    (info (:node this) "Closing multitable bank client")
    (when-let [conn (:conn this)]
      (try
        (.close conn)
        (catch Exception e
          (warn (:node this) "Error closing connection:" (.getMessage e)))))))

(defrecord BankClient [conn]
  client/Client

  (open! [this test node]
    (let [conn (cl/open node test)]
      (assoc this :conn conn :node node)))

(setup! [this test node]
  (let [this (client/open! this test node)
        conn (:conn this)
        initial-balance-per-account (/ (:total-amount test)
                                      (count (:accounts test)))]
    
    (assert (integer? initial-balance-per-account)
            err-unable-to-distribute-balances)
    
    (Thread/sleep 10000) 
    (when (and (= node (first (db/primaries test)))
               (compare-and-set! bank-initialized? false true))
      (cl/with-conn-failure-retry conn
        (info node "Creating table" table-name)
        (j/execute! conn 
                   [(str "CREATE TABLE IF NOT EXISTS " table-name 
                        "(id INT NOT NULL PRIMARY KEY, balance INT NOT NULL)")])
        
        (j/execute! conn 
                   [(str "SELECT LUA('local s = box.space."
                        (clojure.string/upper-case table-name)
                        "; if s then s:alter{is_sync=true}; return true; else return false; end')")])
        
        (doseq [a (:accounts test)]
          (info node "Populating account" a)
          (sql/insert! conn table-name
                      {:id a
                       :balance initial-balance-per-account}))))
    
    (Thread/sleep 5000)
    this))

(invoke! [this test op]
  (try
    (case (:f op)
      :read (->> (sql/query (:conn this) [(str "SELECT * FROM " table-name)])
                 (map (juxt :ID :BALANCE))
                 (into (sorted-map))
                 (assoc op :type :ok, :value))

      :transfer
      (let [{:keys [from to amount]} (:value op)
            conn (:conn this) 
            b1 (-> conn
                   (sql/query [(str "SELECT * FROM " table-name " WHERE id = ? ") from])
                   first
                   :BALANCE
                   (- amount))
            b2 (-> conn
                   (sql/query [(str "SELECT * FROM " table-name " WHERE id = ? ") to])
                   first
                   :BALANCE
                   (+ amount))]
        (cond (or (neg? b1) (neg? b2))
              (assoc op :type :fail, :value {:from from :to to :amount amount})
              true
              (do
                (j/execute! conn [(str "UPDATE " table-name " SET balance = balance - ? WHERE id = ?") amount from])
                (j/execute! conn [(str "UPDATE " table-name " SET balance = balance + ? WHERE id = ?") amount to])
                (assoc op :type :ok)))))
    
    (catch Exception e
      (if (.getMessage e) 
        (assoc op :type :fail, :error (.getMessage e))
        (assoc op :type :fail, :error :connection-error)))))

(teardown! [this test]
  (info (:node this) "Tearing down bank client")
  (when-let [conn (:conn this)]
    (when (and (not (:leave-db-running? test))
               (= (:node this) (first (db/primaries test))))
      (info (:node this) "Dropping table" table-name)
      (try
        (let [conn-var conn]
          (cl/with-conn-failure-retry conn-var
            (j/execute! conn-var [(str "DROP TABLE IF EXISTS " table-name)])))
        (catch Exception e
          (warn (:node this) "Error dropping table:" (.getMessage e)))))))

  (close! [this test]
    (info (:node this) "Closing bank client")
    (when-let [conn (:conn this)]
      (try
        (.close conn)
        (catch Exception e
          (warn (:node this) "Error closing connection:" (.getMessage e)))))))

(defrecord MultiBankClient [conn tbl-created?]
  client/Client
  
  (open! [this test node]
    (assoc this :conn (cl/open node test)))

  (setup! [this test node]
    (locking tbl-created?
      (let [this (client/open! this test node)
            conn (:conn this)
            initial-balance-per-account (/ (:total-amount test)
                                          (count (:accounts test)))]
        (assert (integer? initial-balance-per-account)
                err-unable-to-distribute-balances)
        (Thread/sleep 10000) 
        (when (= node (first (db/primaries test)))
          (when (compare-and-set! tbl-created? false true)
            (cl/with-conn-failure-retry conn
              (doseq [a (:accounts test)]
                (info node "Creating table" table-name a)
                (j/execute! conn [(str "CREATE TABLE IF NOT EXISTS " table-name a
                                       "(id     INT NOT NULL PRIMARY KEY,"
                                       "balance INT NOT NULL)")])
                (j/execute! conn [(str "SELECT LUA('local s = box.space."
                                       (clojure.string/upper-case (str table-name a))
                                       "; if s then s:alter{is_sync=true}; end')")])
                (info node "Populating account" a)
                (sql/insert! conn (str table-name a)
                  {:id 0
                   :balance initial-balance-per-account})))))
        
        (Thread/sleep 5000)
        this)))

  (invoke! [this test op]
    (try
      (case (:f op)
        :read
        (->> (:accounts test)
             (map (fn [x]
                    [x (->> (sql/query (:conn this) [(str "SELECT balance FROM " table-name x)]
                                     {:row-fn :BALANCE})
                            first)]))
             (into (sorted-map))
             (map (fn [[k {b :BALANCE}]] [k b]))
             (into {})
             (assoc op :type :ok, :value))

        :transfer
        (let [{:keys [from to amount]} (:value op)
              from (str table-name from)
              to   (str table-name to)
              con  (cl/open (first (db/primaries test)) test)
              b1 (-> con
                     (sql/query [(str "SELECT balance FROM " from)])
                     first
                     :BALANCE
                     (- amount))
              b2 (-> con
                     (sql/query [(str "SELECT balance FROM " to)])
                     first
                     :BALANCE
                     (+ amount))]
          (cond (neg? b1)
                (assoc op :type :fail, :error [:negative from b1])
                (neg? b2)
                (assoc op :type :fail, :error [:negative to b2])
                true
                (do (j/execute! con [(str "UPDATE " from " SET balance = balance - ? WHERE id = 0") amount])
                    (j/execute! con [(str "UPDATE " to " SET balance = balance + ? WHERE id = 0") amount])
                    (assoc op :type :ok)))))
      
      (catch Exception e
        (if (.getMessage e) 
          (assoc op :type :fail, :error (.getMessage e))
          (assoc op :type :fail, :error :connection-error)))))

  (teardown! [this test]
    (when-not (:leave-db-running? test)
      (when-let [conn (:conn this)]
        (try
          (doseq [a (:accounts test)]
            (info (:node this) "Drop table" table-name a)
            (j/execute! conn [(str "DROP TABLE IF EXISTS " table-name a)]))
          (catch Exception e
            (warn (:node this) "Error dropping tables:" (.getMessage e)))))))

  (close! [this test]
    (info (:node this) "Closing multitable bank client")
    (when-let [conn (:conn this)]
      (try
        (.close conn)
        (catch Exception e
          (warn (:node this) "Error closing connection:" (.getMessage e)))))))

(defn workload
  [opts]
  (assoc (bank/test opts)
         :client (BankClient. nil)))

(defn multitable-workload
  [opts]
  (assoc (workload opts)
         :client (MultiBankClient. nil (atom false))))

(defn workload-lua
  [opts]
  (assoc (workload opts)
         :client (BankClientWithLua. nil)))

(defn multitable-workload-lua
  [opts]
  (assoc (workload opts)
         :client (MultiBankClientWithLua. nil (atom false))))