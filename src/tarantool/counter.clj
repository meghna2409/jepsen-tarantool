(ns tarantool.counter
  (:require [jepsen.client        :as client]
            [jepsen.generator     :as gen]
            [jepsen.independent   :as independent]
            [jepsen.checker       :as checker]
            [jepsen.checker.timeline :as timeline]
            [clojure.tools.logging :refer [info warn]]
            [next.jdbc            :as j]
            [tarantool.client     :as cl]
            [tarantool.db         :as db]))

(def table "counter")

(defrecord CounterClient [conn node]
  client/Client

  (open! [_ test node]
    (let [c (cl/open node test)]
      (assert c "tarantool.client.open returned nil!")
      (assoc (CounterClient. c node)
            :conn c :node node)))


  (setup! [this test node]
    (let [this (client/open! this test node)      
          conn (:conn this)]
      (Thread/sleep 10000)                       
      (when (= node (first (db/primaries test)))  
        (j/execute! conn
          [(format "CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, cnt INT)" table)])
        (j/execute! conn
          [(format "INSERT OR IGNORE INTO %s VALUES (0,0)" table)]))
      this))          

(invoke! [this test op]
  (let [conn (:conn this)
        f (:f op)]
    (try
      (if (= f :add)
        (do
          (j/execute! conn
            [(str "UPDATE " table " SET cnt = cnt + ? WHERE id = 0")
             (:value op)])
          (assoc op :type :ok))
        (let [v (-> (j/execute! conn [(str "SELECT cnt FROM " table " WHERE id = 0")])
                   first :cnt)]
          (assoc op :type :ok :value v)))
      (catch Exception e
        (warn "counter op failed" (.getMessage e))
        (assoc op :type :fail :error (.getMessage e))))))
          
  (teardown! [this test]
    (when (and (= (:node this) (first (db/primaries test)))
               (not (:leave-db-running? test)))
      (j/execute! (:conn this) [(str "DROP TABLE IF EXISTS " table)])))

  (close!   [this _] (some-> (:conn this) .close)))

(def add-op  {:type :invoke :f :add  :value 1})
(def read-op {:type :invoke :f :read})

(defn with-index [g]
  (let [i (atom 0)]
    (gen/map #(assoc % :op-index (swap! i inc)) g)))

(defn workload-inc [_opts]
  {:client  (CounterClient. nil nil)
   :checker (checker/compose
              {:timeline (timeline/html)
               :counter  (checker/counter)})
   :generator (with-index
                (gen/stagger 0.1
                  (gen/mix (concat (repeat 90 add-op) (repeat 10 read-op)))))})
