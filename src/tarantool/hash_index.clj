(ns tarantool.hash-index
  "Tests for hash index duplicate tuples bug in Tarantool MVCC"
  (:require [clojure.tools.logging :refer [info warn error]]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [jepsen [cli :as cli]
                    [client :as client]
                    [checker :as checker]
                    [control :as c]
                    [generator :as gen]
                    [util :refer [timeout meh]]]
            [next.jdbc :as j]
            [next.jdbc.sql :as sql]
            [tarantool [client :as cl]
                       [db :as db]]))

(def test-script "
-- Initialize transaction proxy for testing
local txn_proxy = {}
txn_proxy.__index = txn_proxy

function txn_proxy:new()
  local obj = {}
  setmetatable(obj, self)
  return obj
end

function txn_proxy:__call(lua_code)
  local fn, err = load(lua_code)
  if fn then
    local ok, result = pcall(fn)
    if ok then
      return result
    else
      return nil, result
    end
  else
    return nil, err
  end
end

-- Return the proxy
return txn_proxy
")

(def hash-index-test-script "
-- Create a test space with hash index
box.schema.space.create('m', {engine = 'memtx'})
box.space.m:create_index('p', {parts = {
  {1, 'uint'},
  {2, 'uint'}}})
box.space.m:create_index('s1', {parts = {
  {3, 'uint', exclude_null = true},
  {2, 'uint'}}})
box.space.m:create_index('s2', {parts = {
  {1, 'uint'},
  {4, 'uint', exclude_null = true}}})
box.space.m:create_index('s3', {parts = {
  {3, 'uint', exclude_null = true},
  {4, 'uint', exclude_null = true}}})
box.space.m:create_index('s4', {type = 'HASH', parts = {
  {1, 'uint'},
  {2, 'uint'}}})

-- Sequence of operations that lead to duplicate tuples
local tx11 = box.begin()
local tx22 = box.begin()
local tx10 = box.begin()
local tx9 = box.begin()
box.space.m:upsert({2, 4, 5, 3}, {{'=', 3, 8}, {'=', 4, box.NULL}})
local tx19 = box.begin()
box.space.m:upsert({6, 1, 2, 5}, {{'=', 3, 4}, {'=', 4, box.NULL}})
local tx26 = box.begin()
box.space.m:insert{8, 6, 6, 7}
local tx31 = box.begin()
local tx25 = box.begin()
tx26:commit()
local tx24 = box.begin()
box.space.m:replace{7, 7, 4, 1}
local tx15 = box.begin()
box.space.m:replace{5, 5, box.NULL, 6}
box.space.m:insert{1, 3, 6, 6}
local tx16 = box.begin()
box.space.m:upsert({3, 8, 4, 7}, {{'=', 3, 1}, {'=', 4, 1}})
box.space.m:insert{2, 1, 3, 5}
box.space.m:upsert({7, 8, 7, 4}, {{'=', 3, 8}, {'=', 4, 6}})
tx24:commit()
box.space.m:upsert({6, 8, 1, box.NULL}, {{'=', 3, 1}, {'=', 4, 5}})
box.space.m:upsert({8, 5, 6, 4}, {{'=', 3, box.NULL}, {'=', 4, 3}})
box.space.m:upsert({6, 7, 3, box.NULL}, {{'=', 3, 5}, {'=', 4, 4}})
tx31:commit()
tx25:commit()
tx10:rollback()
box.space.m:upsert({1, 8, 5, 2}, {{'=', 3, 6}, {'=', 4, 1}})
tx22:commit()
tx15:rollback()
tx19:rollback()
tx16:rollback()
tx9:rollback()

-- Now scan the hash index and check for duplicates
local result = box.space.m.index[4]:select({}, {fullscan = true})
local tuples = {}
local duplicates = {}

for _, tuple in ipairs(result) do
    local tuple_str = table.concat(tuple, ',')
    if tuples[tuple_str] then
        duplicates[tuple_str] = (duplicates[tuple_str] or 1) + 1
    else
        tuples[tuple_str] = true
    end
end

return {result=result, duplicates=duplicates}
")

(defrecord HashIndexClient [conn test-results-atom]
  client/Client
  
  (open! [this test node]
    (let [conn (cl/open node test)]
      (assert conn)
      (assoc this :conn conn :node node)))
  
  (setup! [this test node]
    (info node "Setting up hash index test")
    (let [this (client/open! this test node)
          conn (:conn this)]
      
      ;; Wait for cluster to stabilize
      (Thread/sleep 10000)
      
      ;; Initialize the results atom
      (reset! test-results-atom {:found-duplicates false
                                 :scan-results []
                                 :duplicates {}})
      
      ;; Setup transaction proxy on the primary node
      (when (= node (first (db/primaries test)))
        (info node "Setting up transaction proxy")
        (try
          (j/execute! conn [(str "SELECT LUA('" test-script "')")])
          (catch Exception e
            (warn node "Error setting up transaction proxy:" (.getMessage e)))))
      
      this)))
  
  (invoke! [this test op]
    (case (:f op)
      :test-hash-index
      (try
        ;; Run the test script on the primary node
        (let [results-json (-> (:conn this)
                               (j/execute! [(str "SELECT LUA('" hash-index-test-script "')")])
                               first
                               :COLUMN_1)
              results (json/read-str results-json)
              scan-results (get results "result")
              duplicates (get results "duplicates")]
          
          ;; Check if we found duplicates
          (let [found-duplicates (> (count duplicates) 0)]
            ;; Store results for the report
            (reset! test-results-atom {:found-duplicates found-duplicates
                                       :scan-results scan-results
                                       :duplicates duplicates})
            
            (assoc op :type :ok, 
                   :value {:found-duplicates found-duplicates
                           :scan-results scan-results
                           :duplicates duplicates})))
        (catch Exception e
          (if (.getMessage e)
            (assoc op :type :fail, :error (.getMessage e))
            (assoc op :type :fail, :error :connection-error)))))
  
  (teardown! [this test]
    (info (:node this) "Tearing down hash index test")
    (when-let [conn (:conn this)]
      (when (and (not (:leave-db-running? test))
                 (= (:node this) (first (db/primaries test))))
        (info (:node this) "Dropping test space")
        (try
          (cl/with-conn-failure-retry conn
            (j/execute! conn ["SELECT LUA('pcall(function() box.space.m:drop() end)')"]))
          (catch Exception e
            (warn (:node this) "Error dropping space:" (.getMessage e)))))))
  
  (close! [this test]
    (info (:node this) "Closing hash index client")
    (when-let [conn (:conn this)]
      (try
        (.close conn)
        (catch Exception e
          (warn (:node this) "Error closing connection:" (.getMessage e)))))))

;; Generate an HTML visualization of the hash index scan results
(defn generate-hash-index-report
  "Creates an HTML visualization of the hash index scan results"
  [results]
  (let [found-duplicates (:found-duplicates results)
        scan-results (:scan-results results)
        duplicates (:duplicates results)]
    
    (str "<!DOCTYPE html>
<html>
<head>
    <title>Hash Index Duplicate Tuples Test</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 1000px; margin: 0 auto; }
        h1, h2 { color: #333; }
        .success { color: green; }
        .failure { color: red; font-weight: bold; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        .duplicate { background-color: #ffcccc; font-weight: bold; }
        .conclusion { margin-top: 40px; padding: 20px; background-color: #f0f0f0; border-radius: 5px; }
    </style>
</head>
<body>
    <div class=\"container\">
        <h1>Hash Index Duplicate Tuples Test</h1>
        
        <div class=\"status\">
            <h2>Status: 
                <span class=\"" (if found-duplicates "failure" "success") "\">
                    " (if found-duplicates "DUPLICATES FOUND (Bug Detected)" "NO DUPLICATES FOUND (No Bug Detected)") "
                </span>
            </h2>
        </div>
        
        <p>This test demonstrates the bug where scanning a hash index with MVCC enabled returns the same tuple twice.</p>
        
        <h3>Hash Index Scan Results:</h3>
        <table>
            <tr>
                <th>#</th>
                <th>Tuple</th>
                <th>Status</th>
            </tr>"
            
            (apply str 
                   (map-indexed 
                     (fn [idx tuple]
                       (let [tuple-str (str/join "," tuple)
                             is-duplicate (contains? duplicates tuple-str)]
                         (str "<tr class=\"" (if is-duplicate "duplicate" "") "\">"
                              "<td>" (inc idx) "</td>"
                              "<td>" tuple-str "</td>"
                              "<td>" (if is-duplicate 
                                       (str "<b>DUPLICATE</b> (appears " 
                                            (inc (get duplicates tuple-str)) 
                                            " times)")
                                       "Unique") 
                              "</td>"
                              "</tr>")))
                     scan-results))
            
            "</table>
        
        <div class=\"conclusion\">
            <h3>Bug Description:</h3>
            <p>The bug occurs because of an issue in MVCC's handling of hash indexes during concurrent operations.
            When a transaction reads from a hash index with fullscan=true, it should return each tuple exactly once.
            However, under certain sequences of operations involving multiple transactions (commits, rollbacks, upserts),
            the hash index can return duplicate tuples.</p>
            
            <h3>Impact:</h3>
            <p>Applications that rely on accurate hash index scans may process the same data multiple times,
            leading to incorrect results or data inconsistencies. This can affect operations that rely on
            counting records, aggregations, or other analytical operations that assume each record appears exactly once.</p>
        </div>
    </div>
</body>
</html>")))

;; Custom checker to analyze and visualize the results
(defn hash-index-checker [results-atom]
  (reify checker/Checker
    (check [this test history opts]
      (let [results @results-atom
            html-report (generate-hash-index-report results)
            report-path (str (:dir test) "/hash-index-duplicates.html")]
        
        ;; Write HTML report to file
        (spit report-path html-report)
        
        ;; Check if we found duplicates (which would confirm the bug)
        (let [found-duplicates (:found-duplicates results)]
          
          {:valid? found-duplicates ; The test is valid (successful) if it demonstrates the bug
           :report-path report-path
           :html-report true
           :found-duplicates found-duplicates
           :duplicates (:duplicates results)
           :problem (when found-duplicates
                      "Hash index scan returns duplicate tuples when using MVCC")}))
      )))

(defn workload
  "A workload that tests for duplicate tuples in hash index scans"
  [opts]
  (let [results-atom (atom {})]
    {:client (HashIndexClient. nil results-atom)
     :checker (checker/compose
               {:hash-index (hash-index-checker results-atom)})
     :generator (gen/once {:type :invoke, :f :test-hash-index, :value nil})}))