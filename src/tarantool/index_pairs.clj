(ns tarantool.index-pairs
  "Tests for inconsistent behavior of index.pairs() between memtx and vinyl engines"
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

(def default-timeout 10000)
(def test-duration 60) 

(defrecord IndexPairsClient [conn results-atom]
  client/Client
  
  (open! [this test node]
    (let [conn (cl/open node test)]
      (assert conn)
      (assoc this :conn conn :node node)))
  
  (setup! [this test node]
    (info node "Setting up index pairs test")
    (let [this (client/open! this test node)
          conn (:conn this)]
      
      (Thread/sleep 10000)
      
      (reset! results-atom {:memtx [] :vinyl []})
      
      (when (= node (first (db/primaries test)))
        (info node "Creating test spaces and indices")
        (try
          (j/execute! conn ["SELECT LUA('box.schema.space.create(\"memtx_test\", {engine = \"memtx\"}):create_index(\"primary\")')"])
          (j/execute! conn ["SELECT LUA('box.schema.space.create(\"vinyl_test\", {engine = \"vinyl\"}):create_index(\"primary\")')"])
          (doseq [i (range 1 5)]
            (j/execute! conn [(str "SELECT LUA('box.space.memtx_test:insert{" i ", " i "}')")]))
          
          (doseq [i (range 1 5)]
            (j/execute! conn [(str "SELECT LUA('box.space.vinyl_test:insert{" i ", " i "}')")]))
          
          (catch Exception e
            (warn node "Error setting up test spaces:" (.getMessage e)))))
      
      this))
  
  (invoke! [this test op]
    (case (:f op)
      :test-pairs
      (try
        (let [engine (:engine op)]
          (let [lua-script (str "
            function test_pairs_" engine "()
              local space_name = '" engine "_test'
              local results = {}
              
              -- Get the first two values using pairs iterator
              local gen, param, state = box.space[space_name]:pairs()
              
              -- First iteration
              local state, tuple = gen(param, state)
              table.insert(results, tuple and tuple[1] or nil)
              
              -- Second iteration  
              local state, tuple = gen(param, state)
              table.insert(results, tuple and tuple[1] or nil)
              
              -- Now modify data while iterator is open
              box.space[space_name]:replace{2, 'x'}
              box.space[space_name]:replace{3, 'x'}
              
              -- Continue iterating
              local state, tuple = gen(param, state)
              table.insert(results, tuple and tuple[1] or nil)
              
              -- One more iteration
              local state, tuple = gen(param, state)
              table.insert(results, tuple and tuple[1] or nil)
              
              -- Return collected results
              return require('json').encode(results)
            end
            
            return test_pairs_" engine "()
          ")
                
                results-json (-> (:conn this)
                                 (j/execute! [(str "SELECT LUA('" lua-script "')")])
                                 first
                                 :COLUMN_1)
                results (json/read-str results-json)]
            
            (swap! results-atom assoc engine results)
            
            (assoc op :type :ok, :value {:engine engine, :results results})))
        (catch Exception e
          (if (.getMessage e)
            (assoc op :type :fail, :error (.getMessage e))
            (assoc op :type :fail, :error :connection-error))))
      
      :compare-results
      (try
        (let [memtx-results (:memtx @results-atom)
              vinyl-results (:vinyl @results-atom)
              are-consistent? (= memtx-results vinyl-results)]
          
          (assoc op :type :ok, 
                 :value {:memtx memtx-results
                         :vinyl vinyl-results
                         :consistent? are-consistent?}))
        (catch Exception e
          (if (.getMessage e)
            (assoc op :type :fail, :error (.getMessage e))
            (assoc op :type :fail, :error :connection-error))))))
  
  (teardown! [this test]
    (info (:node this) "Tearing down index pairs test")
    (when-let [conn (:conn this)]
      (when (and (not (:leave-db-running? test))
                 (= (:node this) (first (db/primaries test))))
        (info (:node this) "Dropping test spaces")
        (try
          (cl/with-conn-failure-retry conn
            (j/execute! conn ["SELECT LUA('pcall(function() box.space.memtx_test:drop() end)')"]))
          (cl/with-conn-failure-retry conn
            (j/execute! conn ["SELECT LUA('pcall(function() box.space.vinyl_test:drop() end)')"]))
          (catch Exception e
            (warn (:node this) "Error dropping spaces:" (.getMessage e)))))))
  
  (close! [this test]
    (info (:node this) "Closing index pairs client")
    (when-let [conn (:conn this)]
      (try
        (.close conn)
        (catch Exception e
          (warn (:node this) "Error closing connection:" (.getMessage e)))))))

(defn generate-comparison-report
  "Creates an HTML visualization of the index.pairs() inconsistency"
  [results]
  (let [memtx-results (:memtx results)
        vinyl-results (:vinyl results)
        consistent? (:consistent? results)]
    
    (str "<!DOCTYPE html>
<html>
<head>
    <title>Index.pairs() Behavior Comparison: Memtx vs Vinyl</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { display: flex; flex-direction: column; max-width: 1000px; margin: 0 auto; }
        .engine-container { margin-bottom: 40px; }
        h1, h2 { color: #333; }
        .consistent { color: green; }
        .inconsistent { color: red; font-weight: bold; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        .highlight { background-color: #ffffcc; }
        .diff { background-color: #ffcccc; font-weight: bold; }
        .timeline { position: relative; margin: 40px 0; height: 240px; }
        .event { position: absolute; width: 180px; padding: 10px; border-radius: 5px; border: 1px solid #ccc; background-color: #f9f9f9; }
        .event-1 { left: 0; top: 0; }
        .event-2 { left: 200px; top: 0; }
        .event-3 { left: 400px; top: 0; }
        .event-4 { left: 600px; top: 0; }
        .event-5 { left: 200px; top: 120px; }
        .event-6 { left: 400px; top: 120px; }
        .arrow { position: absolute; height: 0; border-top: 2px solid #333; }
        .arrow::after { content: ''; position: absolute; top: -5px; right: 0; width: 0; height: 0; border: 5px solid transparent; border-left-color: #333; }
        .arrow-1 { width: 180px; left: 20px; top: 40px; }
        .arrow-2 { width: 180px; left: 220px; top: 40px; }
        .arrow-3 { width: 180px; left: 420px; top: 40px; }
        .arrow-4 { width: 0px; left: 310px; top: 60px; border-left: 2px solid #333; height: 50px; }
        .arrow-4::after { top: 50px; right: -4px; transform: rotate(90deg); }
        .arrow-5 { width: 0px; left: 510px; top: 60px; border-left: 2px solid #333; height: 50px; }
        .arrow-5::after { top: 50px; right: -4px; transform: rotate(90deg); }
        .conclusion { margin-top: 40px; padding: 20px; background-color: #f0f0f0; border-radius: 5px; }
    </style>
</head>
<body>
    <div class=\"container\">
        <h1>Index.pairs() Behavior Comparison: Memtx vs Vinyl</h1>
        
        <div class=\"status\">
            <h2>Status: 
                <span class=\"" (if consistent? "consistent" "inconsistent") "\">
                    " (if consistent? "CONSISTENT (No Bug Detected)" "INCONSISTENT (Bug Detected)") "
                </span>
            </h2>
        </div>
        
        <p>This test demonstrates the inconsistent behavior between memtx and vinyl engines when using index.pairs() during concurrent modifications.</p>
        
        <div class=\"timeline\">
            <div class=\"event event-1\">Start iteration with pairs()</div>
            <div class=\"event event-2\">Get first tuple</div>
            <div class=\"event event-3\">Get second tuple</div>
            <div class=\"event event-4\">Get third & fourth tuples</div>
            <div class=\"event event-5\">Modify tuple 2</div>
            <div class=\"event event-6\">Modify tuple 3</div>
            
            <div class=\"arrow arrow-1\"></div>
            <div class=\"arrow arrow-2\"></div>
            <div class=\"arrow arrow-3\"></div>
            <div class=\"arrow arrow-4\"></div>
            <div class=\"arrow arrow-5\"></div>
        </div>
        
        <table>
            <tr>
                <th>Step</th>
                <th>Action</th>
                <th>Memtx Result</th>
                <th>Vinyl Result</th>
                <th>Consistent?</th>
            </tr>
            <tr>
                <td>1</td>
                <td>First iteration value</td>
                <td>" (get memtx-results 0) "</td>
                <td>" (get vinyl-results 0) "</td>
                <td>" (if (= (get memtx-results 0) (get vinyl-results 0)) "Yes" "No") "</td>
            </tr>
            <tr>
                <td>2</td>
                <td>Second iteration value</td>
                <td>" (get memtx-results 1) "</td>
                <td>" (get vinyl-results 1) "</td>
                <td>" (if (= (get memtx-results 1) (get vinyl-results 1)) "Yes" "No") "</td>
            </tr>
            <tr>
                <td>3</td>
                <td>Modify tuples 2 and 3</td>
                <td colspan=\"3\">Space modifications during iteration</td>
            </tr>
            <tr class=\"" (if (= (get memtx-results 2) (get vinyl-results 2)) "" "diff") "\">
                <td>4</td>
                <td>Third iteration value (after modification)</td>
                <td>" (get memtx-results 2) "</td>
                <td>" (get vinyl-results 2) "</td>
                <td>" (if (= (get memtx-results 2) (get vinyl-results 2)) "Yes" "<b>No</b>") "</td>
            </tr>
            <tr class=\"" (if (= (get memtx-results 3) (get vinyl-results 3)) "" "diff") "\">
                <td>5</td>
                <td>Fourth iteration value (after modification)</td>
                <td>" (get memtx-results 3) "</td>
                <td>" (get vinyl-results 3) "</td>
                <td>" (if (= (get memtx-results 3) (get vinyl-results 3)) "Yes" "<b>No</b>") "</td>
            </tr>
        </table>
        
        <div class=\"conclusion\">
            <h3>Bug Description:</h3>
            <p>The inconsistency occurs because in vinyl, the pairs() iterator is sent to a read view on a conflict, 
            while in memtx it is not. This means that vinyl will continue to show a snapshot of the data from when the 
            iterator started, while memtx will show updated data. The bug is particularly problematic because 
            index.select() is implemented via index.pairs(), so these inconsistencies can impact query results.</p>
            
            <h3>Impact:</h3>
            <p>Applications that rely on consistent behavior between engines could experience different results depending 
            on the engine used. This could lead to data inconsistencies, especially in applications that switch between 
            engines or that use both engines in the same database.</p>
        </div>
    </div>
</body>
</html>")))

(defn index-pairs-checker [results-atom]
  (reify checker/Checker
    (check [this test history opts]
      (let [results @results-atom
            html-report (generate-comparison-report results)
            report-path (str (:dir test) "/index-pairs-comparison.html")]
        
        (spit report-path html-report)
        
        (let [memtx-results (:memtx results)
              vinyl-results (:vinyl results)
              consistent? (= memtx-results vinyl-results)]
          
          {:valid? consistent? 
           :report-path report-path
           :html-report true
           :consistent? consistent?
           :memtx-results memtx-results
           :vinyl-results vinyl-results
           :problem (when (not consistent?)
                      "Inconsistent behavior detected between memtx and vinyl with index.pairs() during modifications")}))
      )))

(defn workload
  "A workload that tests index.pairs() inconsistency between memtx and vinyl"
  [opts]
  (let [results-atom (atom {:memtx [] :vinyl []})]
    {:client (IndexPairsClient. nil results-atom)
     :checker (checker/compose
               {:index-pairs (index-pairs-checker results-atom)})
     :generator (gen/phases
                 (gen/once {:type :invoke, :f :test-pairs, :value nil, :engine "memtx"})
                 
                 (gen/once {:type :invoke, :f :test-pairs, :value nil, :engine "vinyl"})
                 
                 (gen/once {:type :invoke, :f :compare-results, :value nil}))}))