(ns tarantool.mvcc-gc
  "Tests for MVCC garbage collection issues in Tarantool"
  (:require [clojure.tools.logging :refer [info warn error]]
            [clojure.string :as str]
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
(def log-interval 5)  
(def test-duration 120) 

(defrecord MVCCGarbageClient [conn stats-atom]
  client/Client
  
  (open! [this test node]
    (let [conn (cl/open node test)]
      (assert conn)
      (assoc this :conn conn :node node)))
  
  (setup! [this test node]
    (info node "Setting up MVCC garbage collection test")
    (let [this (client/open! this test node)
          conn (:conn this)]
      
      (Thread/sleep 10000)
      
      (when (= node (first (db/primaries test)))
        (info node "Creating test space and index")
        (try
          (j/execute! conn ["SELECT LUA('box.schema.space.create(\"mvcc_test\"):create_index(\"primary\")')"])
          
          (reset! stats-atom [])
          
          (future
            (try
              (loop [elapsed 0]
                (when (< elapsed test-duration)
                  (try
                    (let [stats-str (-> conn
                                       (j/execute! ["SELECT LUA('local stats = box.stat.memtx.tx().mvcc.tuples; return \"used_stories_count=\" .. stats.used.stories.count .. \";used_stories_total=\" .. stats.used.stories.total .. \";retained_count=\" .. stats.used.retained.count .. \";retained_total=\" .. stats.used.retained.total')"])
                                       first
                                       :COLUMN_1)
                          stats-map (parse-stats stats-str)
                          timestamp (java.time.Instant/now)]
                      
                      (swap! stats-atom conj {:timestamp timestamp
                                              :stats stats-map}))
                    (catch Exception e
                      (error node "Error collecting MVCC stats:" (.getMessage e))))
                  
                  (Thread/sleep (* 1000 log-interval))
                  (recur (+ elapsed log-interval))))
              (catch Exception e
                (error node "Stats collection thread error:" (.getMessage e)))))
          
          (catch Exception e
            (warn node "Error setting up test space:" (.getMessage e)))))
      
      this))
  
  (invoke! [this test op]
    (case (:f op)
      :insert
      (try
        (let [con (cl/open (first (db/primaries test)) test)
              values (:value op)
              prepared-vals (map (fn [i] [i (* i 2)]) values)]
          
          (j/with-transaction [tx con]
            (doseq [[id val] prepared-vals]
              (j/execute! tx ["SELECT LUA('box.space.mvcc_test:replace{" id ", " val "}')"])))
          
          (assoc op :type :ok))
        (catch Exception e
          (if (.getMessage e)
            (assoc op :type :fail, :error (.getMessage e))
            (assoc op :type :fail, :error :connection-error))))
      
      :select-full
      (try
        (let [results (-> (:conn this)
                          (j/execute! ["SELECT LUA('return box.space.mvcc_test:select({}, {fullscan = true})')"])
                          first
                          :COLUMN_1)]
          
          (assoc op :type :ok, :value results))
        (catch Exception e
          (if (.getMessage e)
            (assoc op :type :fail, :error (.getMessage e))
            (assoc op :type :fail, :error :connection-error))))
      
      :get-stats
      (try
        (let [stats-str (-> (:conn this)
                           (j/execute! ["SELECT LUA('local stats = box.stat.memtx.tx().mvcc.tuples; return \"used_stories_count=\" .. stats.used.stories.count .. \";used_stories_total=\" .. stats.used.stories.total .. \";retained_count=\" .. stats.used.retained.count .. \";retained_total=\" .. stats.used.retained.total')"])
                           first
                           :COLUMN_1)
              stats-map (parse-stats stats-str)]
          
          (assoc op :type :ok, :value stats-map))
        (catch Exception e
          (if (.getMessage e)
            (assoc op :type :fail, :error (.getMessage e))
            (assoc op :type :fail, :error :connection-error))))))
  
  (teardown! [this test]
    (info (:node this) "Tearing down MVCC garbage collection test")
    (when-let [conn (:conn this)]
      (when (and (not (:leave-db-running? test))
                 (= (:node this) (first (db/primaries test))))
        (info (:node this) "Dropping test space")
        (try
          (cl/with-conn-failure-retry conn
            (j/execute! conn ["SELECT LUA('box.space.mvcc_test:drop()')"]))
          (catch Exception e
            (warn (:node this) "Error dropping space:" (.getMessage e)))))))
  
  (close! [this test]
    (info (:node this) "Closing MVCC garbage collection client")
    (when-let [conn (:conn this)]
      (try
        (.close conn)
        (catch Exception e
          (warn (:node this) "Error closing connection:" (.getMessage e)))))))

(defn parse-stats [stats-str]
  (let [parts (str/split stats-str #";")
        stats-map (into {} (map (fn [part]
                                  (let [[k v] (str/split part #"=")]
                                    [(keyword k) (Long/parseLong v)]))
                                parts))]
    stats-map))

(defn generate-mvcc-stats-graph
  "Creates an HTML/JS visualization of MVCC stats over time"
  [stats-data]
  (let [timestamps-str (str/join "," (map (fn [entry] 
                                           (-> entry
                                               :timestamp
                                               .toEpochMilli)) 
                                         stats-data))
        stories-count-str (str/join "," (map (fn [entry] 
                                              (get-in entry [:stats :used_stories_count])) 
                                            stats-data))
        retained-count-str (str/join "," (map (fn [entry] 
                                               (get-in entry [:stats :retained_count])) 
                                             stats-data))
        stories-total-str (str/join "," (map (fn [entry] 
                                              (get-in entry [:stats :used_stories_total])) 
                                            stats-data))
        retained-total-str (str/join "," (map (fn [entry] 
                                               (get-in entry [:stats :retained_total])) 
                                             stats-data))]
    
    (str "<!DOCTYPE html>
<html>
<head>
    <title>MVCC Garbage Collection Statistics</title>
    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>
</head>
<body>
    <h1>MVCC Garbage Collection Statistics</h1>
    <div style=\"width: 100%; max-width: 1200px;\">
        <canvas id=\"mvccStatsChart\"></canvas>
    </div>
    <div style=\"width: 100%; max-width: 1200px;\">
        <canvas id=\"mvccMemoryChart\"></canvas>
    </div>
    <script>
        const timestamps = [" timestamps-str "];
        const storiesCount = [" stories-count-str "];
        const retainedCount = [" retained-count-str "];
        const storiesTotal = [" stories-total-str "];
        const retainedTotal = [" retained-total-str "];
        
        // Format timestamps for display
        const labels = timestamps.map(ts => {
            const date = new Date(ts);
            return date.toLocaleTimeString();
        });
        
        // Create chart for tuple counts
        const ctx1 = document.getElementById('mvccStatsChart').getContext('2d');
        new Chart(ctx1, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Stories Count',
                    data: storiesCount,
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1
                }, {
                    label: 'Retained Count',
                    data: retainedCount,
                    borderColor: 'rgb(255, 99, 132)',
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'MVCC Tuples Count Over Time'
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Time'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Count'
                        },
                        beginAtZero: true
                    }
                }
            }
        });
        
        // Create chart for memory usage
        const ctx2 = document.getElementById('mvccMemoryChart').getContext('2d');
        new Chart(ctx2, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Stories Memory (bytes)',
                    data: storiesTotal,
                    borderColor: 'rgb(54, 162, 235)',
                    tension: 0.1
                }, {
                    label: 'Retained Memory (bytes)',
                    data: retainedTotal,
                    borderColor: 'rgb(255, 159, 64)',
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'MVCC Memory Usage Over Time'
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Time'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Memory (bytes)'
                        },
                        beginAtZero: true
                    }
                }
            }
        });
    </script>
</body>
</html>")))

(defn mvcc-gc-stats-checker
  "Checks for MVCC garbage collection issues and generates a report"
  [stats-atom]
  (reify checker/Checker
    (check [this test history opts]
      (let [stats-data @stats-atom
            html-report (generate-mvcc-stats-graph stats-data)
            report-path (str (:dir test) "/mvcc-gc-stats.html")]
        
        (spit report-path html-report)
        
        (let [readings (count stats-data)
              last-reading (last stats-data)
              first-reading (first stats-data)]
          (if (< readings 2)
            {:valid? false
             :error "Not enough data points collected"}
            
            (let [initial-stories-count (get-in first-reading [:stats :used_stories_count])
                  final-stories-count (get-in last-reading [:stats :used_stories_count])
                  initial-stories-total (get-in first-reading [:stats :used_stories_total])
                  final-stories-total (get-in last-reading [:stats :used_stories_total])
                  
                  garbage-growth (- final-stories-count initial-stories-count)
                  memory-growth (- final-stories-total initial-stories-total)
                  
                  gc-problem? (and (> garbage-growth 0) 
                                   (> memory-growth 0))]
              
              {:valid? (not gc-problem?)
               :report-path report-path
               :html-report true
               :stats {:initial {:stories-count initial-stories-count
                                :stories-memory initial-stories-total}
                       :final {:stories-count final-stories-count
                               :stories-memory final-stories-total}
                       :growth {:tuples garbage-growth
                                :memory memory-growth}}
               :problem (when gc-problem?
                          (str "MVCC garbage growth detected: " 
                               garbage-growth " tuples and " 
                               memory-growth " bytes not cleaned up"))})))))))

(defn workload
  "A workload that tests MVCC garbage collection in Tarantool"
  [opts]
  (let [stats-atom (atom [])]
    {:client (MVCCGarbageClient. nil stats-atom)
     :checker (checker/compose
               {:stats (mvcc-gc-stats-checker stats-atom)})
     :generator (gen/phases
                 (->> (fn [] {:type :invoke, :f :insert, :value (range 0 100)})
                      (gen/stagger 1)
                      (gen/limit 50))
                 
                 (->> (fn [] {:type :invoke, :f :select-full, :value nil})
                      (gen/stagger 1/2)
                      (gen/time-limit test-duration))
                 
                 (gen/once {:type :invoke, :f :get-stats, :value nil}))}))