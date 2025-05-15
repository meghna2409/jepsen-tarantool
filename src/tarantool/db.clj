(ns tarantool.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [next.jdbc :as j]
            [tarantool.client :as cl]
            [jepsen.os.debian :as debian]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.time :as nt]
            [jepsen [core :as jepsen]
                    [control :as c]
                    [db :as db]
                    [util :as util :refer [parse-long]]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def data-dir "/var/lib/tarantool/jepsen")
(def logfile "/var/log/tarantool/jepsen.log")
(def dir     "/opt/tarantool")
(def tarantool-repo
  "Where can we clone tarantool from?"
  "https://github.com/tarantool/tarantool")

(def installer-name "installer.sh")
(def installer-url (str "https://tarantool.io/" installer-name))

(def build-dir
  "A remote directory to clone project and compile."
  "/tmp/jepsen/build")

(def build-file
  "A file we create to track the last built version; speeds up compilation."
  "jepsen-built-version")

(defn node-uri
  "An uri for connecting to a node on a particular port."
  [node port]
  (str "jepsen:jepsen@" (name node) ":" port))

(defn peer-uri
  "An uri for other peers to talk to a node."
  [node]
  (node-uri node 3301))

(defn replica-set
  "Build a Lua table with an URI's instances in a cluster."
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str "'" (peer-uri node) "'")))
       (str/join ",")))

(defn install-build-prerequisites!
  "Installs prerequisite packages for building Tarantool."
  []
  (info "Install prerequisites")
  (debian/install [:autoconf
       :automake
       :build-essential
       :cmake
       :coreutils
       :libtool
       :libreadline-dev
       :libncurses5-dev
       :libssl-dev
       :libunwind-dev
       :libicu-dev
       :make
       :sed
       :zlib1g-dev]))

(defn checkout-repo!
  "Checks out a repo at the given version into a directory in build/ named
  dir. Returns the path to the build directory."
  [repo-url dir version]
  (let [full-dir (str build-dir "/" dir)]
    (when-not (cu/exists? full-dir)
      (c/cd build-dir
            (info "Cloning into" full-dir)
            (c/exec :mkdir :-p build-dir)
            (c/exec :git :clone repo-url dir)))

    (c/cd full-dir
          (try+ (c/exec :git :checkout version)
                (catch [:exit 1] e
                  (if (re-find #"pathspec .+ did not match any file" (:err e))
                    (do 
                        (c/exec :git :fetch)
                        (c/exec :git :checkout version))
                    (throw+ e)))))

    (c/cd full-dir
          (c/exec :git :submodule :update :--init :--recursive))

    full-dir))

(def build-locks
  "We use these locks to prevent concurrent builds."
  (util/named-locks))

(defmacro with-build-version
  "Takes a test, a repo name, a version, and a body. Builds the repo by
  evaluating body, only if it hasn't already been built. Takes out a lock on a
  per-repo basis to prevent concurrent builds. Remembers what version was last
  built by storing a file in the repo directory. Returns the result of body if
  evaluated, or the build directory."
  [node repo-name version & body]
  (util/with-named-lock build-locks [~node ~repo-name]
     (let [build-file# (str build-dir "/" ~repo-name "/" build-file)]
       (if (try+ (= (str ~version) (c/exec :cat build-file#))
                (catch [:exit 1] e# 
                  false))
         (str build-dir "/" ~repo-name)
         (let [res# (do ~@body)]
           (c/exec :echo ~version :> build-file#)
           res#)))))

(defn wait-dpkg-lock! []
  (c/exec :bash :-c
    (str
      "while fuser /var/lib/dpkg/lock >/dev/null 2>&1 || "
      "fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do\n"
      "  echo 'waiting for dpkg lock'\n"
      "  sleep 1\n"
      "done")))

(defn install! [node version]
  (c/su
    (c/exec :sudo :systemctl :disable :--now
          "apt-daily.timer" "apt-daily.service"
          "apt-daily-upgrade.timer" "apt-daily-upgrade.service")
    (wait-dpkg-lock!)
    (c/exec :apt-get :-y :update)
    (wait-dpkg-lock!)
    (c/exec :apt-get :-y :--fix-broken :install)
    (wait-dpkg-lock!)
    (c/exec :apt-get :remove :--purge :-y :tarantool*)
    (wait-dpkg-lock!)
    (c/exec :apt-get :remove :tarantool-common :-y)
    (wait-dpkg-lock!)
    (c/exec :rm :-rf "/var/lib/apt/lists/*")
    (wait-dpkg-lock!)
    (c/exec :apt-get :clean)
    (wait-dpkg-lock!)
    (c/exec :bash :-c
      "rm -f /etc/apt/sources.list.d/*tarantool*.list /etc/apt/sources.list.d/*bizmrg*")
    (wait-dpkg-lock!)
    (c/exec :apt-get :update)
    (wait-dpkg-lock!)
    (c/exec :apt-get :install :-y :gnupg2 :apt-transport-https)
    (wait-dpkg-lock!)
    (c/exec :apt-key :adv
            :--fetch-keys
            "https://download.tarantool.org/tarantool/release/series-2/gpgkey")

    (let [codename (-> (c/exec :lsb_release :-sc)
                      (clojure.string/trim))
          repo-line (str "deb https://download.tarantool.org/"
                        "tarantool/release/series-2/ubuntu/ "
                        codename
                        " main")]
      (info "Writing tarantool.list:" repo-line)
      (c/exec :bash :-c
        (format "echo '%s' > /etc/apt/sources.list.d/tarantool.list"
                repo-line)))

    (wait-dpkg-lock!)
    (c/exec :apt-get :update)
    (wait-dpkg-lock!)
    (try (c/exec :apt-get :-f :install :-y)
         (catch Exception _ (info "Ignoring fix‑broken")))
    (wait-dpkg-lock!)
    (c/exec :apt-get :install :-y :tarantool))

  (c/exec :tarantool :-v))

(defn start!
  "Starts tarantool service"
  [test node]
  (c/su (c/exec :sudo :systemctl :start "tarantool@jepsen")))

(defn restart!
  "Restarts tarantool service"
  [test node]
  (c/su (c/exec :sudo :systemctl :restart "tarantool@jepsen")))

(defn stop!
  "Stops tarantool service"
  [test node]
    (c/su (c/exec :sudo :systemctl :stop "tarantool@jepsen" "||" "true")))

(defn wipe!
  "Removes logs, data files and uninstall package"
  [test node]
  (c/exec :sudo :systemctl :disable :--now
          "apt-daily.timer" "apt-daily.service"
          "apt-daily-upgrade.timer" "apt-daily-upgrade.service")

  (wait-dpkg-lock!)
  (c/exec :sudo :apt-get :-y :remove :tarantool-dev :|| :true)

  (wait-dpkg-lock!)
  (c/exec :sudo :dpkg :--purge :--force-all :tarantool :|| :true)

  (wait-dpkg-lock!)
  (c/exec :sudo :dpkg :--configure :-a :|| :true)

  (c/exec :sudo :rm :-rf logfile (str data-dir "/*")))

(defn is-single-mode?
  [test]
  (let [n (count (:nodes test))]
    (cond
      (= n 1) true
      :else false)))

(defn primaries
  "Return a seq of primaries in a cluster."
  [test]
  (if (= 1 (count (:nodes test)))
    (:nodes test)
    (->> (pmap cl/primary (:nodes test))
         (set)
         (into [])
         (remove nil?))))

(defn calculate-quorum
  "Calculate quorum for a given number of nodes."
  [test]
  (->> (/ (count (:nodes test)) 2)
       (double)
       (Math/round)))

 (defn configure!
   "Configure instance"
   [test node]
  (c/exec :sudo :mkdir  :-p data-dir)
  (c/exec :sudo :chown :-R "tarantool:tarantool" data-dir)  
  (c/exec :sudo :mkdir  :-p "/etc/tarantool/instances.available")
  (c/exec :sudo :mkdir  :-p "/etc/tarantool/instances.enabled")
  (let [conf (-> "tarantool/jepsen.lua" io/resource slurp
                 (str/replace #"%TARANTOOL_QUORUM%"    (str (calculate-quorum test)))
                 (str/replace #"%TARANTOOL_IP_ADDRESS%" node)
                 (str/replace #"%TARANTOOL_REPLICATION%" (replica-set test))
                 (str/replace #"%TARANTOOL_MVCC%"       (str (:mvcc test)))
                 (str/replace #"%TARANTOOL_SINGLE_MODE%" (str (is-single-mode? test)))
                 (str/replace #"%TARANTOOL_DATA_ENGINE%" (:engine test))
                 (str/replace #"%TARANTOOL_DATA_DIR%"    data-dir))]    
    (c/exec :sudo :bash :-c
      (format "cat > /etc/tarantool/instances.enabled/jepsen.lua << 'EOF'\n%s\nEOF"
              conf)))
   (c/exec :sudo :cp "/etc/tarantool/instances.enabled/jepsen.lua"
                    "/etc/tarantool/instances.available"))

(defn db
  "Tarantool DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "Setting up Tarantool" version)
      (c/su
        (wait-dpkg-lock!)
        (c/exec :apt-get :-y :update)
        (wait-dpkg-lock!)
        (c/exec :apt-get :-y :autoremove :tarantool-dev)
        (wait-dpkg-lock!)
        (c/exec :apt-get :-y "--fix-broken" :install))

        (install! node version)
        (configure! test node)
        (c/exec :sudo :systemctl :daemon-reload)
        (c/exec :sudo :systemctl :start "tarantool@jepsen")
        (c/exec :bash :-c
          "until nc -z localhost 3301; do echo 'waiting for tarantool'; sleep 1; done"))

    (teardown! [_ test node]
      (info node "Stopping Tarantool")
      (stop! test node)
      (wipe! test node))

    db/Primary
    (setup-primary! [_ test node])

    (primaries [_ test]
      (primaries test))

    db/Process
    (start! [_ test node]
      (info node :starting :tarantool)
      (c/su
        (start! test node)))

    (kill! [_ test node]
      (info node :stopping :tarantool)
      (c/su
        (cu/grepkill! :kill "tarantool")))

    db/Pause
      (pause!  [_ test node] (c/su (cu/grepkill! :stop "tarantool")))
      (resume! [_ test node] (c/su (cu/grepkill! :cont "tarantool")))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))