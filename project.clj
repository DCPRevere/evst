(defproject evst "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[s3-wagon-private "1.3.2"]]

  :repositories {"private" {:url "s3p://hrwd-mvn-repo/releases/"
                            :username      :env/aws_access_key_id
                            :passphrase    :env/aws_secret_access_key}}

  :dependencies [[cheshire "5.8.1"]
                 [clj-time "0.15.1"]
                 [com.geteventstore/eventstore-client_2.12 "5.0.2"]
                 [com.typesafe.akka/akka-actor_2.12 "2.5.13"]
                 [com.typesafe.akka/akka-testkit_2.12 "2.5.13"]
                 [expound "0.7.2"]
                 [org.clojure.gaverhae/okku "0.1.5"]
                 [org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.4.490"]
                 [org.clojure/core.match "0.3.0-alpha5"]
                 [org.clojure/test.check "0.10.0-alpha3"]
                 [org.scala-lang/scala-library "2.12.6"]
                 [respeced "0.0.1"]])
