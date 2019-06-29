(ns evst.core-test
  (:require [evst.core :as sut]
            [respeced.test :as rt :refer [check]]
            [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [expound.alpha :as exp])
  (:import [java.util , UUID]))

(create-ns 'evst)
(alias 'evst 'evst)

(create-ns 'evst.external)
(alias 'ext 'evst.external)

(create-ns 'evst.credentials)
(alias 'creds 'evst.credentials)

(create-ns 'evst.settings)
(alias 'sett 'evst.settings)

(create-ns 'evst.position)
(alias 'position 'evst.position)

(create-ns 'evst.stream)
(alias 'stream 'evst.stream)

(create-ns 'evst.event)
(alias 'event 'evst.event)

(create-ns 'evst.event.data)
(alias 'e.data 'evst.event.data)

(create-ns 'evst.event.data.type)
(alias 'e.d.type 'evst.event.data.type)

(create-ns 'evst.event.number)
(alias 'number 'evst.event.number)

(create-ns 'evst.version)
(alias 'version 'evst.version)

(create-ns 'evst.result)
(alias 'result 'evst.result)

(create-ns 'evst.direction)
(alias 'direction 'evst.direction)

(create-ns 'evst.transaction)
(alias 'transaction 'evst.transaction)

(create-ns 'evst.persistent)
(alias 'pers 'evst.persistent)

(create-ns 'evst.persistent.nak)
(alias 'nak 'evst.persistent.nak)

(create-ns 'evst.settings.overflow)
(alias 'of 'evst.settings.overflow)

(create-ns 'evst.settings.cluster)
(alias 'cl 'evst.settings.cluster)



(defn num-tests [n]
  {:clojure.spec.test.check/opts {:num-tests n}})

(t/deftest ->UserCredentials-test
  (t/is (rt/successful? (check `sut/->UserCredentials (num-tests 50)))))

(t/deftest UserCredentials->test
  (t/is (rt/successful? (check `sut/UserCredentials-> (num-tests 50)))))

(t/deftest ->Position-test
  (t/is (rt/successful? (check `sut/->Position (num-tests 50)))))

(t/deftest Position->test
  (t/is (rt/successful? (check `sut/Position-> (num-tests 50)))))

(t/deftest ->EventStream-test
  (t/is (rt/successful? (check `sut/->EventStream (num-tests 50)))))

(t/deftest EventStream->test
  (t/is (rt/successful? (check `sut/EventStream-> (num-tests 50)))))

(t/deftest ->Content-test
  (t/is (rt/successful? (check `sut/->Content (num-tests 50))))

  (t/testing "Can create Content objs"

    (t/testing "Binary"
     (t/testing "string"
       (let [type ::e.d.type/binary
             value "foo"
             o {::e.data/type type, ::e.data/value value}
             expected (eventstore.Content/apply "foo")
             actual (sut/->Content o)]
         (t/is (= expected actual))))

     (t/testing "other"
       (let [type ::e.d.type/binary
             value 5
             o {::e.data/type type, ::e.data/value value}
             expected (eventstore.Content/apply "5")
             actual (sut/->Content o)]
         (t/is (= expected actual)))))

    (t/testing "JSON"
      (t/testing "other"
        (let [type ::e.d.type/json
              value 5
              o {::e.data/type type, ::e.data/value value}
              expected (.apply (eventstore.Content$Json$.) "5")
              actual (sut/->Content o)]
          (t/is (= expected actual)))))))

(t/deftest Content->test
  (t/is (rt/successful? (check `sut/Content-> (num-tests 50))))

  (t/testing "Can decode Content objs"

    (t/testing "JSON"
      (let [s "{\"foo\": \"bar\"}"
            o (.apply (eventstore.Content$Json$.) s)
            value {:foo "bar"}
            expected {::e.data/value value, ::e.data/type ::e.d.type/json}
            actual (sut/Content-> o)]
        (t/is (= expected actual)))

      (let [s "5"
            o (.apply (eventstore.Content$Json$.) s)
            value 5
            expected {::e.data/value value, ::e.data/type ::e.d.type/json}
            actual (sut/Content-> o)]
        (t/is (= expected actual)))

      (let [s "foo"
            o (.apply (eventstore.Content$Json$.) s)
            value "foo"
            expected {::e.data/value value, ::e.data/type ::e.d.type/json}
            actual (sut/Content-> o)]
        (t/is (= expected actual))))

    (t/testing "Binary"
      (let [s "foo"
            o (eventstore.Content/apply s)
            value "foo"
            expected {::e.data/value value, ::e.data/type ::e.d.type/binary}
            actual (sut/Content-> o)]
        (t/is (= expected actual))))))

(t/deftest ->EventData-test
  (t/is (rt/successful? (check `sut/->EventData (num-tests 50)))))

(t/deftest EventData->test
  (t/is (rt/successful? (check `sut/EventData-> (num-tests 50)))))

(t/deftest ->EventNumber-test
  (t/is (rt/successful? (check `sut/->EventNumber (num-tests 50)))))

(t/deftest EventNumber->test
  (t/is (rt/successful? (check `sut/EventNumber-> (num-tests 50)))))

(t/deftest ->EventNumberRange-test
  (t/is (rt/successful? (check `sut/->EventNumberRange (num-tests 50)))))

(t/deftest EventNumberRange->test
  (t/is (rt/successful? (check `sut/EventNumberRange-> (num-tests 50)))))

(t/deftest Event->test
  (t/is (rt/successful? (check `sut/Event-> (num-tests 50)))))

(t/deftest ->ExpectedVersion-test
  (t/is (rt/successful? (check `sut/->ExpectedVersion (num-tests 50)))))

(t/deftest ExpectedVersion->test
  (t/is (rt/successful? (check `sut/ExpectedVersion-> (num-tests 50)))))

(t/deftest WriteResult->test
  (t/is (rt/successful? (check `sut/WriteResult-> (num-tests 50)))))

(t/deftest DeleteResult->test
  (t/is (rt/successful? (check `sut/DeleteResult-> (num-tests 50)))))

(t/deftest ->ReadDirection-test
  (t/is (rt/successful? (check `sut/->ReadDirection (num-tests 50)))))

(t/deftest ReadDirection->test
  (t/is (rt/successful? (check `sut/ReadDirection-> (num-tests 50)))))

(t/deftest ->IdentifyClient-test
  (t/is (rt/successful? (check `sut/->IdentifyClient (num-tests 50)))))

(t/deftest ->WriteEvents-test
  (t/is (rt/successful? (check `sut/->WriteEvents (num-tests 5)))))

;; completed?

(t/deftest ->DeleteStream-test
  (t/is (rt/successful? (check `sut/->DeleteStream (num-tests 50)))))

(t/deftest ->TransactionStart-test
  (t/is (rt/successful? (check `sut/->TransactionStart (num-tests 50)))))

(t/deftest ->TransactionWrite-test
  (t/is (rt/successful? (check `sut/->TransactionWrite (num-tests 5)))))

(t/deftest ->TransactionCommit-test
  (t/is (rt/successful? (check `sut/->TransactionCommit (num-tests 50)))))

(t/deftest ->ReadEvent-test
  (t/is (rt/successful? (check `sut/->ReadEvent (num-tests 50)))))

(t/deftest ReadEventCompleted->test
  (t/is (rt/successful? (check `sut/ReadEventCompleted-> (num-tests 50)))))

(t/deftest ->ReadStreamEvents-test
  (t/is (rt/successful? (check `sut/->ReadStreamEvents (num-tests 50)))))

(t/deftest ReadStreamEventsCompleted->test
  (t/is (rt/successful? (check `sut/ReadStreamEventsCompleted-> (num-tests 50)))))

(t/deftest ->ReadAllEvents-test
  (t/is (rt/successful? (check `sut/->ReadAllEvents (num-tests 50)))))

(t/deftest ReadAllEventsCompleted->test
  (t/is (rt/successful? (check `sut/ReadAllEventsCompleted-> (num-tests 50)))))

(t/deftest ->ConsumerStrategy-test
  (t/is (rt/successful? (check `sut/->ConsumerStrategy (num-tests 50)))))

(t/deftest ->PersistentSubscriptionSettings-test
  (t/is (rt/successful? (check `sut/->PersistentSubscriptionSettings (num-tests 50)))))

(t/deftest ->CreatePersistentSubscription-test
  (t/is (rt/successful? (check `sut/->CreatePersistentSubscription (num-tests 50)))))

(t/deftest ->UpdatePersistentSubscription-test
  (t/is (rt/successful? (check `sut/->UpdatePersistentSubscription (num-tests 50)))))

(t/deftest ->DeletePersistentSubscription-test
  (t/is (rt/successful? (check `sut/->DeletePersistentSubscription (num-tests 50)))))

(t/deftest ->Ack-test
  (t/is (rt/successful? (check `sut/->Ack (num-tests 50)))))

(t/deftest ->Nak-test
  (t/is (rt/successful? (check `sut/->Nak (num-tests 50)))))

(t/deftest ->Connect-test
  (t/is (rt/successful? (check `sut/->Connect (num-tests 50)))))

(t/deftest ->SubscribeTo-test
  (t/is (rt/successful? (check `sut/->SubscribeTo (num-tests 50)))))

(t/deftest ->Unsubscribe-test
  (t/is (rt/successful? (check `sut/->Unsubscribe (num-tests 50)))))

(t/deftest ->ScavengeDatabase-test
  (t/is (rt/successful? (check `sut/->ScavengeDatabase (num-tests 50)))))

(t/deftest ->Authenticate-test
  (t/is (rt/successful? (check `sut/->Authenticate (num-tests 50)))))

(t/deftest ->Cluster-test
  (t/is (rt/successful? (check `sut/->Cluster (num-tests 100)))))

(t/deftest ->HttpSettings-test
  (t/is (rt/successful? (check `sut/->HttpSettings (num-tests 100)))))

(t/deftest ->Settings-test
  (t/is (rt/successful? (check `sut/->Settings (num-tests 100)))))

(t/deftest Settings->test
  (t/is (rt/successful? (check `sut/Settings-> (num-tests 100)))))

(t/deftest ->Settings-test
  (t/is (rt/successful? (check `sut/->Settings (num-tests 100)))))
