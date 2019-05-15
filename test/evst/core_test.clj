(ns evst.core-test
  (:require [evst.core :as sut]
            [respeced.test :as rt :refer [check]]
            [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [expound.alpha :as exp]))

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
  (t/is (rt/successful? (check `sut/->Content (num-tests 50)))))

(t/deftest Content->test
  (t/is (rt/successful? (check `sut/Content-> (num-tests 50)))))

(t/deftest ->EventData-test
  (t/is (rt/successful? (check `sut/->EventData (num-tests 50)))))

(t/deftest EventData->test
  (t/is (rt/successful? (check `sut/EventData-> (num-tests 50)))))

(t/deftest ->EventNumber-test
   (let [res (check `sut/->EventNumber (num-tests 50))]
     (t/is (rt/successful? res) res)))

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
