(ns evst.core
  (:require
   [cheshire.core :as json]
   [clj-time.coerce :as c]
   [clj-time.core :as t]
   [clojure.core.match :as match]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.test.alpha :as stest]
   [evst.utils :as u]
   [expound.alpha :as exp]
   [clojure.set :as set])
  (:use [clojure.core.match.regex])
  (:import [java.util , UUID]
           [scala , Option]
           [scala.concurrent.duration Duration]
           [scala.collection , JavaConversions]
           [eventstore.j , DeleteStreamBuilder]))

(create-ns 'evst)
(alias 'evst 'evst)

(create-ns 'evst.external)
(alias 'ext 'evst.external)

(create-ns 'evst.credentials)
(alias 'creds 'evst.credentials)

(create-ns 'evst.settings)
(alias 'sett 'evst.settings)

(s/def ::creds/login (s/and string? seq))
(s/def ::creds/password (s/and string? seq))
(s/def ::evst/credentials (s/keys :req [::creds/login ::creds/password]))

(s/def ::ext/credentials
  (s/with-gen #(instance? eventstore.UserCredentials %)
    (fn [] (gen/fmap (fn [[x y]] (eventstore.UserCredentials. x y))
                    (s/gen (s/cat :login ::creds/login :pass ::creds/password))))))

(defn ->UserCredentials
  ([] (eventstore.UserCredentials/DefaultAdmin))
  ([{::creds/keys [login password]}]
     (eventstore.UserCredentials. login password)))

(s/fdef ->UserCredentials
  :args (s/alt :default (s/cat)
               :custom ::evst/credentials)
  :ret ::ext/credentials)

(defn UserCredentials-> [o]
  {::creds/login (.login o)
   ::creds/password (.password o)})

(s/fdef UserCredentials->
  :args (s/cat :credentials ::ext/credentials)
  :ret ::evst/credentials
  :fn (fn [x] (let [input (-> x :args :credentials)
                   ret (-> x :ret)]
               (and (= (.login input) (::creds/login ret))
                    (= (.password input) (::creds/password ret))))))

(def default-credentials
  (-> (->UserCredentials) UserCredentials->))



;; Position

(create-ns 'evst.position)
(alias 'position 'evst.position)

(s/def ::position/last #{::position/last})

(s/def ::position/commit int?)

(s/def ::position/prepare int?)

(s/def ::position/map
  (s/and (s/keys :req [::position/commit ::position/prepare])
         #(>= (% ::position/commit)
             (% ::position/prepare))))

(s/def ::position/exact
  (s/or :first #{::position/first}
        :num nat-int?
        :map ::position/map))

(s/def ::evst/position
  (s/or :last ::position/last
        :exact ::position/exact))

(s/def ::ext/position
  (s/with-gen #(isa? (class %) eventstore.Position)
    (fn [] (gen/fmap (fn [[p d]] (eventstore.Position/apply (+ p d) p))
                    (s/gen (s/tuple int? pos-int?))))))

(s/def ::ext/position-exact (s/and ::ext/position #(not= % (eventstore.Position/apply -1))))

(defn ->Position [o]
  (match/match
   o   ::position/first (eventstore.Position/First)
   ,   ::position/last  (eventstore.Position$Last$.)
   ,   {::position/commit c
   ,    ::position/prepare p} (eventstore.Position/apply c p)
   ,   :else (eventstore.Position/apply o)))

(s/fdef ->Position
  :args (s/cat :position ::evst/position)
  :ret #(instance? eventstore.Position %))



(defmulti Position-> class)

(defmethod Position-> eventstore.Position$Last$ [o] ::position/last)

(defmethod Position-> eventstore.Position$Exact [o]
  {::position/commit (.commitPosition o)
   ::position/prepare (.preparePosition o)})

(s/fdef Position->
  :args (s/cat :position ::ext/position)
  :ret ::evst/position)



;; Streams

(create-ns 'evst.stream)
(alias 'stream 'evst.stream)

(s/def ::stream/all
  (s/and (s/keys :req [::stream/type ::stream/system? ::stream/metadata?])
         #(= (::stream/type %) ::stream/all)))

(s/def ::stream/has-id
  (s/and
   (s/keys :req [::stream/type ::stream/system? ::stream/metadata?
                 ::stream/id ::stream/value ::stream/prefix]
           :opt [::stream/metadata])
   #(not= (::stream/type %) ::stream/all)
   #(seq (::stream/id %))))

(s/def ::evst/stream
  (s/or :all ::stream/all
        :id ::stream/has-id))

(s/def ::stream/type
  #{::stream/all ::stream/system ::stream/plain ::stream/metadata ::stream/undefined})

(s/def ::stream/system? boolean?)

(s/def ::stream/metadata? boolean?)

(s/def ::stream/id (s/and string? seq))

(s/def ::stream/value string?)

(s/def ::stream/prefix string?)

(s/def ::stream/metadata ::evst/stream)

(defn stream-of [id]
  (match/match [id]
               [(:or "" nil)]
               ,   {::stream/type ::stream/all, ::stream/system? true, ::stream/metadata? false}
               [#"\$\$.*"]
               ,   {::stream/type ::stream/metadata
                    ::stream/system? false
                    ::stream/metadata? true
                    ::stream/id id
                    ::stream/prefix "$$"
                    ::stream/value (subs id 2)}
               [#"\$.*"]
               ,   {::stream/type ::stream/system
                    ::stream/system? true
                    ::stream/metadata? false
                    ::stream/id id
                    ::stream/prefix "$"
                    ::stream/value (subs id 1)}
               :else
               ,   {::stream/type ::stream/plain
                    ::stream/system? false
                    ::stream/metadata? false
                    ::stream/id id
                    ::stream/prefix ""
                    ::stream/value id}))

(defmulti ->EventStream class)

(defmethod ->EventStream java.lang.String
  [stream-id]
  (eventstore.EventStream/apply stream-id))

(defmethod ->EventStream java.util.Map
  [{::stream/keys [type id]}]
  (if (= type ::stream/all)
    (eventstore.EventStream$All$.)
    (eventstore.EventStream/apply id)))

(s/def ::ext/stream
  (s/with-gen #(instance? eventstore.EventStream %)
    (fn [] (gen/fmap (fn [name] (eventstore.EventStream/apply name))
                    (s/gen string?)))))

(s/def ::ext/stream-id
  (s/with-gen #(instance? eventstore.EventStream %)
    (fn [] (gen/fmap (fn [name] (eventstore.EventStream/apply name))
                    (s/gen (s/and string? seq))))))

(s/fdef ->EventStream
  :args (s/cat :stream ::evst/stream)
  :ret ::ext/stream)



(defmulti EventStream-> class)

(defmethod EventStream-> eventstore.EventStream$HasMetadata [o]
  {::stream/type (if (.isSystem o) ::stream/system ::stream/plain)
   ::stream/system? (.isSystem o)
   ::stream/metadata? (.isMetadata o)
   ::stream/id (.streamId o)
   ::stream/value (.value o)
   ::stream/prefix (.prefix o)
   ::stream/metadata (EventStream-> (.metadata o))})

(defmethod EventStream-> eventstore.EventStream$Id [o]
  {::stream/type (if (.isMetadata o) ::stream/metadata ::stream/undefined)
   ::stream/system? (.isSystem o)
   ::stream/metadata? (.isMetadata o)
   ::stream/id (.streamId o)
   ::stream/value (.value o)
   ::stream/prefix (.prefix o)})

(defmethod EventStream-> eventstore.EventStream [o]
  {::stream/type ::stream/all
   ::stream/system? (.isSystem o)
   ::stream/metadata? (.isMetadata o)})

(s/fdef EventStream->
  :args (s/cat :stream ::ext/stream)
  :ret ::evst/stream)



;; Event data

(create-ns 'evst.event)
(alias 'event 'evst.event)

(s/def ::event/type (s/and string? seq))
(s/def ::event/id uuid?)

(create-ns 'evst.event.data)
(alias 'e.data 'evst.event.data)

(create-ns 'evst.event.data.type)
(alias 'e.d.type 'evst.event.data.type)

(s/def ::e.data/value any?)
(s/def ::e.data/type #{::e.d.type/json ::e.d.type/binary})
(s/def ::event/data (s/keys :req [::e.data/value ::e.data/type]))
(s/def ::event/metadata ::event/data)



;; Content

(s/def ::ext/content-bytestring
  (s/with-gen #(instance? eventstore.Content %)
    (fn [] (gen/fmap #(eventstore.Content/apply %) (s/gen string?)))))

(s/def ::ext/content-json
  (s/with-gen #(instance? eventstore.Content %)
    (fn [] (gen/fmap #(.apply (eventstore.Content$Json$.) %)
                    (s/gen string?)))))

(s/def ::ext/content
  (s/or :bytestring ::ext/content-bytestring
        :json ::ext/content-json))



(defmulti ->Content ::e.data/type)

(defmethod ->Content ::e.d.type/binary [m]
  (eventstore.Content/apply (-> m ::e.data/value str)))

(defmethod ->Content ::e.d.type/json [m]
  (.apply (eventstore.Content$Json$.) (json/generate-string (::e.data/value m))))

(defmethod ->Content :default [m]
  (->Content {::e.data/type ::e.d.type/json
             ::e.data/value m}))

(s/fdef ->Content
  :args (s/cat :content ::event/data)
  :ret ::ext/content)



(defmulti Content->
  (fn [o]
    (if (nil? o) o
        (class (.contentType o)))))

(defmethod Content->
  eventstore.ContentType$Json$ [o]
  {::e.data/value
   (let [s (.utf8String (.value o))]
     (try (json/parse-string s true)
         (catch com.fasterxml.jackson.core.JsonParseException e s)))
   ::e.data/type ::e.d.type/json})

(defmethod Content->
  :default [o]
  {::e.data/value (.utf8String (.value o))
   ::e.data/type ::e.d.type/binary})

(s/fdef Content->
  :args (s/cat :content ::ext/content)
  :ret ::event/data)



;; Event data

(s/def ::evst/event-data (s/keys :req [::event/type ::event/id] :opt [::event/data ::event/metadata]))

(s/def ::ext/event-data
  (s/with-gen #(instance? eventstore.EventData %)
    (fn [] (gen/fmap (fn [[type id data metadata]]
                      (.apply (eventstore.EventData$Json$.) type id data metadata))
                    (s/gen (s/tuple (s/and string? seq) uuid? string? string?))))))

(defn ->EventData [{::event/keys [type id data metadata]}]
  (let [b (eventstore.j.EventDataBuilder. type)]
    (cond-> b
      (some? id)       (.eventId id)
      (some? data)     (.data (->Content data))
      (some? metadata) (.metadata (->Content metadata))

      true             (.build))))

(s/fdef ->EventData
  :args (s/cat :event ::evst/event-data)
  :ret ::ext/event-data)

(defn EventData-> [o]
  (let [data (Content-> (.data o))
        metadata (Content-> (.metadata o))]
    (merge {::event/type (.eventType o)
            ::event/id (.eventId o)}
           (if-not (nil? (::e.data/value data)) {::event/data data})
           (if-not (nil? (::e.data/value metadata)) {::event/metadata metadata}))))

(s/fdef EventData->
  :args (s/cat :event-data ::ext/event-data)
  :ret ::evst/event-data)



;; Event number

(create-ns 'evst.event.number)
(alias 'number 'evst.event.number)

(s/def ::number/exact
  (s/or :first #{::number/first}
        :num nat-int?))

(s/def ::number/last #{::number/last})

(s/def ::event/number
  (s/or :exact ::number/exact
        :last ::number/last))

(s/def ::ext/event-number
  (s/with-gen #(instance? eventstore.EventNumber %)
    (fn [] (gen/fmap #(eventstore.EventNumber/apply %) (s/gen int?)))))

(s/def ::ext/event-number-exact
  (s/with-gen #(instance? eventstore.EventNumber$Exact %)
    (fn [] (gen/fmap #(eventstore.EventNumber/apply %)
                    (s/gen nat-int?)))))

(defn ->EventNumber [o]
  (match/match
   o   ::number/first (eventstore.EventNumber/First)
   ,   (:or ::number/current ::number/last) (eventstore.EventNumber/Current)
   ,   :else (eventstore.EventNumber/apply o)))

(s/fdef ->EventNumber
  :args (s/cat :event-number ::event/number)
  :ret ::ext/event-number)



(defmulti EventNumber-> class)

(defmethod EventNumber-> eventstore.EventNumber$Exact [o]
  (.value o))

(defmethod EventNumber-> eventstore.EventNumber$Last$ [o]
  ::number/last)

(defmethod EventNumber-> java.lang.Number [o] o)

(s/fdef EventNumber->
  :args (s/cat :number ::ext/event-number)
  :ret (s/or :number ::event/number
             :range ::number/range))



;; Event number ranges

(s/def ::number/start ::number/exact)

(s/def ::number/end ::number/exact)

(defn event-number-compare [x y]
  (match/match x
               ::number/first (if (= y ::number/first) 0 -1)
               ::number/last (if (= y ::number/last) 0 1)
               :else (compare x y)))

(s/def ::number/range
  (s/and (s/keys :req [::number/start ::number/end])
         (fn [num] (<= (event-number-compare (::number/start num) (::number/end num)) 0))))

(s/def ::ext/event-number-range
  (s/with-gen #(instance? eventstore.EventNumber$Range %)
    (fn [] (gen/fmap (fn [[s e]](.apply (eventstore.EventNumber$Range$.) s e))
                    (gen/such-that (fn [[s e]] (>= e s))
                                   (s/gen (s/tuple nat-int? nat-int?)))))))

(defn exact->int [n]
  (match/match n
               ::number/first 0
               :else n))

(defn ->EventNumberRange [{::number/keys [start end]}]
  (.apply (eventstore.EventNumber$Range$.) (exact->int start) (exact->int end)))

(s/fdef ->EventNumberRange
  :args (s/cat :range ::number/range)
  :ret ::ext/event-number-range)

(defn EventNumberRange-> [o]
  {::number/start (EventNumber-> (.start o))
   ::number/end (EventNumber-> (.end o))})

(s/fdef EventNumberRange->
  :args (s/cat :range ::ext/event-number-range)
  :ret ::number/range)



;; Event

(s/def ::ext/date-time
  (s/with-gen #(instance? org.joda.time.DateTime %)
    (fn [] (gen/fmap #(c/from-long %) (s/gen int?)))))

(s/def ::ext/event-record
  (s/with-gen #(instance? eventstore.EventRecord %)
    (fn [] (gen/fmap
           (fn [[stream number data created]]
             (new eventstore.EventRecord stream number data created))
           (s/gen (s/tuple ::ext/stream-id
                           ::ext/event-number-exact
                           ::ext/event-data
                           (u/optional ::ext/date-time)))))))

(s/def ::ext/event-indexed
  (s/with-gen #(instance? eventstore.IndexedEvent %)
    (fn [] (gen/fmap (fn [[event pos]] (eventstore.IndexedEvent. event pos))
                    (s/gen (s/tuple ::ext/event ::ext/position-exact))))))

(s/def ::ext/event
  (s/or :record ::ext/event-record))

(s/def ::evst/date-time
  (s/with-gen #(instance? org.joda.time.DateTime %)
    (fn [] (gen/fmap (fn [l] (c/from-long l)) (s/gen int?)))))

(s/def ::evst/created (s/nilable ::evst/date-time))

(s/def ::evst/event
  (s/keys :req [::evst/stream ::event/number ::evst/event-data ::evst/created]
          :opt [::event/record]))

(s/def ::event/indexed
  (s/keys :req [::evst/event ::evst/position]))

(defmulti Event-> class)

(defmethod Event-> eventstore.EventRecord [o]
  {::evst/stream (EventStream-> (.streamId o)) ;; TODO change EventStream to EventStream.Id
   ::event/number (EventNumber-> (.number o))
   ::evst/event-data (EventData-> (.data o))
   ::evst/created (u/get-or-else (.created o) nil)})

(defmethod Event-> eventstore.ResolvedEvent [o]
  {::evst/stream (EventStream-> (.streamId o))
   ::event/number (EventNumber-> (.number o))
   ::evst/event-data (EventData-> (.data o))
   ::event/record (Event-> (.record o))
   ::evst/created (u/get-or-else (.created o) nil)})

(s/fdef Event->
  :args (s/cat :event ::ext/event)
  :ret ::evst/event)



(defn IndexedEvent-> [o]
  {::evst/event (Event-> (.event o))
   ::evst/position (Position-> (.position o))})



;; Version

(create-ns 'evst.version)
(alias 'version 'evst.version)

(s/def ::version/no-stream #{::version/no-stream -1})

(s/def ::version/any #{::version/any -2})

(s/def ::version/exact nat-int?)

(s/def ::version/existing
  (s/or :any ::version/any
        :exact ::version/exact))

(s/def ::evst/version
  (s/or :no-stream ::version/no-stream
        :existing ::version/existing))

(s/def ::ext/version
  (s/with-gen #(instance? eventstore.ExpectedVersion %)
    (fn [] (gen/fmap #(eventstore.ExpectedVersion/apply %)
                    (s/gen (s/and int? #(>= % -2)))))))

(s/def ::ext/version-exact
  (s/with-gen #(instance? eventstore.ExpectedVersion %)
    (fn [] (gen/fmap #(eventstore.ExpectedVersion/apply %)
                    (s/gen nat-int?)))))

(defn ->ExpectedVersion [o]
  (match/match [o]
               [::version/no-stream] (eventstore.ExpectedVersion/apply -1)
               [::version/any] (eventstore.ExpectedVersion/apply -2)
               :else (eventstore.ExpectedVersion/apply o)))

(s/fdef ->ExpectedVersion
  :args (s/cat :version ::evst/version)
  :ret ::ext/version
  :fn (fn [x] (let [input (-> x :args :version)
                   return (-> x :ret)]
               (match/match input
                            [:existing [:exact n]] (= return (eventstore.ExpectedVersion/apply n))
                            [:existing [:any _]]   (= return (eventstore.ExpectedVersion/apply -2))
                            [:no-stream _]         (= return (eventstore.ExpectedVersion/apply -1))
                            :else false))))



(defmulti ExpectedVersion-> class)

(defmethod ExpectedVersion->
  eventstore.ExpectedVersion$NoStream$ [o] ::version/no-stream)

(defmethod ExpectedVersion->
  eventstore.ExpectedVersion$Any$ [o] ::version/any)

(defmethod ExpectedVersion->
  eventstore.ExpectedVersion$Exact [o] (.value o))

(s/fdef ExpectedVersion->
  :args (s/cat :version ::ext/version)
  :ret ::evst/version)



;; Results

(create-ns 'evst.result)
(alias 'result 'evst.result)

(s/def ::result/next-version ::evst/version)

(s/def ::result/write (s/keys :req [::result/next-version ::evst/position]))

(s/def ::ext/write-result
  (s/with-gen #(instance? eventstore.WriteResult %)
    (fn [] (gen/fmap (fn [[version position]] (eventstore.WriteResult. version position))
                    (s/gen (s/tuple ::ext/version-exact ::ext/position))))))

(defn WriteResult-> [o]
  {::result/next-version (ExpectedVersion-> (.nextExpectedVersion o))
   ::evst/position (Position-> (.logPosition o))})

(s/fdef WriteResult->
  :args (s/cat :result ::ext/write-result)
  :req ::result/write)



(s/def ::result/delete (s/keys :req [::evst/position]))

(s/def ::ext/delete-result
  (s/with-gen #(instance? eventstore.DeleteResult %)
    (fn [] (gen/fmap #(eventstore.DeleteResult. %) (s/gen ::ext/position)))))

(defn DeleteResult-> [o]
  {::evst/position (Position-> (.logPosition o))})

(s/fdef DeleteResult->
  :args (s/cat :delete ::ext/delete-result)
  :ret ::result/delete)



;; Direction

(create-ns 'evst.direction)
(alias 'direction 'evst.direction)

(s/def ::evst/direction
  #{::direction/forward ::direction/backward})

(s/def ::ext/read-direction
  (s/with-gen #(instance? eventstore.ReadDirection %)
    (fn [] (gen/fmap #(if %
                       (eventstore.j.ReadDirection/forward)
                       (eventstore.j.ReadDirection/backward))
                    (s/gen boolean?)))))

(defn ->ReadDirection [o]
  (match/match
   o   ::direction/forward (eventstore.j.ReadDirection/forward)
   ,   ::direction/backward (eventstore.j.ReadDirection/backward)))

(s/fdef ->ReadDirection
  :args (s/cat :direction ::evst/direction)
  :ret ::ext/read-direction
  :fn (fn [x] (let [input (-> x :args :direction)
                   return (-> x :ret)]
               (match/match [input]
                            [::direction/forward]
                            ,   (eventstore.j.ReadDirection/forward)
                            [::direction/backward]
                            ,   (eventstore.j.ReadDirection/forward)))))



(defn ReadDirection-> [o]
  (let [Forward eventstore.ReadDirection$Forward$
        Backward eventstore.ReadDirection$Backward$]
    (match/match (class o)
                 Forward  ::direction/forward
                 Backward ::direction/backward)))

(s/fdef ReadDirection->
  :args (s/cat :direction ::ext/read-direction)
  :ret ::evst/direction)



(s/def ::evst/client-version (s/and nat-int? #(<= % 1000)))

(s/def ::evst/connection-name string?)


(s/def ::evst/identify-client
  (s/keys :req [::evst/client-version ::evst/connection-name]))

(defn ->IdentifyClient
  [{::evst/keys [client-version connection-name]}]
  (eventstore.IdentifyClient. client-version (Option/apply connection-name)))

(s/fdef ->IdentifyClient
  :args (s/cat :client ::evst/identify-client)
  :ret #(instance? eventstore.IdentifyClient %)
  :fn #(s/and (= (.version (:ret %))
                 (-> % :args :client ::evst/client-version))
              (= (.connectionName (:ret %))
                 (-> % :args :client ::evst/connection-name))))



(s/def ::evst/master? boolean?)
(s/def ::evst/hard-delete? boolean?)
(s/def ::evst/resolve-links? boolean?)

(s/def ::evst/events (s/coll-of ::evst/event :gen-max 5))

(s/def ::evst/event-datas (s/coll-of ::evst/event-data :gen-max 5))

(s/def ::evst/write-events
  (s/keys :req [::stream/has-id ::evst/event-datas] :opt [::evst/version ::evst/master?]))

(defn ->WriteEvents
  [{::evst/keys [event-datas version master?]
    ::stream/keys [has-id]
    :or {version ::version/any
         master? false}}]
  (eventstore.WriteEvents. (->EventStream has-id)
                           (u/->scala-List (map ->EventData event-datas))
                           (->ExpectedVersion version)
                           master?))

(s/fdef ->WriteEvents
  :args (s/cat :write-events ::evst/write-events)
  :ret #(instance? eventstore.WriteEvents %))

;; (defn ReadEventCompleted-> [o]
;;   {::evst/event (Event-> (.event o))})

;; (s/fdef ReadEventCompleted->
;;   :args (s/cat :read-event-completed ::ext/read-event-completed)
;;   :ret ::evst/read-event-completed)

(s/def ::ext/write-events-completed
  (s/with-gen #(instance? eventstore.WriteEventsCompleted %)
    (fn [] (gen/fmap #(eventstore.WriteEventsCompleted. % %2)
                    (s/gen (s/tuple (u/optional ::number/range)
                                    (u/optional ::evst/position)))))))

(s/def ::evst/write-events-completed
  (s/keys :req [::number/range ::evst/position]))

(defn WriteEventsCompleted-> [o]
  (let [range (some-> o (.numberRange) u/get-or-else EventNumberRange->)
        position (some-> o (.position) u/get-or-else Position->)]
    {::number/range range
     ::evst/position position}))

(s/fdef WriteEventsCompleted->
  :args (s/cat :write-events-completed ::ext/write-events-completed)
  :ret ::evst/write-events-completed)



(s/def ::evst/delete-stream
  (s/keys :req [::stream/has-id] :opt [::evst/version ::evst/hard-delete? ::evst/master?]))

(defn ->DeleteStream
  [{::evst/keys [hard-delete? master?]
    ::version/keys [existing]
    ::stream/keys [has-id]
    :or {existing ::version/any
         hard-delete? false master? false}}]
  (eventstore.DeleteStream. (->EventStream has-id)
                            (->ExpectedVersion existing)
                            hard-delete? master?))

(s/fdef ->DeleteStream
  :args (s/cat :delete-stream ::evst/delete-stream)
  :ret #(instance? eventstore.DeleteStream %))



;; Transactions

(create-ns 'evst.transaction)
(alias 'transaction 'evst.transaction)

(s/def ::evst/transaction-start
  (s/keys :req [::stream/has-id] :opt [::evst/version ::evst/master?]))

(defn ->TransactionStart
  [{::evst/keys [version master?]
    ::stream/keys [has-id]
    :or {version ::version/any
         master? false}}]
  (eventstore.TransactionStart.
   (->EventStream has-id)
   (->ExpectedVersion version)
   master?))

(s/fdef ->TransactionStart
  :args (s/cat :transaction-start ::evst/transaction-start)
  :ret #(instance? eventstore.TransactionStart %))



(s/def ::transaction/id (s/and int? #(>= % 0)))
(s/def ::evst/transaction-write
  (s/keys :req [::transaction/id ::evst/event-datas] :opt [::evst/master?]))

(defn ->TransactionWrite
  [{::evst/keys [event-datas master?]
    ::transaction/keys [id]
    :or {master? false}}]
  (eventstore.TransactionWrite. id (u/->scala-List (map ->EventData event-datas)) master?))

(s/fdef ->TransactionWrite
  :args (s/cat :transaction-write ::evst/transaction-write)
  :ret #(instance? eventstore.TransactionWrite %)
  )



(s/def ::evst/transaction-commit
  (s/keys :req [::transaction/id] :opt [::evst/master?]))

(defn ->TransactionCommit
  [{::evst/keys [master?] ::transaction/keys [id]
    :or {master? false}}]
  (eventstore.TransactionCommit. id master?))

(s/fdef ->TransactionCommit
  :args (s/cat :commit ::evst/transaction-commit)
  :ret #(instance? eventstore.TransactionCommit %))



(s/def ::evst/read-event
  (s/keys :req [::stream/has-id ::event/number] :opt [::evst/resolve-links? ::evst/master?]))

(defn ->ReadEvent
  [{::event/keys [number]
    ::stream/keys [has-id]
    ::evst/keys [resolve-links? master?]
    :or {resolve-links? true master? false}}]
  (eventstore.ReadEvent. (->EventStream has-id)
                         (->EventNumber number)
                         resolve-links? master?))

(s/fdef ->ReadEvent
  :args (s/cat :read-event ::evst/read-event)
  :ret #(instance? eventstore.ReadEvent %))



(s/def ::ext/read-event-completed
  (s/with-gen #(instance? eventstore.ReadEventCompleted %)
    (fn [] (gen/fmap #(eventstore.ReadEventCompleted. %)
                    (s/gen ::ext/event)))))

(s/def ::evst/read-event-completed
  (s/keys :req [::evst/event]))

(defn ReadEventCompleted-> [o]
  {::evst/event (Event-> (.event o))})

(s/fdef ReadEventCompleted->
  :args (s/cat :read-event-completed ::ext/read-event-completed)
  :ret ::evst/read-event-completed)



(s/def ::event/max-count (s/and pos-int? #(<= % 10000)))
(s/def ::evst/read-stream-events
  (s/keys :req [::stream/has-id]
          :opt [::event/number ::event/max-count ::evst/direction ::evst/resolve-links? ::evst/master?]))

(defn ->ReadStreamEvents
  [{::evst/keys [direction resolve-links? master?]
    ::stream/keys [has-id]
    ::number/keys [from] ::event/keys [max-count]
    :or {from ::number/first
         max-count (.readBatchSize (eventstore.Settings/Default))
         direction ::direction/forward
         resolve-links? true
         master? false}}]
  (eventstore.ReadStreamEvents.
   (->EventStream has-id)
   (->EventNumber from)
   max-count
   (->ReadDirection direction)
   resolve-links?
   master?))

(s/fdef ->ReadStreamEvents
  :args (s/cat :read-stream-events ::evst/read-stream-events)
  :ret #(instance? eventstore.ReadStreamEvents %))



(s/def ::number/next ::event/number)
(s/def ::number/previous ::number/exact)
(s/def ::evst/end-of-stream? boolean?)
(s/def ::position/last-commit int?)

(s/def ::result/read-stream-events
  (s/keys :req [::evst/events ::number/next ::number/previous
                ::evst/end-of-stream? ::position/last-commit ::evst/direction]))

(s/def ::ext/events
  (s/with-gen (fn [events] (every? #(s/valid? ::ext/event %) (u/scala-List-> events)))
    (fn [] (gen/fmap (fn [events] (u/->scala-List events))
                    (s/gen (s/coll-of ::ext/event :kind vector? :gen-max 5))))))

(s/def ::ext/read-stream-events-completed
  (s/with-gen #(instance? eventstore.ReadStreamEventsCompleted %)
    (fn [] (gen/fmap (fn [[events next-num last-num end? last-commit direction]]
                      (eventstore.ReadStreamEventsCompleted.
                       events next-num last-num end? last-commit direction))
                    (s/gen
                     (s/and
                      (s/tuple ::ext/events
                               ::ext/event-number
                               ::ext/event-number-exact
                               boolean?
                               int?
                               ::ext/read-direction)
                      (fn [[_ n _ _ _ d]]
                        (or (not= d (eventstore.j.ReadDirection/forward))
                            (not= n (eventstore.EventNumber/Current))))))))))

(defn ReadStreamEventsCompleted-> [o]
  {::evst/events (map Event-> (u/scala-List-> (.events o)))
   ::number/next (EventNumber-> (.nextEventNumber o))
   ::number/previous (EventNumber-> (.lastEventNumber o))
   ::evst/end-of-stream? (.endOfStream o)
   ::position/last-commit (.lastCommitPosition o)
   ::evst/direction (ReadDirection-> (.direction o))})

(s/fdef ReadStreamEventsCompleted->
  :args (s/cat :read ::ext/read-stream-events-completed)
  :ret ::result/read-stream-events)



(s/def ::evst/read-all-events
  (s/keys :opt [::evst/position ::event/max-count ::evst/direction ::evst/resolve-links? ::evst/master?]))

(defn ->ReadAllEvents
  [{::evst/keys [position direction resolve-links? master?]
    ::event/keys [max-count]
    :or {position ::position/first
         max-count (.readBatchSize (eventstore.Settings/Default))
         direction ::direction/forward
         resolve-links? true
         master? true}}]
  (eventstore.ReadAllEvents.
   (->Position position)
   max-count
   (->ReadDirection direction)
   resolve-links?
   master?))

(s/fdef ->ReadAllEvents
  :args (s/cat :read-all-events ::evst/read-all-events)
  :ret #(instance? eventstore.ReadAllEvents %))



(s/def ::evst/events-indexed (s/coll-of ::event/indexed :gen-max 5))

(s/def ::position/previous ::position/exact)
(s/def ::position/next ::position/exact)

(s/def ::result/read-all-events
  (s/keys :req [::evst/events-indexed ::position/previous ::position/next ::evst/direction]))

(s/def ::ext/events-indexed
  (s/with-gen (fn [events] (every? #(s/valid? ::ext/event-indexed %) (u/scala-List-> events)))
    (fn [] (gen/fmap (fn [events] (u/->scala-List events))
                    (s/gen (s/coll-of ::ext/event-indexed :kind vector? :gen-max 5))))))

(s/def ::ext/read-all-events-completed
  (s/with-gen #(instance? eventstore.ReadAllEventsCompleted %)
    (fn [] (gen/fmap (fn [[events pos next dir]]
                      (eventstore.ReadAllEventsCompleted. events pos next dir))
                    (s/gen (s/tuple ::ext/events-indexed
                                    ::ext/position-exact
                                    ::ext/position-exact
                                    ::ext/read-direction))))))

(defn ReadAllEventsCompleted-> [o]
  (let [events (map IndexedEvent-> (u/scala-List-> (.events o)))
        position (Position-> (.position o))
        next-position (Position-> (.nextPosition o))
        direction (ReadDirection-> (.direction o))]
    {::evst/events-indexed events
     ::position/previous position
     ::position/next next-position
     ::evst/direction direction}))

(s/fdef ReadAllEventsCompleted->
  :args (s/cat :read ::ext/read-all-events-completed)
  :ret ::result/read-all-events)


;; Persistent subscriptions

(create-ns 'evst.persistent)
(alias 'pers 'evst.persistent)


;; Settings

(s/def ::pers/buffer-size int?)

(s/def ::pers/start-from ::event/number)
(s/def ::pers/extra-statistics boolean?)
(s/def ::pers/message-timeout-millis (s/and nat-int? #(< % 31557600))) ;; duration?
(s/def ::pers/max-retry-count int?)
(s/def ::pers/live-buffer-size int?)
(s/def ::pers/read-batch-size int?)
(s/def ::pers/history-buffer-size int?)
(s/def ::pers/check-point-after-secs (s/and nat-int? #(< % 31557600)))
(s/def ::pers/min-check-point-count int?)
(s/def ::pers/max-check-point-count int?)
(s/def ::pers/max-subscriber-count int?)
(s/def ::pers/consumer-strategy
  (s/or :stock #{::pers/dispatch-to-single
                 ::pers/round-robin}
        :custom (s/and string? seq)))

(s/def ::pers/settings
  (s/keys :opt [
                ::evst/resolve-links?
                ::pers/start-from
                ::pers/extra-statistics
                ::pers/message-timeout-millis
                ::pers/max-retry-count
                ::pers/live-buffer-size
                ::pers/read-batch-size
                ::pers/history-buffer-size
                ::pers/check-point-after-secs
                ::pers/min-check-point-count
                ::pers/max-check-point-count
                ::pers/max-subscriber-count
                ::pers/consumer-strategy
                ]))

(defn ->ConsumerStrategy [o]
  (match/match
   o   ::pers/dispatch-to-single (eventstore.ConsumerStrategy$DispatchToSingle$.)
   ,   ::pers/round-robin (eventstore.ConsumerStrategy$RoundRobin$.)
   ,   :else (eventstore.ConsumerStrategy/apply o)
   ))

(s/fdef ->ConsumerStrategy
  :args (s/cat :strategy ::pers/consumer-strategy)
  :ret #(instance? eventstore.ConsumerStrategy %))



(defn ->PersistentSubscriptionSettings
  [{::evst/keys [resolve-links?]
    ::pers/keys [start-from extra-statistics message-timeout-millis
                      max-retry-count live-buffer-size read-batch-size
                      history-buffer-size check-point-after-secs
                      min-check-point-count max-check-point-count
                      max-subscriber-count consumer-strategy]}]
  (let [b (eventstore.j.PersistentSubscriptionSettingsBuilder.)]
    (cond-> b
      (some? resolve-links?)            (.resolveLinkTos resolve-links?)
      (some? start-from)                (.startFrom (->EventNumber start-from))
      extra-statistics                  (.withExtraStatistic)
      (some? message-timeout-millis)    (.messageTimeout message-timeout-millis)
      (some? max-retry-count)           (.maxRetryCount max-retry-count)
      (some? live-buffer-size)          (.liveBufferSize live-buffer-size)
      (some? read-batch-size)           (.readBatchSize read-batch-size)
      (some? history-buffer-size)       (.historyBufferSize history-buffer-size)
      (some? check-point-after-secs)    (.checkPointAfter check-point-after-secs)
      (some? min-check-point-count)     (.minCheckPointCount min-check-point-count)
      (some? max-check-point-count)     (.maxCheckPointCount max-check-point-count)
      (some? max-subscriber-count)      (.maxSubscriberCount max-subscriber-count)
      (some? consumer-strategy)         (.consumerStrategy (->ConsumerStrategy consumer-strategy))
      true                              (.build))))

(s/fdef ->PersistentSubscriptionSettings
  :args (s/cat :settings ::pers/settings)
  :ret #(instance? eventstore.PersistentSubscriptionSettings %))



(s/def ::pers/group (s/and string? seq))

(s/def ::pers/create
  (s/keys :req [::stream/has-id ::pers/group]
          :opt [::pers/settings]))

(defn ->CreatePersistentSubscription
  [{::stream/keys [has-id] ::pers/keys [group settings] :or {settings {}}}]
  (eventstore.PersistentSubscription$Create.
   (->EventStream has-id) group (->PersistentSubscriptionSettings settings)))

(s/fdef ->CreatePersistentSubscription
  :args (s/cat :create ::pers/create)
  :ret #(instance? eventstore.PersistentSubscription$Create %))



(s/def ::pers/update
  (s/keys :req [::stream/has-id ::pers/group]
          :opt [::pers/settings]))

(defn ->UpdatePersistentSubscription
  [{::stream/keys [has-id] ::pers/keys [group settings] :or {settings {}}}]
  (eventstore.PersistentSubscription$Update.
   (->EventStream has-id) group (->PersistentSubscriptionSettings settings)))

(s/fdef ->UpdatePersistentSubscription
  :args (s/cat :update ::pers/update)
  :ret #(instance? eventstore.PersistentSubscription$Update %))



(s/def ::pers/delete
  (s/keys :req [::stream/has-id ::pers/group]))

(defn ->DeletePersistentSubscription
  [{::stream/keys [has-id] ::pers/keys [group]}]
  (eventstore.PersistentSubscription$Delete.
   (->EventStream has-id) group))

(s/fdef ->DeletePersistentSubscription
  :args (s/cat :delete ::pers/delete)
  :ret #(instance? eventstore.PersistentSubscription$Delete %))



(s/def ::pers/id (s/and string? seq))
(s/def ::evst/event-ids (s/coll-of ::event/id :min-count 1 :gen-max 5))
(s/def ::pers/ack (s/keys :req [::pers/id ::evst/event-ids]))

(defn ->Ack
  [{::evst/keys [event-ids] ::pers/keys [id]}]
  (eventstore.PersistentSubscription$Ack. id (u/->scala-List event-ids)))

(s/fdef ->Ack
  :args (s/cat :ack ::pers/ack)
  :ret #(instance? eventstore.PersistentSubscription$Ack %))



(create-ns 'evst.persistent.nak)
(alias 'nak 'evst.persistent.nak)

(s/def ::nak/action #{::nak/unknown ::nak/park ::nak/retry ::nak/skip ::nak/stop})

(defn ->NakAction [o]
  (match/match
   o   ::nak/unknown (eventstore.PersistentSubscription$Nak$Action$Unknown$.)
   ,   ::nak/park    (eventstore.PersistentSubscription$Nak$Action$Park$.)
   ,   ::nak/retry   (eventstore.PersistentSubscription$Nak$Action$Retry$.)
   ,   ::nak/skip    (eventstore.PersistentSubscription$Nak$Action$Skip$.)
   ,   ::nak/stop    (eventstore.PersistentSubscription$Nak$Action$Stop$.)
   ))

(s/def ::nak/message string?)

(s/def ::pers/nak (s/keys :req [::pers/id ::evst/event-ids ::nak/action] :opt [::nak/message]))

(defn ->Nak
  [{::evst/keys [event-ids] ::pers/keys [id] ::nak/keys [action message]}]
  (eventstore.PersistentSubscription$Nak.
   id (u/->scala-List event-ids) (->NakAction action) (Option/apply message)))

(s/fdef ->Nak
  :args (s/cat :nak ::pers/nak)
  :ret #(instance? eventstore.PersistentSubscription$Nak %))



(s/def ::pers/connect
  (s/keys :req [::stream/has-id ::pers/group] :opt [::pers/buffer-size]))

(defn ->Connect
  [{::stream/keys [has-id]
    ::evst/keys [buffer-size]
    ::pers/keys [group]
    :or {buffer-size 10}}]
  (eventstore.PersistentSubscription$Connect.
   (->EventStream has-id) group buffer-size))

(s/fdef ->Connect
  :args (s/cat :connect ::pers/connect)
  :ret #(instance? eventstore.PersistentSubscription$Connect %))



(s/def ::evst/subscribe
  (s/keys :req [::stream/has-id] :opt [::evst/resolve-links?]))

(gen/sample (s/gen ::evst/stream))

(defn ->SubscribeTo
  [{::evst/keys [resolve-links?] ::stream/keys [has-id] :or {resolve-links? true}}]
  (eventstore.SubscribeTo. (->EventStream has-id) resolve-links?))

(s/fdef ->SubscribeTo
  :args (s/cat :subscribe ::evst/subscribe)
  :ret #(instance? eventstore.SubscribeTo %))



(s/def ::evst/unsubscribe
  (s/keys :req []))

(defn ->Unsubscribe
  ([] (eventstore.Unsubscribe/out))
  ([x] (->Unsubscribe)))

(s/fdef ->Unsubscribe
  :args (s/cat)
  :ret #(instance? eventstore.Unsubscribe$ %))



(s/def ::evst/scavenge
  (s/keys :req []))

(defn ->ScavengeDatabase
  ([] (eventstore.ScavengeDatabase/out))
  ([x] (->ScavengeDatabase)))

(s/fdef ->ScavengeDatabase
  :args (s/cat)
  :ret #(instance? eventstore.ScavengeDatabase$ %))



(s/def ::evst/authenticate
  (s/keys :req []))

(defn ->Authenticate
  ([] (eventstore.Authenticate/out))
  ([x] (->Authenticate)))

(s/fdef ->Authenticate
  :args (s/cat)
  :ret #(instance? eventstore.Authenticate$ %))



(defmulti In-> class)

(defmethod In-> eventstore.UserCredentials [x] (UserCredentials-> x))
(defmethod In-> eventstore.Position [x] (Position-> x))
(defmethod In-> eventstore.EventStream [x] (EventStream-> x))
(defmethod In-> eventstore.EventData [x] (EventData-> x))
(defmethod In-> eventstore.EventNumber [x] (EventNumber-> x))
(defmethod In-> eventstore.Event [x] (Event-> x))
(defmethod In-> eventstore.IndexedEvent [x] (IndexedEvent-> x))
(defmethod In-> eventstore.ExpectedVersion [x] (ExpectedVersion-> x))
(defmethod In-> eventstore.WriteResult [x] (WriteResult-> x))
(defmethod In-> eventstore.DeleteResult [x] (DeleteResult-> x))
(defmethod In-> eventstore.ReadDirection [x] (ReadDirection-> x))
(defmethod In-> eventstore.ReadEventCompleted [x] (ReadEventCompleted-> x))
(defmethod In-> eventstore.ReadStreamEventsCompleted [x] (ReadStreamEventsCompleted-> x))
(defmethod In-> eventstore.ReadAllEventsCompleted [x] (ReadAllEventsCompleted-> x))

;; Scala durations must be smaller than 2^63 - 1 ns = 9223372036 s
(s/def ::evst/duration (s/and nat-int? #(< % 9223372036)))
(s/def ::evst/duration-!0 (s/and pos-int? #(< % 9223372036)))

(s/def ::sett/address string?)
(s/def ::sett/connection-name string?)
(s/def ::sett/connection-timeout ::evst/duration)
(s/def ::sett/max-reconnections nat-int?)
(s/def ::sett/reconnection-delay-min ::evst/duration-!0)
(s/def ::sett/reconnection-delay-max ::evst/duration-!0)
(s/def ::sett/credentials ::evst/credentials)
(s/def ::sett/hb-interval ::evst/duration)
(s/def ::sett/hb-timeout ::evst/duration)
(s/def ::sett/op-timeout ::evst/duration-!0)
(s/def ::sett/keep-trying? boolean?)
(s/def ::sett/op-max-retries nat-int?)
(s/def ::sett/perform-on-any-node boolean?)
(s/def ::sett/perform-on-master-only? boolean?)
(s/def ::sett/read-batch-size nat-int?)
(s/def ::sett/buffer-size nat-int?)

(create-ns 'evst.settings.overflow)
(alias 'of 'evst.settings.overflow)

(s/def ::sett/on-overflow
  #{::of/drop-head ::of/drop-tail ::of/drop-buffer ::of/drop-new ::of/fail})

(create-ns 'evst.settings.cluster)
(alias 'cl 'evst.settings.cluster)

(s/def ::sett/cluster
  (s/keys :opt [::cl/gossip-or-dns ::cl/dns-timeout ::cl/max-discover-attempts
                ::cl/attempt-interval ::cl/discovery-interval ::cl/gossip-timeout]))

;; TODO redo ::cl/gossip-or-dns
(s/def ::cl/gossip-or-dns
  (s/with-gen #(instance? eventstore.cluster.GossipSeedsOrDns %)
    (fn [] (gen/fmap #(eventstore.cluster.GossipSeedsOrDns/apply %)
                    (s/gen (s/and string? seq))))))

(s/def ::cl/dns-timeout ::evst/duration)
(s/def ::cl/max-discover-attempts (s/and pos-int? #(< % 1000)))
(s/def ::cl/attempt-interval ::evst/duration)
(s/def ::cl/discovery-interval ::evst/duration)
(s/def ::cl/gossip-timeout ::evst/duration)

(defn ->Cluster
  [{::cl/keys [dns-timeout
              max-discover-attempts
              attempt-interval
              discovery-interval
              gossip-timeout]}]
  (let [b (eventstore.j.ClusterSettingsBuilder.)]
    (cond-> b
      (some? dns-timeout)              (.dnsLookupTimeout dns-timeout)
      (some? max-discover-attempts)    (.maxDiscoverAttempts max-discover-attempts)
      (some? attempt-interval)         (.discoverAttemptInterval attempt-interval)
      (some? discovery-interval)       (.discoveryInterval discovery-interval)
      (some? gossip-timeout)           (.gossipTimeout gossip-timeout)

      true                             (.build))))

(s/fdef ->Cluster
  :args (s/cat :cluster ::sett/cluster)
  :ret #(instance? eventstore.cluster.ClusterSettings %))

(defn Cluster-> [^eventstore.cluster.ClusterSettings s]
  {::cl/dns-timeout            (-> s (.dnsLookupTimeout) (.toMillis))
   ::cl/max-discover-attempts  (.maxDiscoverAttempts s)
   ::cl/attempt-interval       (-> s (.discoverAttemptInterval) (.toMillis))
   ::cl/discovery-interval     (-> s (.discoveryInterval) (.toMillis))
   ::cl/gossip-timeout         (-> s (.gossipTimeout) (.toMillis))})



(s/def ::evst/uri-string
  (s/with-gen string? (fn [] (gen/fmap #(.toString %) (s/gen uri?)))))

(s/def ::sett/http ::evst/uri-string)

(defn ->HttpSettings [uri]
  (eventstore.HttpSettings/apply (akka.http.scaladsl.model.Uri/apply uri)))

(s/fdef ->HttpSettings
  :args (s/cat :uri ::sett/http)
  :ret #(instance? eventstore.HttpSettings %))

(defn HttpSettings-> [^eventstore.HttpSettings http]
  (some-> http (.uri) (.toString)))


(s/def ::sett/serialisation-parallelism (s/and pos-int? #(< % 1000)))
(s/def ::sett/serialisation-ordered? boolean?)

(s/def ::evst/connection any?)

(s/def ::ext/credentials
  (s/with-gen #(instance? eventstore.UserCredentials %)
    (fn [] (gen/fmap (fn [[x y]] (eventstore.UserCredentials. x y))
                    (s/gen (s/cat :login ::creds/login :pass ::creds/password))))))

(s/def ::ext/settings
  (s/with-gen #(instance? eventstore.Settings %)
    (fn [] (gen/fmap (fn [_] (-> (eventstore.j.SettingsBuilder.) (.build)))
                    (s/gen int?)))))

(s/def ::evst/settings
  (s/keys :opt
          [::sett/address
           ::sett/buffer-size
           ::sett/cluster
           ::sett/connection-name
           ::sett/connection-timeout
           ::sett/hb-interval
           ::sett/hb-timeout
           ::sett/http
           ::sett/keep-trying?
           ::sett/max-reconnections
           ::sett/on-overflow
           ::sett/op-max-retries
           ::sett/op-timeout
           ::sett/read-batch-size
           ::sett/reconnection-delay-max
           ::sett/reconnection-delay-min
           ::sett/serialisation-ordered?
           ::sett/serialisation-parallelism

           ::evst/connection
           ::evst/credentials
           ::evst/hard-delete?
           ::evst/master?
           ::evst/resolve-links?
           ::evst/version]))

(defn ->of [^eventstore.j.SettingsBuilder b of]
  (match/match [of]
               [::of/drop-head]   (.dropHeadWhenOverflow b)
               [::of/drop-tail]   (.dropTailWhenOverflow b)
               [::of/drop-buffer] (.dropBufferWhenOverflow b)
               [::of/drop-new]    (.dropNewWhenOverflow b)
               [::of/fail]        (.failWhenOverflow b)))

(defn of-> [^eventstore.OverflowStrategy of]
  (let [fail eventstore.OverflowStrategy$Fail$
        head eventstore.OverflowStrategy$DropHead$
        tail eventstore.OverflowStrategy$DropTail$
        buffer eventstore.OverflowStrategy$DropBuffer$
        new eventstore.OverflowStrategy$DropNew$]
    (match/match [(class of)]
                 [fail] ::of/fail
                 [head] ::of/drop-head
                 [tail] ::of/drop-tail
                 [buffer] ::of/drop-buffer
                 [new] ::of/drop-new)))

(def millis java.util.concurrent.TimeUnit/MILLISECONDS)

(defn ->Settings
  [{::sett/keys [address
                connection-name
                connection-timeout
                max-reconnections
                reconnection-delay-min
                reconnection-delay-max
                hb-interval
                hb-timeout
                op-timeout
                limit-op-retries
                keep-trying?
                op-max-retries
                master?
                read-batch-size
                buffer-size
                on-overflow
                cluster
                http
                serialisation-parallelism
                serialisation-ordered?]
    ::evst/keys [resolve-links?
                credentials
                master?]}]

  (let [b (eventstore.j.SettingsBuilder.)]
    (cond-> b
      (some? address)                   (.address address)
      (some? connection-name)           (.connectionName connection-name)
      (some? connection-timeout)        (.connectionTimeout connection-timeout millis)
      (some? max-reconnections)         (.maxReconnections max-reconnections)
      (some? reconnection-delay-min)    (.reconnectionDelayMin reconnection-delay-min millis)
      (some? reconnection-delay-max)    (.reconnectionDelayMax reconnection-delay-max millis)
      (some? default-credentials)       (.defaultCredentials (->UserCredentials default-credentials))
      (some? hb-interval)               (.heartbeatInterval hb-interval millis)
      (some? hb-timeout)                (.heartbeatTimeout hb-timeout millis)
      (some? limit-op-retries)          (.limitRetriesForOperationTo limit-op-retries)
      keep-trying?                      (.keepRetrying)
      (some? op-max-retries)            (.operationMaxRetries op-max-retries)
      (some? op-timeout)                (.operationTimeout op-timeout millis)
      master?                           (.requireMaster master?)
      (some? read-batch-size)           (.readBatchSize read-batch-size)
      (some? buffer-size)               (.bufferSize buffer-size)
      (some? on-overflow)               (->of on-overflow)
      cluster                           (.cluster (->Cluster cluster))
      http                              (.http (->HttpSettings http))
      serialisation-parallelism         (.serializationParallelism serialisation-parallelism)
      serialisation-ordered?            (.serializationOrdered serialisation-ordered?)

      true                              (.build))))

(s/fdef ->Settings
  :args (s/cat :settings ::evst/settings)
  :ret ::ext/settings)

(defn Settings-> [^eventstore.Settings s]
  {::sett/address                      (some-> s (.address) (.toString))
   ::sett/buffer-size                  (.bufferSize s)
   ::sett/cluster                      (-> s (.cluster) u/get-or-else (#(if (some? %) (Cluster-> %) {})))
   ::sett/connection-name              (u/get-or-else (.connectionName s))
   ::sett/connection-timeout           (some-> s (.connectionTimeout) (.toMillis))
   ::sett/hb-interval                  (some-> s (.heartbeatInterval) (.toMillis))
   ::sett/hb-timeout                   (some-> s (.heartbeatTimeout) (.toMillis))
   ::sett/http                         (HttpSettings-> (.http s))
   ::sett/max-reconnections            (.maxReconnections s)
   ::sett/on-overflow                  (of-> (.bufferOverflowStrategy s))
   ::sett/op-max-retries               (.operationMaxRetries s)
   ::sett/op-timeout                   (some-> s (.operationTimeout) (.toMillis))
   ::sett/read-batch-size              (.readBatchSize s)
   ::sett/reconnection-delay-max       (some-> s (.reconnectionDelayMax) (.toMillis))
   ::sett/reconnection-delay-min       (some-> s (.reconnectionDelayMin) (.toMillis))
   ::sett/serialisation-ordered?       (.serializationOrdered s)
   ::sett/serialisation-parallelism    (.serializationParallelism s)

   ::evst/resolve-links?               (.resolveLinkTos s)
   ::evst/credentials                  (-> s (.defaultCredentials) u/get-or-else (#(if (some? %) (UserCredentials-> %) {})))
   ::evst/master?                      (.requireMaster s)})

(s/fdef Settings->
  :args (s/cat :settings ::ext/settings)
  :ret ::evst/settings)



(defn connect
  ([] (connect nil))
  ([settings]
   (let [eventstore-settings
         ,   (if settings (->Settings settings) (eventstore.Settings/Default))
         system
         ,   (akka.actor.ActorSystem/create)
         connection-actor
         ,   (.actorOf system (eventstore.tcp.ConnectionActor/props eventstore-settings))
         esconn
         ,   (eventstore.EsConnection. connection-actor system eventstore-settings)
         esconn-impl
         ,   (eventstore.j.EsConnectionImpl. esconn eventstore-settings (.dispatcher system))]
     {::evst/settings eventstore-settings
      ::evst/actor-system system
      ::evst/connection-actor connection-actor
      :eventstore/connection esconn
      ::evst/connection esconn-impl})))



(defn value-of [e]
  (if (::evst/event-data e)
    (value-of (::evst/event-data e))
    (let [data (::event/data e)]
      (if (::e.data/value data)
        (::e.data/value data)
        data))))

(s/fdef value-of
  :args (s/or :event (s/cat :event ::evst/event)
              :event-data (s/cat :event-data ::evst/event-data))
  :ret ::e.data/value)

(defn type-of [e]
  (-> e ::evst/event-data ::event/type))

(s/fdef type-of
  :args (s/cat :event ::evst/event)
  :ret ::event/type)

(defn event-of
  ([type] (event-of type (UUID/randomUUID) nil))
  ([type data] (event-of type (UUID/randomUUID) data))
  ([type id data] {::event/type type ::event/id id ::event/data data}))

(s/fdef event-of
  :args (s/or :only-type (s/cat :type ::event/type)
              :type-and-data (s/cat :type ::event/type :data ::event/data)
              :with-id (s/cat :type ::event/type :id ::event/id :data ::event/data)))
