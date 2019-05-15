(ns evst.objs
  (:require
   [cheshire.core :as json]
   [clj-time.coerce :as c]
   [clj-time.core :as t]
   [clojure.core.match :as match]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.test.alpha :as stest]
   [evst.utils :as u]
   [expound.alpha :as exp])
  (:use [clojure.core.match.regex])
  (:import [scala , Option]
           [scala.collection , JavaConversions]
           [eventstore.j , DeleteStreamBuilder]))

(create-ns 'evst)
(alias 'evst 'evst)

(create-ns 'evst.external)
(alias 'ext 'evst.external)

(create-ns 'evst.credentials)
(alias 'creds 'evst.credentials)

(s/def ::creds/login (s/and string? seq))
(s/def ::creds/password (s/and string? seq))

(s/def ::evst/credentials
  (s/keys :req [::creds/login
                ::creds/password]))

(defn ->UserCredentials
  ([] (eventstore.UserCredentials/DefaultAdmin))
  ([{::creds/keys [login password]}]
     (eventstore.UserCredentials. login password)))

(s/fdef ->UserCredentials
  :args (s/alt :default (s/cat)
               :custom ::credentials)
  :ret #(instance? eventstore.UserCredentials %))



(defn UserCredentials-> [o]
  {::creds/login (.login o)
   ::creds/password (.password o)})

(s/def ::ext/credentials
  (s/with-gen #(instance? eventstore.UserCredentials %)
    (fn [] (gen/fmap (fn [[x y]] (eventstore.UserCredentials. x y))
                    (s/gen (s/cat :login ::creds/login :pass ::creds/password))))))

(s/fdef UserCredentials->
  :args (s/cat :credentials ::ext/credentials)
  :ret ::evst/credentials
  :fn (fn [x] (let [input (-> x :args :credentials)
                   ret (-> x :ret)]
               (and (= (.login input) (::creds/login ret))
                    (= (.password input) (::creds/password ret))))))



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

(s/def ::position
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
  :args (s/cat :position ::position)
  :ret #(instance? eventstore.Position %))



(defmulti Position-> class)

(defmethod Position-> eventstore.Position$Last$ [o] ::position/last)

(defmethod Position-> eventstore.Position$Exact [o]
  {::position/commit (.commitPosition o)
   ::position/prepare (.preparePosition o)})

(s/fdef Position->
  :args (s/cat :position ::ext/position)
  :ret ::position)



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

(s/def ::stream
  (s/or :all ::stream/all
        :id ::stream/has-id))

(s/def ::stream/type
  #{::stream/all ::stream/system ::stream/plain ::stream/metadata ::stream/undefined})

(s/def ::stream/system? boolean?)

(s/def ::stream/metadata? boolean?)

(s/def ::stream/id string?)

(s/def ::stream/value string?)

(s/def ::stream/prefix string?)

(s/def ::stream/metadata ::stream)

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
  (if (= type :all)
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
  :args (s/cat :stream ::stream)
  :return ::ext/stream)



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
  :ret ::stream)



;; Event data

(create-ns 'evst.event)
(alias 'event 'evst.event)

(s/def ::event/type (s/and string? seq))
(s/def ::event/id uuid?)

(create-ns 'evst.event.data)
(alias 'e.data 'evst.event.data)

(create-ns 'evst.event.data.type)
(alias 'e.d.type 'evst.event.data.type)

(s/def ::e.data/value string?)
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
  (eventstore.Content/apply (::e.data/value m)))

(defmethod ->Content ::e.d.type/json [m]
  (.apply (eventstore.Content$Json$.)
          (json/generate-string (::e.data/value m) {:pretty true})))

;; TODO add ->Content fspec and test

(s/fdef ->Content
  :args (s/cat :content ::event/data)
  :ret ::ext/content)



(defmulti Content->
  (fn [o]
    (if (nil? o) o
        (class (.contentType o)))))

(defmethod Content->
  eventstore.ContentType$Json$ [o]
  {::e.data/value (json/parse-string (.utf8String (.value o)))
   ::e.data/type ::e.d.type/json})

(defmethod Content->
  :default [o]
  {::e.data/value (.utf8String (.value o))
   ::e.data/type ::e.d.type/binary})

(s/fdef Content->
  :args (s/cat :content ::ext/content)
  :ret ::event/data)


;; Event data

(s/def ::event-data (s/keys :req [::event/type ::event/id] :opt [::event/data ::event/metadata]))

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
  :args (s/cat :event ::event-data)
  :ret ::ext/event-data)



(defn EventData-> [o]
  (let [data (Content-> (.data o))
        metadata (Content-> (.metadata o))]
    (merge {::event/type (.eventType o)
            ::event/id (.eventId o)}
           (if-not (empty? (::e.data/value data)) {::event/data data})
           (if-not (empty? (::e.data/value metadata)) {::event/metadata metadata}))))

(s/fdef EventData->
  :args (s/cat :event-data ::ext/event-data)
  :ret ::event-data)



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

(s/def ::date-time
  (s/with-gen #(instance? org.joda.time.DateTime %)
    (fn [] (gen/fmap (fn [l] (c/from-long l)) (s/gen int?)))))

(s/def ::created (s/nilable ::date-time))

(s/def ::event
  (s/keys :req [::stream ::event/number ::event-data ::created]
          :opt [::event/record]))

(s/def ::event/indexed
  (s/keys :req [::event ::position]))

(defmulti Event-> class)

(defmethod Event-> eventstore.EventRecord [o]
  {::stream (EventStream-> (.streamId o)) ;; TODO change EventStream to EventStream.Id
   ::event/number (EventNumber-> (.number o))
   ::event-data (EventData-> (.data o))
   ::created (u/get-or-else (.created o) nil)})

(defmethod Event-> eventstore.ResolvedEvent [o]
  {::stream (EventStream-> (.streamId o))
   ::event/number (EventNumber-> (.number o))
   ::event-data (EventData-> (.data o))
   ::event/record (Event-> (.record o))
   ::created (u/get-or-else (.created o) nil)})

(s/fdef Event->
  :args (s/cat :event ::ext/event)
  :ret ::event)



(defn IndexedEvent-> [o]
  {::event (Event-> (.event o))
   ::position (Position-> (.position o))})



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
  :args (s/cat :version ::version)
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

(s/def ::result/write (s/keys :req [::result/next-version ::position]))

(s/def ::ext/write-result
  (s/with-gen #(instance? eventstore.WriteResult %)
    (fn [] (gen/fmap (fn [[version position]] (eventstore.WriteResult. version position))
                    (s/gen (s/tuple ::ext/version-exact ::ext/position))))))

(defn WriteResult-> [o]
  {::result/next-version (ExpectedVersion-> (.nextExpectedVersion o))
   ::position (Position-> (.logPosition o))})

(s/fdef WriteResult->
  :args (s/cat :result ::ext/write-result)
  :req ::result/write)



(s/def ::result/delete (s/keys :req [::position]))

(s/def ::ext/delete-result
  (s/with-gen #(instance? eventstore.DeleteResult %)
    (fn [] (gen/fmap #(eventstore.DeleteResult. %) (s/gen ::ext/position)))))

(defn DeleteResult-> [o]
  {::position (Position-> (.logPosition o))})

(s/fdef DeleteResult->
  :args (s/cat :delete ::ext/delete-result)
  :ret ::result/delete)



;; Direction

(create-ns 'evst.direction)
(alias 'direction 'evst.direction)

(s/def ::direction
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
  :args (s/cat :direction ::direction)
  :ret #(instance? eventstore.ReadDirection %)
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
  :ret ::direction)



(s/def ::client-version (s/and nat-int? #(<= % 1000)))

(s/def ::connection-name string?)


(s/def ::identify-client
  (s/keys :req [::client-version ::connection-name]))

(defn ->IdentifyClient
  [{::evst/keys [client-version connection-name]}]
  (eventstore.IdentifyClient. client-version (Option/apply connection-name)))

(s/fdef ->IdentifyClient
  :args (s/cat :client ::identify-client)
  :ret #(instance? eventstore.IdentifyClient %)
  :fn #(s/and (= (.version (:ret %))
                 (-> % :args :client ::client-version))
              (= (.connectionName (:ret %))
                 (-> % :args :client ::connection-name))))



(s/def ::master? boolean?)
(s/def ::hard? boolean?)
(s/def ::resolve-links? boolean?)

(s/def ::events (s/coll-of ::event :gen-max 5))

(s/def ::event-datas (s/coll-of ::event-data :gen-max 5))

(s/def ::write-events
  (s/keys :req [::stream/has-id ::event-datas] :opt [::version ::master?]))

(defn ->WriteEvents
  [{::keys [event-datas version master?]
    ::stream/keys [has-id]
    :or {version ::version/any
         master? false}}]
  (eventstore.WriteEvents. (->EventStream has-id)
                           (u/->scala-List (map ->EventData event-datas))
                           (->ExpectedVersion version)
                           master?))

(s/fdef ->WriteEvents
  :args (s/cat :write-events ::write-events)
  :ret #(instance? eventstore.WriteEvents %))

;; (defn ReadEventCompleted-> [o]
;;   {::event (Event-> (.event o))})

;; (s/fdef ReadEventCompleted->
;;   :args (s/cat :read-event-completed ::ext/read-event-completed)
;;   :ret ::read-event-completed)

(s/def ::ext/write-events-completed
  (s/with-gen #(instance? eventstore.WriteEventsCompleted %)
    (fn [] (gen/fmap #(eventstore.WriteEventsCompleted. % %2)
                    (s/gen (s/tuple (u/optional ::number/range)
                                    (u/optional ::position)))))))

(s/def ::write-events-completed
  (s/keys :req [::number/range ::position]))

(defn WriteEventsCompleted-> [o]
  (let [range (some-> o (.numberRange) u/get-or-else EventNumberRange->)
        position (some-> o (.position) u/get-or-else Position->)]
    {::number/range range
     ::position position}))

(s/fdef WriteEventsCompleted->
  :args (s/cat :write-events-completed ::ext/write-events-completed)
  :ret ::write-events-completed)



(s/def ::delete-stream
  (s/keys :req [::stream/has-id] :opt [::version ::hard? ::master?]))

(defn ->DeleteStream
  [{::keys [hard? master?]
    ::version/keys [existing]
    ::stream/keys [has-id]
    :or {existing ::version/any
         hard? false master? false}}]
  (eventstore.DeleteStream. (->EventStream has-id)
                            (->ExpectedVersion existing)
                            hard? master?))

(s/fdef ->DeleteStream
  :args (s/cat :delete-stream ::delete-stream)
  :ret #(instance? eventstore.DeleteStream %))



;; Transactions

(create-ns 'evst.transaction)
(alias 'transaction 'evst.transaction)

(s/def ::transaction-start
  (s/keys :req [::stream/has-id] :opt [::version ::master?]))

(defn ->TransactionStart
  [{::keys [version master?]
    ::stream/keys [has-id]
    :or {version ::version/any
         master? false}}]
  (eventstore.TransactionStart.
   (->EventStream has-id)
   (->ExpectedVersion version)
   master?))

(s/fdef ->TransactionStart
  :args (s/cat :transaction-start ::transaction-start)
  :ret #(instance? eventstore.TransactionStart %))



(s/def ::transaction/id (s/and int? #(>= % 0)))
(s/def ::transaction-write
  (s/keys :req [::transaction/id ::event-datas] :opt [::master?]))

(defn ->TransactionWrite
  [{::keys [event-datas master?]
    ::transaction/keys [id]
    :or {master? false}}]
  (eventstore.TransactionWrite. id (u/->scala-List (map ->EventData event-datas)) master?))

(s/fdef ->TransactionWrite
  :args (s/cat :transaction-write ::transaction-write)
  :ret #(instance? eventstore.TransactionWrite %)
  )



(s/def ::transaction-commit
  (s/keys :req [::transaction/id] :opt [::master?]))

(defn ->TransactionCommit
  [{::keys [master?] ::transaction/keys [id]
    :or {master? false}}]
  (eventstore.TransactionCommit. id master?))

(s/fdef ->TransactionCommit
  :args (s/cat :commit ::transaction-commit)
  :ret #(instance? eventstore.TransactionCommit %))



(s/def ::read-event
  (s/keys :req [::stream/has-id ::event/number] :opt [::resolve-links? ::master?]))

(defn ->ReadEvent
  [{::event/keys [number]
    ::stream/keys [has-id]
    ::keys [resolve-links? master?]
    :or {resolve-links? true master? false}}]
  (eventstore.ReadEvent. (->EventStream has-id)
                         (->EventNumber number)
                         resolve-links? master?))

(s/fdef ->ReadEvent
  :args (s/cat :read-event ::read-event)
  :ret #(instance? eventstore.ReadEvent %))



(s/def ::ext/read-event-completed
  (s/with-gen #(instance? eventstore.ReadEventCompleted %)
    (fn [] (gen/fmap #(eventstore.ReadEventCompleted. %)
                    (s/gen ::ext/event)))))

(s/def ::read-event-completed
  (s/keys :req [::event]))

(defn ReadEventCompleted-> [o]
  {::event (Event-> (.event o))})

(s/fdef ReadEventCompleted->
  :args (s/cat :read-event-completed ::ext/read-event-completed)
  :ret ::read-event-completed)



(s/def ::event/max-count (s/and pos-int? #(<= % 10000)))
(s/def ::read-stream-events
  (s/keys :req [::stream/has-id]
          :opt [::event/number ::event/max-count ::direction ::resolve-links? ::master?]))

(defn ->ReadStreamEvents
  [{::keys [direction resolve-links? master?]
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
  :args (s/cat :read-stream-events ::read-stream-events)
  :ret #(instance? eventstore.ReadStreamEvents %))



(s/def ::number/next ::event/number)
(s/def ::number/previous ::number/exact)
(s/def ::end-of-stream? boolean?)
(s/def ::position/last-commit int?)

(s/def ::result/read-stream-events
  (s/keys :req [::events ::number/next ::number/previous
                ::end-of-stream? ::position/last-commit ::direction]))

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
  {::events (map Event-> (u/scala-List-> (.events o)))
   ::number/next (EventNumber-> (.nextEventNumber o))
   ::number/previous (EventNumber-> (.lastEventNumber o))
   ::end-of-stream? (.endOfStream o)
   ::position/last-commit (.lastCommitPosition o)
   ::direction (ReadDirection-> (.direction o))})

(s/fdef ReadStreamEventsCompleted->
  :args (s/cat :read ::ext/read-stream-events-completed)
  :ret ::result/read-stream-events)



(s/def ::read-all-events
  (s/keys :opt [::position ::event/max-count ::direction ::resolve-links? ::master?]))

(defn ->ReadAllEvents
  [{::keys [position direction resolve-links? master?]
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
  :args (s/cat :read-all-events ::read-all-events)
  :ret #(instance? eventstore.ReadAllEvents %))



(s/def ::events-indexed (s/coll-of ::event/indexed :gen-max 5))

(s/def ::position/previous ::position/exact)
(s/def ::position/next ::position/exact)

(s/def ::result/read-all-events
  (s/keys :req [::events-indexed ::position/previous ::position/next ::direction]))

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
    {::events-indexed events
     ::position/previous position
     ::position/next next-position
     ::direction direction}))

(s/fdef ReadAllEventsCompleted->
  :args (s/cat :read ::ext/read-all-events-completed)
  :ret ::result/read-all-events)


;; Persistent subscriptions

(create-ns 'evst.persistent)
(alias 'persistent 'evst.persistent)


;; Settings

(s/def ::persistent/buffer-size int?)

(s/def ::persistent/start-from ::event/number)
(s/def ::persistent/extra-statistics boolean?)
(s/def ::persistent/message-timeout-millis (s/and nat-int? #(< % 31557600))) ;; duration?
(s/def ::persistent/max-retry-count int?)
(s/def ::persistent/live-buffer-size int?)
(s/def ::persistent/read-batch-size int?)
(s/def ::persistent/history-buffer-size int?)
(s/def ::persistent/check-point-after-secs (s/and nat-int? #(< % 31557600)))
(s/def ::persistent/min-check-point-count int?)
(s/def ::persistent/max-check-point-count int?)
(s/def ::persistent/max-subscriber-count int?)
(s/def ::persistent/consumer-strategy
  (s/or :stock #{::persistent/dispatch-to-single
                 ::persistent/round-robin}
        :custom (s/and string? seq)))

(s/def ::persistent/settings
  (s/keys :opt [
                ::resolve-links?
                ::persistent/start-from
                ::persistent/extra-statistics
                ::persistent/message-timeout-millis
                ::persistent/max-retry-count
                ::persistent/live-buffer-size
                ::persistent/read-batch-size
                ::persistent/history-buffer-size
                ::persistent/check-point-after-secs
                ::persistent/min-check-point-count
                ::persistent/max-check-point-count
                ::persistent/max-subscriber-count
                ::persistent/consumer-strategy
                ]))

(defn ->ConsumerStrategy [o]
  (match/match
   o   ::persistent/dispatch-to-single (eventstore.ConsumerStrategy$DispatchToSingle$.)
   ,   ::persistent/round-robin (eventstore.ConsumerStrategy$RoundRobin$.)
   ,   :else (eventstore.ConsumerStrategy/apply o)
   ))

(s/fdef ->ConsumerStrategy
  :args (s/cat :strategy ::persistent/consumer-strategy)
  :ret #(instance? eventstore.ConsumerStrategy %))



(defn ->PersistentSubscriptionSettings
  [{::keys [resolve-links?]
    ::persistent/keys [start-from extra-statistics message-timeout-millis
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
  :args (s/cat :settings ::persistent/settings)
  :ret #(instance? eventstore.PersistentSubscriptionSettings %))



(s/def ::persistent/group (s/and string? seq))

(s/def ::persistent/create
  (s/keys :req [::stream/has-id ::persistent/group]
          :opt [::persistent/settings]))

(defn ->CreatePersistentSubscription
  [{::stream/keys [has-id] ::persistent/keys [group settings] :or {settings {}}}]
  (eventstore.PersistentSubscription$Create.
   (->EventStream has-id) group (->PersistentSubscriptionSettings settings)))

(s/fdef ->CreatePersistentSubscription
  :args (s/cat :create ::persistent/create)
  :ret #(instance? eventstore.PersistentSubscription$Create %))



(s/def ::persistent/update
  (s/keys :req [::stream/has-id ::persistent/group]
          :opt [::persistent/settings]))

(defn ->UpdatePersistentSubscription
  [{::stream/keys [has-id] ::persistent/keys [group settings] :or {settings {}}}]
  (eventstore.PersistentSubscription$Update.
   (->EventStream has-id) group (->PersistentSubscriptionSettings settings)))

(s/fdef ->UpdatePersistentSubscription
  :args (s/cat :update ::persistent/update)
  :ret #(instance? eventstore.PersistentSubscription$Update %))



(s/def ::persistent/delete
  (s/keys :req [::stream/has-id ::persistent/group]))

(defn ->DeletePersistentSubscription
  [{::stream/keys [has-id] ::persistent/keys [group]}]
  (eventstore.PersistentSubscription$Delete.
   (->EventStream has-id) group))

(s/fdef ->DeletePersistentSubscription
  :args (s/cat :delete ::persistent/delete)
  :ret #(instance? eventstore.PersistentSubscription$Delete %))



(s/def ::persistent/id (s/and string? seq))
(s/def ::event-ids (s/coll-of ::event/id :min-count 1 :gen-max 5))
(s/def ::persistent/ack (s/keys :req [::persistent/id ::event-ids]))

(defn ->Ack
  [{::keys [event-ids] ::persistent/keys [id]}]
  (eventstore.PersistentSubscription$Ack. id (u/->scala-List event-ids)))

(s/fdef ->Ack
  :args (s/cat :ack ::persistent/ack)
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

(s/def ::persistent/nak (s/keys :req [::persistent/id ::event-ids ::nak/action] :opt [::nak/message]))

(defn ->Nak
  [{::keys [event-ids] ::persistent/keys [id] ::nak/keys [action message]}]
  (eventstore.PersistentSubscription$Nak.
   id (u/->scala-List event-ids) (->NakAction action) (Option/apply message)))

(s/fdef ->Nak
  :args (s/cat :nak ::persistent/nak)
  :ret #(instance? eventstore.PersistentSubscription$Nak %))



(s/def ::persistent/connect
  (s/keys :req [::stream/has-id ::persistent/group] :opt [::persistent/buffer-size]))

(defn ->Connect
  [{::stream/keys [has-id]
    ::keys [buffer-size]
    ::persistent/keys [group]
    :or {buffer-size 10}}]
  (eventstore.PersistentSubscription$Connect.
   (->EventStream has-id) group buffer-size))

(s/fdef ->Connect
  :args (s/cat :connect ::persistent/connect)
  :ret #(instance? eventstore.PersistentSubscription$Connect %))



(s/def ::subscribe
  (s/keys :req [::stream] :opt [::resolve-links?]))

(defn ->SubscribeTo
  [{::keys [stream resolve-links?] :or {resolve-links? true}}]
  (eventstore.SubscribeTo. (->EventStream stream) resolve-links?))

(s/fdef ->SubscribeTo
  :args (s/cat :subscribe ::subscribe)
  :ret #(instance? eventstore.SubscribeTo %))



(s/def ::unsubscribe
  (s/keys :req []))

(defn ->Unsubscribe
  ([] (eventstore.Unsubscribe/out))
  ([x] (->Unsubscribe)))

(s/fdef ->Unsubscribe
  :args (s/cat)
  :ret #(instance? eventstore.Unsubscribe$ %))



(s/def ::scavenge
  (s/keys :req []))

(defn ->ScavengeDatabase
  ([] (eventstore.ScavengeDatabase/out))
  ([x] (->ScavengeDatabase)))

(s/fdef ->ScavengeDatabase
  :args (s/cat)
  :ret #(instance? eventstore.ScavengeDatabase$ %))



(s/def ::authenticate
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
