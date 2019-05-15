(ns evst.base
  (:require [clojure.core.match :refer [match]]
            [evst.core :as core]
            [evst.utils :as utils])
  (:import [akka.actor ActorSystem]
           [scala.concurrent Future Await]
           [eventstore
            , SubscriptionObserver
            , EventNumber
            , ExpectedVersion
            , UserCredentials
            , Position
            , Position$Last$
            , ReadDirection$Forward$
            , ReadDirection$Backward$
            ]
           [eventstore.j
            , EsConnection
            , EsConnectionFactory
            , ReadDirection
            ]))

(defn write-events
  [connection stream version events credentials master?]
  (utils/->future
   (.writeEvents connection
                 stream
                 (core/->ExpectedVersion version)
                 (map core/->EventData events)
                 (core/->UserCredentials credentials)
                 master?)))

(defn delete-stream
  [connection stream version hard? credentials master?]
  (utils/->future
   (.deleteStream connection
                  stream
                  (core/->ExpectedVersion version)
                  hard?
                  (core/->UserCredentials credentials)
                  master?)))

(defn read-event
  [connection stream event-no resolve-links? credentials master?]
  (utils/->future
   (.readEvent connection
               stream
               (core/->EventNumber event-no)
               resolve-links?
               (core/->UserCredentials credentials)
               master?)))

(defn read-stream-events-forward
  [connection stream from-no max-count resolve-links? credentials master?]
  (utils/->future
   (.readStreamEventsForward connection
                             stream
                             (core/->EventNumber from-no)
                             max-count
                             resolve-links?
                             (core/->UserCredentials credentials)
                             master?)))

(defn read-stream-events-backward
  [connection stream from-no max-count resolve-links? credentials master?]
  (utils/->future
   (.readStreamEventsBackward connection
                              stream
                              (core/->EventNumber from-no)
                              max-count
                              resolve-links?
                              (core/->UserCredentials credentials)
                              master?)))

(defn read-all-events-forward
  [connection from-pos max-count resolve-links? credentials master?]
  (utils/->future
   (.readAllEventsForward connection
                          (core/->Position from-pos)
                          max-count
                          resolve-links?
                          (core/->UserCredentials credentials)
                          master?)))

(defn read-all-events-backward
  [connection from-pos max-count resolve-links? credentials master?]
  (utils/->future
   (.readAllEventsBackward connection
                           (core/->Position from-pos)
                           max-count
                           resolve-links?
                           (core/->UserCredentials credentials)
                           master?)))

(defn create-observer
  [{:keys [on-live-processing-start on-event on-error on-close]}]
  (let [noop (constantly nil)]
    (reify SubscriptionObserver
      (onLiveProcessingStart    [this subscription]          (or on-live-processing-start noop))
      (onEvent                  [this event subscription]    (or on-event noop))
      (onError                  [this e]                     (or on-error noop))
      (onClose                  [this]                       (or on-close noop)))))

(defn subscribe-to-stream
  [connection stream observer resolve-links? credentials]
  (.subscribeToStream connection
                      stream
                      observer
                      resolve-links?
                      (core/->UserCredentials credentials)))

(defn subscribe-to-stream-from
  [connection stream observer from-no resolve-links? credentials]
  (.subscribeToStreamFrom connection
                          stream
                          observer
                          (long from-no)
                          resolve-links?
                          (core/->UserCredentials credentials)))

(defn subscribe-to-all
  [connection resolve-links? credentials]
  (.subscribeToAll connection
                   resolve-links?
                   (core/->UserCredentials credentials)))

(defn subscribe-to-all-from
  [connection observer from-pos resolve-links? credentials]
  (.subscribeToAllFrom connection
                       observer
                       (core/->Position from-pos)
                       resolve-links?
                       (core/->UserCredentials credentials)))

(defn set-stream-metadata
  [connection stream version metadata credentials]
  (utils/->future
   (.setStreamMetadata connection
                       stream
                       (core/->ExpectedVersion version)
                       (bytes metadata)
                       (core/->UserCredentials credentials)))
  )

(defn get-stream-metadata
  [connection stream credentials]
  (utils/->future
   (.getStreamMetadataBytes connection
                            stream
                            (core/->UserCredentials credentials)))
  )

;; Transactions

(defn start-transaction
  [connection stream version credentials master?]
  (.startTransaction connection
                     stream
                     (core/->ExpectedVersion version)
                     (core/->UserCredentials credentials)
                     master?))

(defn continue-transaction
  [connection txn-id credentials]
  (.continueTransaction connection txn-id (core/->UserCredentials credentials)))

;; Persistent subscriptions

(defn create-persistent-subscription
  [connection stream group settings credentials]
  (utils/->future
   (.createPersistentSubscription
    connection stream group
    (core/->PersistentSubscriptionSettings settings)
    (core/->UserCredentials credentials))))

(defn update-persistent-subscription
  [connection stream group settings credentials]
  (utils/->future
   (.updatePersistentSubscription
    connection stream group
    (core/->PersistentSubscriptionSettings settings)
    (core/->UserCredentials credentials))))

(defn delete-persistent-subscription
  [connection stream group credentials]
  (utils/->future
   (.deletePersistentSubscription connection stream group (core/->UserCredentials credentials))))
