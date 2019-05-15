(ns evst.api
  (:require [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log]
            [evst.base :as base]
            [evst.core :as core]
            [evst.utils :as utils]
            [okku.core :as okku])
  (:import [eventstore.j
            , EventDataBuilder
            , EsTransaction]
           [eventstore.tcp
            , ConnectionActor]
           [akka.actor
            , ActorSystem]))

(create-ns 'evst)
(alias 'evst 'evst)

(create-ns 'evst.version)
(alias 'version 'evst.version)

(create-ns 'evst.direction)
(alias 'direction 'evst.direction)

(def defaults
  (merge
   (core/Settings-> (eventstore.Settings/Default))
   {::evst/version ::version/any
    ::evst/connection nil
    ::evst/credentials core/default-credentials
    ::evst/hard-delete? false
    ::evst/master? false
    ::evst/resolve-links? true}))

;; (def ps-defaults )


(defn merge-with-defaults [settings]
  (merge defaults settings))

(defn add-connection
  ([settings]
   (merge settings (core/connect)))
  ([settings connection-map]
   (merge settings connection-map)))

;; API methods

(defn override [settings overrides]
  (merge settings (apply hash-map overrides)))

(defn write-events
  [stream events settings & overrides]
  (let [{::evst/keys [connection version credentials master?]}
        , (override settings overrides)]
    (base/write-events connection stream version events credentials master?)))

(defn delete-stream
  [stream settings & overrides]
  (let [{::evst/keys [connection version hard-delete? credentials master?]}
        , (override settings overrides)]
    (base/delete-stream connection stream version hard-delete? credentials master?)))

(defn read-event
  [stream event-no settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials master?]}
        , (override settings overrides)]
    (base/read-event connection stream event-no resolve-links? credentials master?)))

(defn read-stream-events-forward
  [stream from-no max-count settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials master?]}
        , (override settings overrides)]
    (base/read-stream-events-forward
     connection stream from-no max-count resolve-links? credentials master?)))

(defn read-stream-events-backward
  [stream from-no max-count settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials master?]}
        , (override settings overrides)]
    (base/read-stream-events-backward
     connection stream from-no max-count resolve-links? credentials master?)))

(defn read-stream-events
  [stream from-no max-count direction settings & overrides]
  (case direction
    ::direction/forward     (apply read-stream-events-forward stream from-no max-count settings overrides)
    ::direction/backward    (apply read-stream-events-backward stream from-no max-count settings overrides)))

(defn read-all-events-forward
  [from-pos max-count settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials master?]}
        , (override settings overrides)]
    (base/read-all-events-forward
     connection from-pos max-count resolve-links? credentials master?)))

(defn read-all-events-backward
  [from-pos max-count settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials master?]}
        , (override settings overrides)]
    (base/read-all-events-backward
     connection from-pos max-count resolve-links? credentials master?)))

(def create-observer base/create-observer)

(defn subscribe-to-stream
  [stream fns settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials]} (override settings overrides)
        observer (create-observer fns)]
    (base/subscribe-to-stream connection stream observer resolve-links? credentials)))

(defn subscribe-to-stream-from
  [stream fns from-no settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials]} (override settings overrides)
        observer (create-observer fns)]
    (base/subscribe-to-stream-from
     connection stream observer from-no resolve-links? credentials)))

(defn subscribe-to-all
  [fns settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials]} (override settings overrides)
        observer (create-observer fns)]
    (base/subscribe-to-all
     connection observer resolve-links? credentials)))

(defn subscribe-to-all-from
  [fns from-pos settings & overrides]
  (let [{::evst/keys [connection resolve-links? credentials]} (override settings overrides)
        observer (create-observer fns)]
    (base/subscribe-to-all
     connection observer from-pos resolve-links? credentials)))

(defn set-stream-metadata
  [stream metadata settings & overrides]
  (let [{::evst/keys [connection version credentials]} (override settings overrides)]
    (base/set-stream-metadata connection stream version metadata credentials)))

(defn get-stream-metadata
  [stream settings & overrides]
  (let [{::evst/keys [connection credentials]} (override settings overrides)]
    (base/get-stream-metadata connection stream credentials)))

;; Transactions

(defn start-transaction ^EsTransaction [stream settings & overrides]
  (let [{::evst/keys [connection version credentials master?]} (override settings overrides)]
    (base/start-transaction connection stream version credentials master?)))

(defn continue-transaction ^EsTransaction [txn-id settings & overrides]
  (let [{::evst/keys [connection credentials]} (override settings overrides)]
    (base/continue-transaction connection txn-id credentials)))

(defn write-transaction [^EsTransaction transaction events]
  (.write transaction events))

(defn commit [^EsTransaction transaction]
  (.commit transaction))

;; PersistentSubscriptions

(create-ns 'evst.persistent)
(alias 'pers 'evst.persistent)

(defn create-ps [stream group ps-settings settings & overrides]
  (let [{::evst/keys [connection credentials]} (override settings overrides)]
    (base/create-persistent-subscription connection stream group ps-settings credentials)))

(defn update-ps [stream group ps-settings settings & overrides]
  (let [{::evst/keys [connection credentials]} (override settings overrides)]
    (base/update-persistent-subscription connection stream group ps-settings credentials)))

(defn delete-ps [stream group settings & overrides]
  (let [{::evst/keys [connection credentials]} (override settings overrides)]
    (base/delete-persistent-subscription connection stream group credentials)))



(defn on-start-default [start]
  (log/infof "Live processing started."))

(defn on-event-default [event]
  (log/infof "Received Event: %s" (class event)))

(defn on-error-default [err]
  (log/infof "Subscription encountered error: %s" (.getMessage err)))

(defn unhandled-default [x]
  (log/infof "Received an unhandled message: %s" (-> x class (.toString))))

(defn route
  "Converts an channels (or functions) into a function that can be used to create an persistent subscriber"

  ([{:keys [on-start on-event on-error unhandled] :as o}] (route o false))

  ([{:keys [on-start on-event on-error unhandled]
     :or {on-start on-start-default on-event on-event-default
          on-error on-error-default unhandled unhandled-default}}
    ch?]

   (if ch?

     (route {:on-start  #(async/go (async/>! on-start %))
             :on-event  #(async/go (async/>! on-event %))
             :on-error  #(async/go (async/>! on-error %))
             :unhandled #(async/go (async/>! unhandled %))
             })

     (fn [m] (cond (instance? eventstore.LiveProcessingStarted$ m) (on-start m)
                  (instance? eventstore.Event m)                  (on-event m)
                  (instance? akka.actor.Status$Failure$ m)        (on-error m)
                  :else                                           (unhandled m))))))

(defn subscribe-pers [stream group observer settings & overrides]
  (let [s (override settings overrides)
        {::evst/keys [connection-actor actor-system credentials]} s
        actor (okku/spawn (okku/actor (onReceive [m] ((route observer) m))) :in actor-system)
        ps-props (eventstore.PersistentSubscriptionActor/props
                  connection-actor actor
                  (core/->EventStream stream) group
                  (scala.Option/apply (core/->UserCredentials credentials))
                  (core/->Settings s) true)
        ps (okku/spawn ps-props :in actor-system)]
    {:client-actor actor
     :ps-actor ps}))
