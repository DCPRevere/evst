(ns evst.utils
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :as stest]
            [clojure.core.match :as match])
  (:import [scala , Option]
           [scala.collection , JavaConversions]
           [scala.concurrent Await Future]
           [scala.concurrent.duration Duration]))

;; Scala lists

(defn ->scala-List [xs]
  (-> xs
     scala.collection.JavaConverters/asScalaBufferConverter
     (.asScala)
     (.toList)))

(s/def ::coll-any (s/coll-of any? :kind vector?))

(s/fdef ->scala-List
  :args (s/cat :coll ::coll-any)
  :ret #(instance? scala.collection.immutable.List %)
  :fn #(= (-> % :args :coll count)
          (-> % :ret (.size))))



(defn scala-List-> [l]
  (->> l
     .iterator
     JavaConversions/asJavaIterator
     iterator-seq
     vec))

(s/def ::scala-List
  (s/with-gen #(instance? scala.collection.immutable.List %)
    (fn [] (gen/fmap #(->scala-List %)
                    (s/gen (s/coll-of any? :kind vector?))))))

(s/fdef scala-List->
  :args (s/cat :list ::scala-List)
  :ret ::coll-any
  :fn #(= (-> % :args :list (.size))
          (-> % :ret count)))

;; Create specs for Option fields

(defn optional [s]
  (s/with-gen
    (fn [o] (and (instance? Option o) (if (.isEmpty o) true (s/valid? s (.get o)))))
    (fn [] (gen/fmap #(Option/apply %) (s/gen (s/nilable s))))))

;; Clojure fn to interact with Scala Options

(defn get-or-else
  ([opt] (get-or-else opt nil))
  ([opt def] (if (.isEmpty opt) def (.get opt))))

(s/fdef get-or-else
  :args (s/or :wo-def (s/cat :opt (optional any?))
              :w-def (s/cat :opt (optional any?) :def any?))
  :ret any?
  :fn #(let [ret (-> % :ret)]
          (match/match (-> % :args)
                   [:wo-def {:opt opt}]
                   ,   (if (.isEmpty opt) (= ret nil) (= ret (.get opt)))
                   [:w-def {:opt opt :def def}] [opt def]
                   ,   (if (.isEmpty opt (= ret def) (= ret (.get opt)))))))

(defn ->future
  ([^Future f]
   (->future f (Duration/Inf)))
  ([^Future f ^Duration dur]
   (future (Await/result f dur))))

;; @(->future
;;   (Future/apply
;;    (reify scala.Function0
;;      (apply [this] (java.lang.Thread/sleep 5000)))
;;    (scala.concurrent.ExecutionContext/global)))
