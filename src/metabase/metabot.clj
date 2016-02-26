(ns metabase.metabot
  (:refer-clojure :exclude [list +])
  (:require (clojure [edn :as edn]
                     [string :as str])
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [aleph.http :as aleph]
            [cheshire.core :as json]
            [korma.core :as k]
            (manifold [bus :as bus]
                      [deferred :as d]
                      [stream :as s])
            [metabase.api.common :refer [let-404]]
            [metabase.db :refer [sel]]
            [metabase.integrations.slack :as slack]
            (metabase.models [card :as card]
                             [dashboard :as dashboard])
            [metabase.task.send-pulses :as pulses]
            [metabase.util :as u]))

;;; # ------------------------------------------------------------ Metabot Command Handlers ------------------------------------------------------------

(def ^:private ^:dynamic *channel-id* nil)

(defn- keys-description
  ([message m]
   (str message " " (keys-description m)))
  ([m]
   (apply str (interpose ", " (for [k (sort (keys m))]
                                (str \` (name k) \`))))))

(defn- dispatch-fn [verb tag]
  (let [fn-map (into {} (for [[symb varr] (ns-interns *ns*)
                              :let        [dispatch-token (get (meta varr) tag)]
                              :when       dispatch-token]
                          {(if (true? dispatch-token)
                             (keyword symb)
                             dispatch-token) varr}))]
    (println (u/format-color 'cyan verb) fn-map)
    (fn dispatch*
      ([]
       (keys-description (format "Here's what I can %s:" verb) fn-map))
      ([what & args]
       (if-let [f (fn-map (keyword what))]
         (apply f args)
         (format "I don't know how to %s `%s`.\n%s"
                 verb
                 (if (instance? clojure.lang.Named what)
                   (name what)
                   what)
                 (dispatch*)))))))

(defn- format-exception
  "Format a `Throwable` the way we'd like for posting it on slack."
  [^Throwable e]
  (str "Uh oh! :cry:\n>" (.getMessage e)))

(defmacro ^:private do-async {:style/indent 0} [& body]
  `(do (future (try ~@body
                    (catch Throwable e#
                      (println (u/format-color '~'red (u/filtered-stacktrace e#)))
                      (slack/post-chat-message! *channel-id* (format-exception e#)))))
       nil))

(defn- format-objects
  "Format a sequence of objects as a nice multiline list for use in responses."
  [get-url-fn objects]
  (apply str (interpose "\n" (for [{id :id, obj-name :name, :as obj} objects]
                               (format "%d.  <%s|\"%s\">" id (get-url-fn obj) obj-name)))))

(def ^:private format-cards      (partial format-objects card/url))
(def ^:private format-dashboards (partial format-objects dashboard/url))


(defn- list:cards {:list :cards} [& _]
  (let [cards (sel :many :fields ['Card :id :name] (k/order :id :DESC) (k/limit 20))]
    (str "Here's your " (count cards) " most recent cards:\n" (format-cards cards))))

(defn- list:dashboards {:list :dashboards} [& _]
  (let [dashboards (sel :many :fields ['Dashboard :id :name] (k/order :id :DESC) (k/limit 10))]
    (str "Here's your " (count dashboards) " most recent dashboards:\n" (format-dashboards dashboards))))

(def ^:private ^:metabot list
  (dispatch-fn "list" :list))


(defn- show:card {:show :card}
  ([]
   "Show which card? Give me a part of a card name or its ID and I can show it to you. If you don't know which card you want, try `metabot list cards`.")
  ([card-id-or-name & _]
   (let-404 [{card-id :id, card-name :name} (cond
                                              (integer? card-id-or-name)     (sel :one :fields ['Card :id :name], :id card-id-or-name)
                                              (or (string? card-id-or-name)
                                                  (symbol? card-id-or-name)) (first (u/prog1 (sel :many :fields ['Card :id :name], :name [like (str \% card-id-or-name \%)])
                                                                                      (when (> (count <>) 1)
                                                                                        (throw (Exception. (str "Could you be a little more specific? I found these cards with names that matched:\n"
                                                                                                                (format-cards <>)))))))
                                              :else                          (throw (Exception. (format "I don't know what Card `%s` is. Give me a Card ID or name."))))]
     (do-async (pulses/send-pulse! {:name     card-name
                                    :cards    [{:id card-id}]
                                    :channels [{:channel_type   "slack"
                                                :recipients     []
                                                :details        {:channel *channel-id*}
                                                :schedule_type  "hourly"
                                                :schedule_day   "mon"
                                                :schedule_hour  8
                                                :schedule_frame "first"}]})))
   "Ok, just a second..."))

(def ^:private ^:metabot show
  (dispatch-fn "show" :show))


(declare apply-metabot-fn)

(defn- ^:metabot help [& _]
  (apply-metabot-fn))


(def ^:private kanye-quotes
  (delay (println "Fetching those kanye quotes!")
         (when-let [file (io/file (io/resource "kanye-quotes.edn"))]
           (edn/read-string (slurp file)))))

(defn- ^:metabot kanye [& _]
  (str ":kanye:\n> " (rand-nth @kanye-quotes)))


(def ^:private n (atom 0))

(defn- ^:metabot + [& numbers]
  (apply swap! n clojure.core/+ numbers))


;;; # ------------------------------------------------------------ Metabot Command Dispatch ------------------------------------------------------------

(def ^:private apply-metabot-fn
  (dispatch-fn "understand" :metabot))

(defn- eval-command-str [s]
  (when (seq s)
    (when-let [tokens (seq (edn/read-string (str "(" (-> s
                                                         (str/replace "“" "\"") ; replace smart quotes
                                                         (str/replace "”" "\"")) ")")))]
      (println (u/format-color 'magenta tokens))
      (apply apply-metabot-fn tokens))))


;;; # ------------------------------------------------------------ Metabot Input Handling ------------------------------------------------------------

(defn- message->command-str [{:keys [text]}]
  (u/prog1 (when (seq text)
             (second (re-matches #"^mea?ta?boa?t\s+(.*)$" text)))
    (println (u/format-color 'yellow <>))))

(defn- respond-to-message! [message response]
  (when response
    (let [response (if (coll? response) (str "```\n" (u/pprint-to-str response) "```")
                       (str response))]
      (when (seq response)
        (println (u/format-color 'green response))
        (slack/post-chat-message! (:channel message) response)))))

(defn- handle-slack-message [message]
  (respond-to-message! message (eval-command-str (message->command-str message))))

(defn- human-message? [{event-type :type, subtype :subtype}]
  (and (= event-type "message")
       (not= subtype "bot_message")
       (not= subtype "message_deleted")))

(defn- event-timestamp-ms [{:keys [ts], :or {ts "0"}}]
  (* (Double/parseDouble ts) 1000))


(defonce ^:private websocket (atom nil))

(defn- handle-slack-event [socket start-time event]
  (when-not (= socket @websocket)
    (println "Go home websocket, you're drunk.")
    (s/close! socket)
    (throw (Exception.)))

  (when-let [event (json/parse-string event keyword)]
    (when (and (human-message? event)
               (> (event-timestamp-ms event) start-time))
      (println (u/pprint-to-str 'cyan event))
      (binding [*channel-id* (:channel event)]
        (do-async (handle-slack-message event))))))


;;; # ------------------------------------------------------------ Websocket Connection Stuff ------------------------------------------------------------

(defn- connect-websocket! []
  (when-let [websocket-url (slack/websocket-url)]
    (let [socket @(aleph/websocket-client websocket-url)]
      (println "Connected to websocket:" socket)
      (reset! websocket socket)
      (d/catch (s/consume (partial handle-slack-event socket (System/currentTimeMillis))
                          socket)
          (partial println "ERROR -> ")))))

;;; Websocket monitor

(defonce ^:private websocket-monitor-id (atom nil))

(defn- start-websocket-monitor! []
  (let [id (java.util.UUID/randomUUID)]
    (reset! websocket-monitor-id id)
    (future (loop []
              (Thread/sleep 500)
              (when (= id @websocket-monitor-id)
                (when (and (slack/websocket-url)
                           (or (not @websocket)
                               (s/closed? @websocket)))
                  (log/info "Launching MetaBot... 🤖")
                  (connect-websocket!))
                (recur))))))

(defn start-metabot!
  "#### Instructions

   *Start* the MetaBot! :robot_face:

   This will spin up a background thread that checks if a Slack API token is available and whether
   we have an open Slack WebSocket connection; it will open one when appropriate."
  []
  (start-websocket-monitor!))
