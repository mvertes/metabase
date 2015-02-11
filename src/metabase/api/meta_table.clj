(ns metabase.api.meta-table
  "/api/meta/table endpoints."
  (:require [compojure.core :refer [GET]]
            [korma.core :refer :all]
            [metabase.api.common :refer :all]
            [metabase.db :refer :all]
            (metabase.models [hydrate :refer :all]
                             [database :refer [Database]]
                             [table :refer [Table]])))

(defendpoint GET "/:id/query_metadata" [id]
  (or-404-> (sel :one Table :id id)
    (hydrate :db :fields)))

(defendpoint GET "/" [org]
  (let [db-ids (->> (sel :many [Database :id] :organization_id org)
                    (map :id))]
    (-> (sel :many Table :db_id [in db-ids] (order :name :ASC))
        (simple-batched-hydrate Database :db_id :db))))


(define-routes)