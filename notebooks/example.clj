; # Including noj & clara
(ns notebooks.example
  (:require
    [clara.rules :refer :all]
    [clara.rules.accumulators :as acc]
    [clojure.set :as set]
    [clojure.walk :as walk]
    [config.core :refer [env]]
    [tablecloth.api :as tc]
    [tech.v3.datatype.functional :as dfn]))
; # Defining clara rule engine
; ## Defining clara records
(defrecord Dataset
  [name columns foreign-keys])

(defrecord ColumnQuery
  [start-col end-col])

(defrecord JoinStep
  [function left-dataset right-dataset on])

(defrecord ConnectionResult
  [start-dataset end-dataset])
; ## Defining clara rules
(defn has-column?
  [columns col]
  (some #{col} columns))

(defrule find-column-datasets
  [ColumnQuery (= ?start-col start-col) (= ?end-col end-col)]
  [?start-ds <- Dataset (has-column? columns ?start-col)]
  [?end-ds <- Dataset (has-column? columns ?end-col)]
  =>
  (insert! (->ConnectionResult ?start-ds ?end-ds)))

(defquery get-connection []
  [?result <- ConnectionResult])

(defrule create-join-step
  {:salience -10}
  [?step <- JoinStep]
  [:not [JoinStep (= ?step this)]] ; Ensure the fact is not already present
  =>
  (do
    (insert! ?step)))

(defquery get-join-steps []
  [?steps <- (acc/all) :from [JoinStep]])

(defn build-join-graph
  [datasets]
  (reduce (fn [graph {:keys [name foreign-keys]}]
            (assoc graph
                   name
                   (set (map (comp :dataset val) foreign-keys))))
          {}
          datasets))

(defn find-join-sequence
  [graph start end]
  (loop [queue (conj clojure.lang.PersistentQueue/EMPTY [start])
         visited #{start}]
    (when-not (empty? queue)
      (let [path (peek queue)
            current (last path)]
        (cond
          (= current end) path
          :else
          (let [neighbors (remove visited (get graph current []))
                new-paths (map #(conj path %) neighbors)]
            (recur (into (pop queue) new-paths)
                   (into visited neighbors))))))))

(defn generate-steps
  [datasets path]
  (when (seq path)
    (for [[left right] (partition 2 1 path)]
      (let [fk (->> (get-in datasets [left :foreign-keys])
                    (keep (fn [[k v]]
                            (when (= (:dataset v) right)
                              {:left-col k :right-col (:column v)})))
                    first)]
        (->JoinStep :tablecloth.api/inner-join
                    (name left)
                    (name right)
                    [{:left-column (:left-col fk)
                      :right-column (:right-col fk)}])))))

(defn nest-join-steps
  [steps]
  (reduce (fn [nested step]
            (if (nil? nested)
              step
              (assoc step :left-dataset nested)))
          nil
          steps))
; ## Defining clara session
(defn get-join-path
  [datasets-map start-col end-col]
  (let [datasets (map (fn [[k v]]
                        (->Dataset k
                                   (set (keys (:columns v)))
                                   (:foreign-keys v)))
                      datasets-map)
        session (-> (mk-session 'notebooks.example :cache false)
                    (insert-all datasets)
                    (insert (->ColumnQuery start-col end-col))
                    fire-rules)

        connection (:?result (first (query session get-connection)))
        graph (build-join-graph datasets)
        path (find-join-sequence graph
                                 (:name (:start-dataset connection))
                                 (:name (:end-dataset connection)))]
    (when-let [steps (generate-steps datasets-map path)]

      (-> session
          (insert-all steps)
          fire-rules
          (query get-join-steps)
          first
          :?steps))))
; # Mock datasets
; ## Defining mock datasets schema
(def datasets
  {:Village {:columns {:village_name :text, :village_code :text, :rural_county_code :text}
             :foreign-keys {:rural_county_code {:dataset :RuralCounty
                                                :column :rural_county_code}}}
   :RuralCounty {:columns {:rural_county_name :text, :rural_county_code :text, :county_code :text}
                 :foreign-keys {:county_code {:dataset :County
                                              :column :county_code}}}
   :County {:columns {:county_name :text, :county_code :text, :province_code :text}
            :foreign-keys {:province_code {:dataset :Province
                                           :column :province_code}}}
   :Province {:columns {:province_name :text, :province_code :text}
              :foreign-keys {}}})
; ## Defining mock datasets
(def mock-datasets
  {:Village (tc/dataset {:village_name ["VillageA" "VillageB" "VillageC"]
                         :village_code ["V001" "V002" "V003"]
                         :rural_county_code ["RC001" "RC002" "RC001"]})
   :RuralCounty (tc/dataset {:rural_county_name ["RuralCountyA" "RuralCountyB"]
                             :rural_county_code ["RC001" "RC002"]
                             :county_code ["C001" "C002"]})
   :County (tc/dataset {:county_name ["CountyA" "CountyB"]
                        :county_code ["C001" "C002"]
                        :province_code ["P001" "P002"]})
   :Province (tc/dataset {:province_name ["ProvinceA" "ProvinceB"]
                          :province_code ["P001" "P002"]})})
; # TableCloth operations

(defn inner-join
  [ds1 ds2 left-column right-column]
  (tc/inner-join ds1 ds2  {:left left-column
                           :right right-column}))

; ### example of inner join
; #### village dataset
(tc/dataset (:Village mock-datasets))
; #### Rural county dataset
(tc/dataset (:RuralCounty mock-datasets))
(inner-join
 (:Village mock-datasets) 
 (:RuralCounty mock-datasets) 
 :rural_county_code :rural_county_code)


(defn get-dataset-by-name
  [name]
  ((keyword name) mock-datasets))

(defn do-inner-join
  [query-map]
  (let [left-dataset (:left-dataset query-map)
        ds1 (if (string? left-dataset)
              (get-dataset-by-name left-dataset)
              left-dataset)
        ds2 (get-dataset-by-name (:right-dataset query-map))
        on-array (first (:on query-map))
        left-column (get on-array :left-column)
        right-column (get on-array :right-column)]
    (inner-join ds2 ds1 right-column left-column)))

#_(nest-join-steps
 (get-join-path datasets :village_code :province_code))
; ## postwalk joining on whole map (DSL)
(defn get-dataset-join-results
  [column1 column2]
  (walk/postwalk #(do (if (and (map? %) (not (nil? (:function %))))
                        (do-inner-join %)
                        %)) (nest-join-steps
                              (get-join-path datasets column1 column2))))
; ### Example result
(get-dataset-join-results :village_code :province_name)
; ## Filter joined results just by required columns
(defn get-dataset-results [columns]
  (tc/select-columns (apply get-dataset-join-results columns) columns))
(get-dataset-results [:village_name :province_name])

#_(println env)
; # Calling clojure function by :namespace/function keyword style
(defn get-fn
  [ns-fn-key]
  (let [ns-name (namespace ns-fn-key)
        fn-name (name ns-fn-key)]
    (ns-resolve (symbol ns-name) (symbol fn-name))))