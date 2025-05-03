(ns notebooks.example
  (:require [tablecloth.api :as tc]
            [tech.v3.datatype.functional :as dfn]
            [clara.rules :refer :all]
            [clojure.set :as set]))

(defrecord Dataset [name columns foreign-keys])
(defrecord ColumnQuery [start-col end-col])
(defrecord JoinStep [function left-dataset right-dataset on])
(defrecord ConnectionResult [start-dataset end-dataset])

(defn has-column? [columns col]
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
    (println "Inserting step:" ?step)
    (insert! ?step)))

(defquery get-join-steps []
  [:?steps]  
  [?steps <- (accumulate :all :from [JoinStep])])  

(defn build-join-graph [datasets]
  (reduce (fn [graph {:keys [name foreign-keys]}]
            (assoc graph 
                   name 
                   (set (map (comp :dataset val) foreign-keys))))
          {}
          datasets))

(defn find-join-sequence [graph start end]
  (loop [queue (conj clojure.lang.PersistentQueue/EMPTY [start])
         visited #{start}]
    (println "start" start)
    (println "end" end)
    (println "Queue:" queue)
    (println "Visited:" visited)
    (when-not (empty? queue)
      (let [path (peek queue)
            current (last path)]
        (println "Path:" path)
        (println "Current:" current)
        (cond
          (= current end) path 
          :else
          (let [neighbors (remove visited (get graph current []))
                new-paths (map #(conj path %) neighbors)]
            (println "Neighbors:" neighbors)
            (recur (into (pop queue) new-paths)
                   (into visited neighbors))))))))

(defn generate-steps [datasets path]
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


(defn get-join-path [datasets-map start-col end-col]
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
                                 (:name (:end-dataset connection)))
        _ (println "path" path)]
    (when-let [steps (generate-steps datasets-map path)
               ]
      (println steps)
      (-> session
          (insert-all steps)
          fire-rules
          (query get-join-steps)
          ;first
          ;:?steps
            ))))

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

(println (get-join-path datasets :village_name :province_name))