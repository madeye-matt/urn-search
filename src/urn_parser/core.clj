(ns urn-parser.core
    (:gen-class)
    (:use [clojure.pprint][clojure.repl])
    (:require
        [clojure.java.io :as io]
        [clojure.string :as str]
        [clojure.tools.namespace.repl :refer (refresh)]
        [clj-http.client :as client]
        [pantomime.extract :as extract]
        [taoensso.timbre :as timbre
            :refer [log  trace  debug  info  warn  error  fatal  report
                    logf tracef debugf infof warnf errorf fatalf reportf
                    spy get-env]]))

(def config (atom {}))

(def match-all-query { :query { :match_all {} }})

(def full-mapping {
    :page-count  { :type :integer }
    :extended-properties/application { :type :keyword }
    :dc/subject { :type :text }
    :meta/page-count  { :type :integer }
    :date { :type :date }
    :cp/subject { :type :text }
    :dc/creator { :type :keyword }
    :creator { :type :keyword }
    :meta/last-author { :type :keyword }
    :extended-properties/template { :type :keyword }
    :meta/word-count  { :type :integer }
    :dcterms/modified { :type :date }
    :application-name { :type :keyword }
    :last-author  { :type :keyword }
    :revision-number { :type :integer }
    :comments { :type :text }
    :word-count  { :type :integer }
    :meta/author { :type :keyword }
    :template { :type :keyword }
    :x-parsed-by { :type :keyword }
    :last-save-date { :type :date }
    :xmptpg/npages { :type :integer }
    :meta/print-date { :type :date }
    :modified  { :type :date }
    :dc/title { :type :text }
    :keywords { :type :keyword }
    :last-modified { :type :date }
    :title  { :type :text }
    :meta/keyword { :type :keyword }
    :author { :type :keyword }
    :meta/creation-date { :type :date }
    :meta/character-count { :type :integer }
    :meta/save-date { :type :date }
    :w/comments { :type :text }
    :dcterms/created { :type :date }
    :creation-date  { :type :date }
    :comment { :type :text }
    :content-type { :type :keyword }
    (keyword "character count") { :type :integer }
    :cp/revision { :type :integer }
    :extended-properties/company { :type :keyword } 
    :subject  { :type :text }
    :company { :type :keyword }
    :filename { :type :keyword }
    :path { :type :keyword }
    :last-printed  { :type :date }
    :text { :type :text }})

(def optimised-mapping {
    :page-count  { :type :integer }
    :date { :type :date }
    :creator { :type :keyword }
    :application-name { :type :keyword }
    :last-author  { :type :keyword }
    :revision-number { :type :integer }
    :comments { :type :text }
    :word-count  { :type :integer }
    :template { :type :keyword }
    :x-parsed-by { :type :keyword }
    :last-save-date { :type :date }
    :modified  { :type :date }
    :keywords { :type :keyword }
    :last-modified { :type :date }
    :title  { :type :text }
    :author { :type :keyword }
    :creation-date  { :type :date }
    :comment { :type :text }
    :content-type { :type :keyword }
    (keyword "character count") { :type :integer }
    :subject  { :type :text }
    :company { :type :keyword }
    :filename { :type :keyword }
    :path { :type :keyword }
    :last-printed  { :type :date }
    :text { :type :text }})

(timbre/set-level! :debug)

(defn- build-url
  ([base-url suffix] (str base-url suffix))
  ([suffix] (build-url (:elasticsearch-url @config) suffix)))

(defn- parse-row
  [row-str]
  (-> row-str
      (str/trim)
      (str/split #"[ ]+")))

(defn- build-map
  [header data]
  (zipmap header data))

(defn- parse-rows
  [body]
  (let [rows (map parse-row (str/split body #"\n"))
              header (first rows)
              data (rest rows)]
        (map (partial build-map header) data)))

(defn- rest-call
  ([rest-fn url-suffix options]
  (let [url (spy :debug (build-url url-suffix))]
    (spy :debug options)
    (-> url
        (rest-fn options)
        :body)))
  ([rest-fn url-suffix]
   (rest-call rest-fn url-suffix {})))

(defn- rest-get-parse
  [url-suffix]
  (let [response (rest-call client/get url-suffix)]
    (parse-rows response)))

(defn health-check [] (rest-get-parse "/_cat/health?v"))

(defn nodes [] (rest-get-parse "/_cat/nodes?v"))

(defn indices [] (rest-get-parse "/_cat/indices?v"))

(defn create-index
  [index-name]
  (info "Creating index" index-name)
  (rest-call client/put (str "/" index-name) { :as :json }))

(defn create-index-with-mapping
  [index-name mapping]
  (info "Creating index" index-name "with mapping" mapping)
  (let [body { :mappings { :doc { :properties mapping }}}]
    (rest-call client/put (str "/" index-name) { :form-params body :content-type :json :as :json })))

(defn delete-index
  [index-name]
  (info "Deleting index" index-name)
  (rest-call client/delete (str "/" index-name) { :as :json }))

(defn delete-documents
  ([index-name query]
   (info "Deleting documents from" index-name)
   (rest-call client/post (str "/" index-name "/_delete_by_query") {:form-params query :content-type :json}))
  ([index-name] (delete-documents index-name match-all-query)))

(defn create-document
  [index-name doc-map]
  (info "Creating document in" index-name)
  (rest-call client/post (str "/" index-name "/doc") { :form-params doc-map :content-type :json :as :json }))

(defn search
  ([index-name query]
   (info "Searching" index-name)
   (rest-call client/post (str "/" index-name "/_search") {:form-params query :content-type :json :as :json }))
  ([index-name] (search index-name match-all-query)))

(defn search-for-non-empty-field
  [index-name field-name]
  (let [query { :query { :constant_score { :filter { :exists { :field field-name }}}}}]
    (search index-name query)))
    
(defn- find-files
  [root-dir]
    (->> root-dir
        io/file
        file-seq
        (filter #(.isFile %))))

(defn- filter-document-fields
  [document mapping]
  (let [fields (spy :debug (set (keys mapping)))]
    (into {} (filter #(contains? fields (key %)) document))))

(defn index-document
  [index-name mapping doc-path]
  (let [document (extract/parse doc-path)
        filtered-document (spy :debug (-> document (filter-document-fields mapping) (assoc :filename (.getName doc-path)) (assoc :path (.getCanonicalPath doc-path))))]
    (info "Indexing " doc-path "into" index-name)
    (create-document index-name filtered-document)))

(defn index-documents
  [index-name mapping root-directory]
  (let [files (find-files root-directory)]
    (delete-documents index-name)
    (dorun (map (partial index-document index-name mapping) files))))

(defn- load-config
  [config-file]
  (spy :info "config" (->> config-file (load-file) (reset! config))))

(defn -main
  [config-file & args]
  (load-config config-file))
        

