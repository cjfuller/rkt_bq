#lang at-exp racket

(require rackjure/threading)
(require rackjure/str)
(require json)
(require net/head)
(require net/url-connect)
(require net/url)
(require openssl)

(provide project
         account
         gcloud-path
         get
         post
         ls-tables
         table-exists?
         job-status
         query-job-config
         load-job-config
         extract-csv-config
         insert-job
         insert-query-job
         insert-backup-load-job
         insert-json-load-job
         insert-export-to-csv-job
         read-table-column-names
         fetch-table-data)

(define/contract project
  (parameter/c string?)
  (make-parameter "khanacademy.org:deductive-jet-827"))

; Google account (i.e. e-mail) by whom requests are being made.  Used to
; activate refresh tokens as needed.
(define/contract account
  (parameter/c string?)
  (make-parameter "colin@khanacademy.org"))

; Path to the google cloud sdk bin directory
(define/contract gcloud-path
  (parameter/c string?)
  (make-parameter (str (find-system-path 'home-dir) "google-cloud-sdk/bin/" )))

(define/contract (base-url)
  (-> string?)
  (str "https://www.googleapis.com/bigquery/v2/projects/"
       (project)))

(define/contract (api-url partial)
  (-> string? string?)
  (str (base-url) partial))

(define/contract (credentials)
  (-> dict?)
  (~> (file->string (str (find-system-path 'home-dir) ".config/gcloud/credentials"))
      string->jsexpr
      (dict-ref 'data)
      car
      (dict-ref 'credential)))

(define/contract (token)
  (-> string?)
  (dict-ref (credentials) 'access_token))

(define/contract (refresh-token)
  (-> string?)
  (dict-ref (credentials) 'refresh_token))

(define/contract (auth-headers)
  (-> string?)
  (str "Authorization: Bearer " (token)))

(define/contract (refresh)
  (-> boolean?)
  (system (format "~agcloud auth activate-refresh-token ~a ~a"
                  (gcloud-path)
                  (account)
                  (refresh-token))))

(define-syntax-rule (with-certs-verified body ...)
  (parameterize ([current-https-protocol (ssl-secure-client-context)])
    body ...))

(define-syntax-rule (rmap lst proc)
  (map proc lst))

(define-syntax-rule (tee val form ...)
  (list (~> val form) ...))

(define-syntax-rule (tap val form ...)
  (begin
    (~> val form) ...
    val))

(define/contract (dict-ref-multi dict . keys)
  (->* (dict?) () #:rest (listof any/c) any/c)
  (let ([v (dict-ref dict (car keys) #f)])
    (if (or (null? (cdr keys))
            (not v))
        v
        (apply dict-ref-multi v (cdr keys)))))

(define/contract (dict-set-multi dict keys value)
  (-> dict? (listof any/c) any/c dict?)
  (if (null? (cdr keys))
      (dict-set dict (car keys) value)
      (dict-set-multi (dict-ref dict (car keys)) (cdr keys) value)))

(define/contract (status headers)
  (-> string? number?)
  (some~> (regexp-match #px"HTTP/1.1 (\\d+)" headers)
          second
          string->number))

(define/contract (get relative-url [retry-with-refresh #t])
  (->* (string?) (boolean?) jsexpr?)
  (with-certs-verified
    (let ([url (string->url (api-url relative-url))])
      (let-values ([(response headers) (get-pure-port/headers url (list (auth-headers)) #:status? #t)])
        (let ([parsed-response (string->jsexpr (port->string response))])
          (if (dict-ref parsed-response 'error #f)
              (if (and retry-with-refresh
                       (= 401 (status headers)))
                  (begin
                    (refresh)
                    (get relative-url #f))
                  (error (format "BigQuery error: ~a" parsed-response)))
              parsed-response))))))

(define/contract (post relative-url body [retry-with-refresh #t])
  (->* (string? jsexpr?) (boolean?) jsexpr?)
  (with-certs-verified
    (let* ([url (string->url (api-url relative-url))]
           [body (jsexpr->string body)]
           [response-port (post-impure-port
                           url (string->bytes/utf-8 body)
                           (list (auth-headers) "Content-Type: application/json; charset=UTF-8"))]
           [status (status (purify-port response-port))]
           [parsed-response (string->jsexpr (port->string response-port))])

      (if (and retry-with-refresh
               (dict-ref parsed-response 'error #f)
               (= 401 status))
          (begin
            (refresh)
            (get relative-url #f))
          parsed-response))))


(define/contract (ls-tables dataset #:n [n 20])
  (-> string? #:n integer? (listof string?))
  (~> (get (format "/datasets/~a/tables?maxResults=~a" dataset n))
      (dict-ref 'tables)
      (rmap (lambda (tbl) (dict-ref-multi tbl 'tableReference 'tableId)))))


(define/contract (table-exists? dataset table)
  (-> string? string? boolean?)
  (and (member table (ls-tables dataset)) #t))

(define/contract (job-status job-id)
  (-> string? (listof (or/c jsexpr?)))
  (~> (get (str "/jobs/" job-id))
      (tee
       (dict-ref-multi 'status 'state)
       (dict-ref-multi 'status 'errorResult))))

(define/contract (query-job-config query dataset table)
  (-> string? string? string? dict?)
  (hash 'configuration
        (hash 'query
              (hash
               'allowLargeResults #t
               'destinationTable (hash 'projectId (project)
                                       'datasetId dataset
                                       'tableId table)
               'query query
               'writeDisposition "WRITE_TRUNCATE"))))

(define/contract (load-job-config gcs-fn dataset table #:format [source-format "DATASTORE_BACKUP"])
  (->* (string? string? string?) (#:format string?) dict?)
  (hash 'configuration
        (hash 'load
              (hash 'destinationTable (hash 'projectId (project)
                                            'datasetId dataset
                                            'tableId table)
                    'sourceFormat source-format
                    'sourceUris (list gcs-fn)
                    'writeDisposition "WRITE_TRUNCATE"))))

(define/contract (extract-csv-config gcs-fn dataset table)
  (-> string? string? string? dict?)
  (hash 'configuration
        (hash 'extract
              (hash 'sourceTable (hash 'projectId (project)
                                       'datasetId dataset
                                       'tableId table)
                    'destinationUris (list gcs-fn)))))

(define/contract (insert-job config)
  (-> jsexpr? string?)
  (~> (post "/jobs" config)
      (dict-ref-multi 'jobReference 'jobId)))

(define/contract (insert-query-job query dataset table)
  (-> string? string? string? string?)
  (insert-job (query-job-config query dataset table)))

(define/contract (insert-backup-load-job gcs-fn dataset table)
  (-> string? string? string? string?)
  (insert-job (load-job-config gcs-fn dataset table)))

(define/contract (insert-json-load-job gcs-fn dataset table schema-fields)
  (-> string? string? string? dict? string?)
  (insert-job (dict-set-multi
               (load-job-config gcs-fn dataset table
                                #:format "NEWLINE_DELIMITED_JSON")
               '(configuration load schema)
               (hash 'fields schema-fields))))

(define/contract (insert-export-to-csv-job gcs-fn dataset table)
  (-> string? string? string? string?)
  (insert-job (extract-csv-config gcs-fn dataset table)))

(define/contract (read-table-column-names dataset table)
  (-> string? string? (listof string?))
  (~> (get (format "/datasets/~a/tables/~a" dataset table))
      (dict-ref-multi 'schema 'fields)
      (rmap (lambda (f) (hash-ref f 'name)))))

(define/contract (fetch-table-data dataset table #:n [n 5])
  (->* (string? string?) (#:n integer?) (listof jsexpr?))
  ; TODO: handle repeated values correctly
  (~> (get (format "/datasets/~a/tables/~a/data?maxResults=~a"
                   dataset table n))
      (dict-ref 'rows)
      (rmap (lambda (r) (dict-ref r 'f)))
      (rmap (lambda (col)
              (map (lambda (val) (dict-ref val 'v))
                   col)))))
