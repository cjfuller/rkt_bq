#lang racket

(require "bq-api-client.rkt")
(require rackjure)

(define/contract default-dataset
  (parameter/c string?)
  (make-parameter ""))

(define/contract (process-description desc #:comment [comment "--"])
  (->* (string?) (#:comment string?) string?)
  (regexp-replace* #px"\n" desc (str "\n" comment " ")))

(define-syntax-rule (defquery name #:table table #:query query
                      #:dataset [dataset default-dataset]
                      #:description [description ""] #:tags [tags '()]
                      #:title [title #f])
  (define name
    (hash 'dataset dataset
          'table table
          'query query
          'description description
          'tags (map (lambda (tag) (str "#" tag)) tags)
          'title (or title (symbol->string name)))))


(define/contract (wait-on-job-completion job-id [poll-interval 1])
  (->* (string?) (number?) void?)
  (match (job-status job-id)
    [(list status #f)
     (displayln (str "Waiting for job " job-id ". Status is: " status "."))
     (if (equal? status "DONE")
         (void)
         (begin
           (sleep poll-interval)
           (wait-on-job-completion job-id (min 15 (* 2 poll-interval)))))]
    [(list status err)
     (error (format "Bigquery error: ~a" err))]))

(define/contract (write-query-to-string query)
  (-> dict? string?)
  (let ([table (str (dict-ref query 'dataset)
                    "."
                    (dict-ref query 'table))]
        [tagstring (string-join (dict-ref query 'tags))])
    (format #<<ENDQUERY
-- Title: ~a
-- Description: ~a
-- Tags: ~a
-- Result stored in ~a

~a
ENDQUERY
            (dict-ref query 'title)
            (process-description (dict-ref query 'description))
            tagstring
            table
            (dict-ref query 'query))))

(define/contract (write-query-to-file query query-file)
  (-> dict? string? void?)
  (display-to-file (write-query-to-string query)
                   (expand-user-path query-file)
                   #:exists 'replace))

(define/contract (commit-and-push tempdir query-file)
  (-> string? string? boolean?)
  (system
   (format "cd ~a && git add ~a && git commit -n -m \"Saved query ~a from rkt-bq.\" && git push"
           tempdir query-file query-file)))

(define/contract (store-query-in-repo repo rel-path query)
  (-> string? string? dict? void?)
  (let* ((tempdir (make-temporary-file "rkttemp-temp-clone-~a" 'directory))
         (query-file (path->string (build-path tempdir rel-path))))
    (system (format "git clone ~a ~a" repo tempdir))
    (write-query-to-file query query-file)
    (commit-and-push tempdir rel-path)
    (delete-directory tempdir)
    (void)))

(define/contract (strip-whitespace-and-comments query-string #:comment [comment "--"])
  (->* (string?) (#:comment string?) string?)
  (let ((comment-regex (pregexp (format "\\s*~a.*\n" comment))))
    (regexp-replace* #px"\\s+"
                     (regexp-replace* comment-regex query-string "")
                     " ")))


(define/contract (run-query-sync query #:push-to-repo [push-to-repo #f] #:path [path ""]
                                       #:export-to [export-to #f] #:verbose [verbose #t])
  (->* (dict?) (#:push-to-repo (or/c string? #f)
                #:path string?
                #:export-to (or/c string? #f)
                #:verbose boolean?)
       void?)
  (let ((q (strip-whitespace-and-comments (dict-ref query 'query))))
    (when verbose
      (displayln "Running query: ")
      (displayln (dict-ref query 'query))
      (displayln (str "--> " (dict-ref query 'dataset) "." (dict-ref query 'table))))
    (wait-on-job-completion
     (insert-query-job q (dict-ref query 'dataset) (dict-ref query 'table)))
    (when push-to-repo
      (store-query-in-repo push-to-repo
                           path
                           query))
    (when export-to
      (let ((job-id (insert-export-to-csv-job export-to (dict-ref query 'dataset)
                                              (dict-ref query 'table))))
        (when verbose
          (format "Exporting to ~a. Job id: ~a." export-to job-id))))
    (when verbose (displayln "\n\n"))))


(define/contract select (table-data #:only [only ()] #:except [except ()])
  (->* (dict?) (#:only (listof string?) #:except (listof string?)) dict?)
  (let ((headers (for/list ([h (in-list (dict-ref table-data 'headers))]
                            #:when (and (member h only)
                                        (not (member h except))))
                   h))
        (rows (map (lambda (r)
                     (for/list ([h (in-list (dict-ref table-data 'headers))]
                                [c (in-list r)]
                                #:when (and (member h only)
                                            (not (member h except))))))
                   (dict-ref table-data 'rows))))
    (hash 'headers headers
          'rows rows)))


(define/contract read-table-data (dataset table #:n [n 10])
  (->* (string? string?) (#:n integer?) dict?)
  (hash 'headers (read-table-column-names dataset table)
        'rows (fetch-table-data dataset table #:n n)))

