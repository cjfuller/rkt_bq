#lang info

(define collection "bq")
(define name "bq")
(define blurb
  '("A Google BigQuery API client library and an interface for doing traceable queries."))
(define primary-file "main.rkt")
(define homepage "https://github.com/cjfuller/rkt_bq")
(define version "0.1.0")
(define scribblings '(("bq.scrbl" ())))
(define deps '("base" "rackjure"))
(define build-deps '("racket-doc"
                     "scribble-lib"))
