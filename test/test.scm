(use-modules (dbi dbi))

(define bad-hnd (dbi-open "odbc" "DSN=completelyinvaliddsn"))
(when (= (car (dbi-get_status bad-hnd)) 0)
  (display "Unexpectedly succeded in opening invalid DSN")
  (newline))

(define hnd (dbi-open "odbc" "DSN=guiledbitest"))
(unless (= (car (dbi-get_status hnd)) 0)
  (display "Unexpected error: ")
  (display (cdr (dbi-get_status hnd)))
  (newline))

(dbi-query hnd "select * from testtable")
(unless (= (car (dbi-get_status hnd)) 0)
  (display "Unexpected error: ")
  (display (cdr (dbi-get_status hnd)))
  (newline))

(define row (dbi-get_row hnd))
(display (dbi-get_status hnd))(newline)
(display row)(newline)

(define row1 (dbi-get_row hnd))
(display (dbi-get_status hnd))(newline)
(display row1)(newline)

