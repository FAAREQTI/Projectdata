select *
FROM table1
INNER JOIN table2 ON table1.invoice_id = table2.invoice_id
INNER JOIN table3 ON table1.invoice_id = table3.invoice_id
