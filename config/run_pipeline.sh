#!/bin/bash

# before
#python src/database.py -f data/supermarket_sales.csv -d postgres -t "raw"
#python src/etl.py
#python src/database.py -f data/table1.csv -d postgres -t "processed"
#python src/database.py -f data/table2.csv -d postgres -t "processed"
#python src/database.py -f data/table3.csv -d postgres -t "processed"
#python src/sqljoin.py
#python src/model.py

# after
python src/database.py -id upload-to-database -d postgres
python src/etl.py
python src/database.py -id cleaned-upload-to-database -d postgres
python main.py -t sqljoin
python main.py -t model