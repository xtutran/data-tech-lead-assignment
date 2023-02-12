venv:
	/usr/local/anaconda3/bin/python -m venv venv && source venv/bin/activate && pip install pyspark==3.3.1
up_db_load_data:
	docker-compose up -d
summary_data:
	docker-compose -f docker-compose-summary.yml up
down:
	docker-compose down
