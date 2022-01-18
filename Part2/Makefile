install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	# Test setting up, loading data and tearing down resources. Additional tests can go here.
	python test_execute_ddl_process.py

lint:
	pylint --disable=R,C,W1203 PrimaryBid/Part2/*.py

all: install lint test