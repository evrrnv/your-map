.PHONY: install clean production serve

serve: clean
	export FLASK_APP=server.py && \
	export FLASK_DEBUG=1 && \
	cd src && \
	flask run --debugger --port 8002

production: clean
	export LC_ALL=C.UTF-8 && \
	export LANG=C.UTF-8 && \
	export FLASK_APP=server.py && \
	export FLASK_DEBUG=0 && \
	cd src && \
	flask run --port 8002

install:
	python -m pip install --no-cache-dir --upgrade -r requirements.txt

clean:
	rm -rf __pycache__*
	rm -rf src/__pycache__*
	rm -rf venv/*
