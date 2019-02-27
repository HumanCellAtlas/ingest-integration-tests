
test:
	pip install -r requirements.txt
	PYTHONWARNINGS=ignore:ResourceWarning python -m unittest discover
