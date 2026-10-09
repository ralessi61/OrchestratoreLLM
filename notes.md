powershell

poetry install

poetry run pytest -v



\-Puoi lanciare il test runner nativo (che non ha bisogno di installare pytest né di scaricare nulla da internet):

poetry run python src/orchestratore\_llm/tests/run\_tests.py



\-oppure direttamente con il tuo Python locale:

python src/orchestratore\_llm/tests/run\_tests.py





