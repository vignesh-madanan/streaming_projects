python_venv:
	python3 -m venv ./data_gen_env
	./data_gen_env/bin/python3 -m pip install -r requirements.txt

# run data generator
data_gen:
	./data_gen_env/bin/python3 data_gen.py

# flush data
flush_data:
	./data_gen_env/bin/python3 utils/flush_data.py

flush_and_generate:
	./data_gen_env/bin/python3 utils/flush_data.py
	./data_gen_env/bin/python3 data_gen.py

.PHONY: python_venv data_gen flush_data