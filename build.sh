docker run --rm -v "$(pwd)":/app -it python:3.9.19-bookworm bash -c "cd /app && python -m pip install --upgrade pip setuptools scikit-build wheel cmake && python setup.py bdist_wheel"
