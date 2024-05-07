./start.sh
python -m pip install pip-tools
python -m pip install build twine bumpver
python -m build
pip-compile --extra dev pyproject.toml
pip-sync
bumpver update --minor
twine check dist/*
twine upload dist/*  --skip-existing --verbose
