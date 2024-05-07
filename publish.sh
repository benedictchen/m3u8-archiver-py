./start.sh
python -m build
pip-compile --extra dev pyproject.toml
pip-sync
bumpver update --minor
twine check dist/* && twine upload dist/*