python -m build
pip-compile pyproject.toml
pip-sync
bumpver update --minor
twine check dist/* && twine upload dist/*