"""Validate focused law receipts (the full gate also includes these files)."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
for file, verdict in [('good.json', 'proven'), ('broken.json', 'broken')]:
    data = json.loads((root / file).read_text())
    rules = data if isinstance(data, list) else data.get('rules', [])
    assert rules, data
    for rule in rules:
        assert rule['ok'] and rule['laws'], rule
        for law in rule['laws']:
            assert law['verdict'] == verdict, law
            if verdict == 'broken':
                assert law.get('run'), law
            print(file, rule['entry'], law['name'], verdict)
