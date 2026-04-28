"""Entry-script package marker.

Makes ``scripts/`` importable so one entry script (e.g. the strategy
runner) can compose another (e.g. the paper runner) without
duplicating its main loop. Each script still works as a direct
``python scripts/<name>.py`` invocation — adding the marker is
backwards compatible.
"""
