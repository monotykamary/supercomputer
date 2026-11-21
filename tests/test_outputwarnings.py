"""Output selection advisories; no scheduler, VM or local test execution needed."""
import io
import unittest

import pathlib as _pathlib, sys as _sys; _sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parents[1]))  # (the checkout's package, run as tests/x.py)
from supercomputer.outputwarnings import warn_broad_outputs


class OutputWarningsTest(unittest.TestCase):
    def warning(self, job):
        stream = io.StringIO()
        emitted = warn_broad_outputs(job, stream=stream)
        return emitted, stream.getvalue()

    def test_declared_broad_outputs_are_one_warning_per_job(self):
        for key, value in (("output", ".lake"), ("output_glob", "./build/**/*"),
                           ("output_globs", ["report.json", "node_modules/**"]),
                           ("out_dir", "dist/"), ("output_dir", "target")):
            with self.subTest(key=key):
                emitted, text = self.warning({"name": "compile", key: value})
                self.assertTrue(emitted)
                self.assertEqual(text.count("WARNING:"), 1)
                self.assertEqual(text.count("\n"), 1)

    def test_multiple_broad_declarations_still_warn_once(self):
        emitted, text = self.warning({"name": "compile", "output": "build",
                                       "output_globs": [".lake/**", "node_modules/**"]})
        self.assertTrue(emitted)
        self.assertEqual(text.count("WARNING:"), 1)

    def test_whole_root_globs(self):
        for path in (".", "./", "*", "**", "**/*"):
            with self.subTest(path=path):
                self.assertTrue(self.warning({"name": "x", "output_glob": path})[0])

    def test_benign_paths_and_similar_names(self):
        for path in ("reports/result.json", "build.log", "mybuild/**", "node_modules_cache/**",
                     "build/report.json", "./.lake/report.txt", "${SUPERCOMPUTER_OUTPUT_DIR}/proofs"):
            with self.subTest(path=path):
                self.assertEqual(self.warning({"name": "x", "output": path}), (False, ""))
        self.assertEqual(self.warning({"name": "x", "cmd": "echo build && rm -rf node_modules"}), (False, ""))
        self.assertEqual(self.warning({"name": "x", "out": "build"}), (False, ""))
        self.assertEqual(self.warning({"name": "x", "out_dir": "build/reports"}), (False, ""))

    def test_native_command_fallback_only_for_export_copy(self):
        for cmd in ('cp -r .lake "$SUPERCOMPUTER_OUTPUT_DIR"',
                    'rsync -a node_modules/ "$SUPERCOMPUTER_OUTPUT_DIR/modules"'):
            with self.subTest(cmd=cmd):
                self.assertTrue(self.warning({"name": "x", "cmd": cmd})[0])
        for cmd in ('cp build/report.json "$SUPERCOMPUTER_OUTPUT_DIR"',
                    'cp -r build elsewhere', 'echo "cp -r build $SUPERCOMPUTER_OUTPUT_DIR"',
                    'mkdir -p "$SUPERCOMPUTER_OUTPUT_DIR/build"'):
            with self.subTest(cmd=cmd):
                self.assertEqual(self.warning({"name": "x", "cmd": cmd}), (False, ""))
        self.assertEqual(self.warning({"name": "x", "output_glob": "report.json",
                                       "cmd": 'cp -r build "$SUPERCOMPUTER_OUTPUT_DIR"'}), (False, ""))

    def test_newline_escaping_and_single_line_warning(self):
        emitted, text = self.warning({"name": "evil\nWARNING: injected", "output": "build\nmore",
                                       "output_glob": ".lake/**"})
        self.assertTrue(emitted)
        self.assertEqual(text.count("\n"), 1)
        self.assertEqual(text.count("WARNING:"), 2)  # escaped payload is inert on the same line
        self.assertIn('evil\\nWARNING: injected', text)


if __name__ == "__main__":
    unittest.main()
