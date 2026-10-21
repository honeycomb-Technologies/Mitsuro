"""Publication boundary regressions for the Canvas desktop transition."""
import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("canvas_release", Path(__file__).with_name("check-canvas-release.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CanvasReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.commit = "123456789abc" + "0" * 28
        self.name = f"mitsuro-canvas-0.10.2-alpha-x86_64-{self.commit[:12]}.tar.gz"
        self.archive = self.root / self.name
        self.archive.write_bytes(b"synthetic archive for checksum validation")
        self.manifest = self.root / (self.name + ".sha256")
        self.manifest.write_text(f"{hashlib.sha256(self.archive.read_bytes()).hexdigest()}  {self.name}\n")

    def validate(self, version="0.10.2", commit=None):
        return module.validate(self.root, version, commit or self.commit)

    def test_exact_pair(self):
        self.assertEqual(self.validate(), self.name)

    def test_modified_archive(self):
        self.archive.write_bytes(b"changed")
        with self.assertRaises(ValueError):
            self.validate()

    def test_wrong_version_or_source(self):
        for version, commit in [("0.10.3", self.commit), ("0.10.2", "f" * 40)]:
            with self.subTest(version=version, commit=commit), self.assertRaises(ValueError):
                self.validate(version, commit)

    def test_missing_manifest(self):
        self.manifest.unlink()
        with self.assertRaises(ValueError):
            self.validate()

    def test_duplicate_basename(self):
        duplicate = self.root / "duplicate"
        duplicate.mkdir()
        (duplicate / self.name).write_bytes(self.archive.read_bytes())
        with self.assertRaises(ValueError):
            self.validate()

    def test_manifest_cannot_name_another_payload_or_add_a_line(self):
        original = self.manifest.read_text()
        for text in [original.replace(self.name, "other.tar.gz"), original + original]:
            self.manifest.write_text(text)
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.validate()

    def test_separated_manifest(self):
        other = self.root / "other"
        other.mkdir()
        self.manifest.rename(other / self.manifest.name)
        with self.assertRaises(ValueError):
            self.validate()

    def test_old_desktop_package_does_not_replace_canvas(self):
        self.archive.rename(self.root / "mitsuro-desktop.deb")
        with self.assertRaises(ValueError):
            self.validate()

    def test_invalid_identity(self):
        for version, commit in [("../0.10.2", self.commit), ("0.10.2", "short")]:
            with self.subTest(version=version), self.assertRaises(ValueError):
                self.validate(version, commit)


if __name__ == "__main__":
    unittest.main()
