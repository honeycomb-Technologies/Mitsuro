"""Keep product version synchronization isolated, repeatable, and non-destructive."""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class InterfaceVersionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = {
            "crates/mitsuro-cli/Cargo.toml": '[package]\nversion = "0.10.2"\n',
            "apps/desktop/canvas/Cargo.toml": '[package]\nversion = "0.10.2"\n',
            "apps/mobile/app.json": '{"expo":{"version":"0.10.1","name":"keep"}}',
            "apps/mobile/package.json": '{"version":"0.10.1","scripts":{"keep":"yes"}}',
            "apps/desktop/canvas/packaging/io.mitsuro.canvas.metainfo.xml": '<component><releases><release version="0.10.1" date="2026-08-30"/></releases></component>\n',
        }
        for name, text in self.files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        (self.root / "scripts").mkdir()
        for name in ["check-product-version.sh", "sync-product-version.sh"]:
            shutil.copy(Path(__file__).with_name(name), self.root / "scripts" / name)

    def run_script(self, name):
        return subprocess.run(["sh", self.root / "scripts" / name], capture_output=True, text=True)

    def test_sync_repairs_drift_and_preserves_metadata(self):
        self.assertNotEqual(self.run_script("check-product-version.sh").returncode, 0)
        result = self.run_script("sync-product-version.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.run_script("check-product-version.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        metadata = (self.root / "apps/desktop/canvas/packaging/io.mitsuro.canvas.metainfo.xml").read_text()
        self.assertIn('version="0.10.1"', metadata)
        self.assertIn('version="0.10.2"', metadata)
        self.assertIn('"keep"', (self.root / "apps/mobile/app.json").read_text())
        before = {name: (self.root / name).read_bytes() for name in self.files}
        self.assertEqual(self.run_script("sync-product-version.sh").returncode, 0)
        self.assertEqual(before, {name: (self.root / name).read_bytes() for name in self.files})

    def test_canvas_crate_drift_requires_fixed_group_repair(self):
        self.assertEqual(self.run_script("sync-product-version.sh").returncode, 0)
        (self.root / "apps/desktop/canvas/Cargo.toml").write_text('[package]\nversion = "0.10.1"\n')
        self.assertNotEqual(self.run_script("check-product-version.sh").returncode, 0)


if __name__ == "__main__":
    unittest.main()
