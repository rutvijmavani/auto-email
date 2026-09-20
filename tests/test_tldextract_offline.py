"""
tests/test_tldextract_offline.py — every tldextract use must be offline and cache-less

Worker units run with ProtectHome=tmpfs, so ~/.cache is unwritable there. A TLDExtract built
with the default cache_dir logs "unable to cache publicsuffix.org-tlds ..." (a WARNING, so it
emails) on every process start, and the module-level tldextract.extract() also re-downloads
the Public Suffix List. All call sites must therefore pass suffix_list_urls=() and
cache_dir=None, and must not use tldextract.extract().
"""

import ast
import os
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SKIP_DIRS = {".git", ".claude", "venv", ".venv", "node_modules", "__pycache__", "tests"}


def _py_files():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for name in filenames:
            if name.endswith(".py"):
                yield os.path.join(dirpath, name)


def _kwarg(call, name):
    return next((k for k in call.keywords if k.arg == name), None)


class TestTldextractOffline(unittest.TestCase):

    def test_all_call_sites_offline_and_cacheless(self):
        offenders = []
        seen = 0
        for path in _py_files():
            with open(path, encoding="utf-8") as fh:
                tree = ast.parse(fh.read(), filename=path)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
                rel = os.path.relpath(path, ROOT)
                if name == "TLDExtract":
                    seen += 1
                    cache = _kwarg(node, "cache_dir")
                    urls = _kwarg(node, "suffix_list_urls")
                    if not (cache and isinstance(cache.value, ast.Constant) and cache.value.value is None):
                        offenders.append(f"{rel}:{node.lineno} TLDExtract without cache_dir=None")
                    if not (urls and isinstance(urls.value, ast.Tuple) and not urls.value.elts):
                        offenders.append(f"{rel}:{node.lineno} TLDExtract without suffix_list_urls=()")
                elif (isinstance(fn, ast.Attribute) and fn.attr == "extract"
                      and isinstance(fn.value, ast.Name) and fn.value.id == "tldextract"):
                    offenders.append(f"{rel}:{node.lineno} tldextract.extract() (online, cached in ~/.cache)")
        self.assertGreater(seen, 0, "scan found no TLDExtract call sites — test is not scanning")
        self.assertEqual(offenders, [], "\n".join(offenders))

    def test_domain_from_url_uses_offline_extractor(self):
        from jobs.utils import domain_from_url
        self.assertEqual(domain_from_url("https://jobs.stripe.com/careers"), "stripe.com")
        self.assertEqual(domain_from_url("https://www.foo.co.uk/x"), "foo.co.uk")
        self.assertIsNone(domain_from_url(""))


if __name__ == "__main__":
    unittest.main()
