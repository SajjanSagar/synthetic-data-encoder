import importlib.abc
import os
import sys
import types
from typing import Iterable


SAFE_MODE = os.getenv("SAFE_MODE", "true").strip().lower() != "false"

BLOCKED_MODULES = {
    "requests",
    "httpx",
    "urllib",
    "urllib3",
    "socket",
    "aiohttp",
    "boto3",
    "googleapiclient",
}


class _BlockedModule(types.ModuleType):
    def __getattr__(self, name):  # pragma: no cover - defensive
        raise RuntimeError(
            f"SAFE_MODE is enabled. Network module '{self.__name__}' is blocked."
        )


class _BlockedImportFinder(importlib.abc.MetaPathFinder):
    def __init__(self, blocked: Iterable[str]):
        self.blocked = set(blocked)

    def find_spec(self, fullname, path, target=None):  # noqa: D401
        root_name = fullname.split(".", 1)[0]
        if root_name in self.blocked:
            raise RuntimeError(
                f"SAFE_MODE is enabled. Import of '{fullname}' is blocked."
            )
        return None


def _install_blocker(blocked: Iterable[str]) -> None:
    for module_name in blocked:
        if module_name not in sys.modules:
            sys.modules[module_name] = _BlockedModule(module_name)
    sys.meta_path.insert(0, _BlockedImportFinder(blocked))


def enforce_local_only() -> None:
    if SAFE_MODE:
        _install_blocker(BLOCKED_MODULES)
