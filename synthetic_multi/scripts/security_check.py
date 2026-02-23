import re
import sys
from pathlib import Path
from typing import Iterable, List, Tuple


BLOCKED_IMPORTS = {
    "requests",
    "httpx",
    "urllib",
    "socket",
    "aiohttp",
    "boto3",
    "google.generativeai",
    "openai",
}

SUSPICIOUS_STRINGS = [
    "http" + "://",
    "https" + "://",
    "api" + "_key",
    "Authorization" + ":",
]


def _scan_file(path: Path) -> List[Tuple[int, str]]:
    findings: List[Tuple[int, str]] = []
    import_pattern = re.compile(r"^\s*(from|import)\s+([a-zA-Z0-9_\.]+)")
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            match = import_pattern.match(line)
            if match:
                module = match.group(2)
                root = module.split(".", 1)[0]
                if module in BLOCKED_IMPORTS or root in BLOCKED_IMPORTS:
                    findings.append((idx, line.rstrip()))
            for needle in SUSPICIOUS_STRINGS:
                if needle in line:
                    findings.append((idx, line.rstrip()))
    return findings


def run_security_check(repo_root: Path) -> bool:
    targets = [repo_root / "src", repo_root / "scripts"]
    failures: List[str] = []
    for target in targets:
        for path in target.rglob("*.py"):
            findings = _scan_file(path)
            if findings:
                for line_no, content in findings:
                    failures.append(f"{path}:{line_no}: {content}")

    if failures:
        print("Security check failed. Risky usage detected:")
        for item in failures:
            print(f"- {item}")
        return False
    return True


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    ok = run_security_check(repo_root)
    if not ok:
        sys.exit(1)
    print("Security check passed.")


if __name__ == "__main__":
    main()
