from enum import Enum, auto
from typing import Iterable

import nox
from nox import Session


class Mode(Enum):
    Fix = auto()
    Check = auto()


def _type_check(session: Session) -> None:
    session.run("mypy", ".")


def _lint(session: Session) -> None:
    session.run("pylint", "./src/", "./tests/")


def _code_format(session: Session, mode: Mode) -> None:
    isort = ["isort"]
    black = ["black"]
    isort = isort if mode == Mode.Fix else isort + ["--check"]
    black = black if mode == Mode.Fix else black + ["--check"]
    session.run(*isort, ".")
    session.run(*black, ".")


@nox.session(python=False)
def fix(session: Session) -> None:
    """Runs all automated fixes on the code base"""
    _code_format(session, Mode.Fix)


@nox.session(name="check", python=False)
def check(session: nox.Session) -> None:
    """Runs all available checks on the project"""
    _type_check(session)
    _lint(session)
    _code_format(session, Mode.Check)


@nox.session(name="test", python=False)
def test(session: nox.Session) -> None:
    """Runs all tests, incl. integration tests"""
    pytest = ["pytest"]
    session.run(*pytest, ".")


@nox.session(name="utest", python=False)
def utest(session: nox.Session) -> None:
    """Runs all unit tests on the project"""
    pytest = ["pytest", "-m", "not shelly"]
    session.run(*pytest, ".")


@nox.session(name="jupyter", python=False)
def jupyter(session: Session) -> None:
    session.run("jupyter", "lab", f"--notebook-dir=.", "--preferred-dir=.")
