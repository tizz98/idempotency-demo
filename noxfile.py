import nox_poetry as nox


@nox.session
def serve(session: nox.Session) -> None:
    session.install(".")
    session.run("uvicorn", "mars.main:app", "--reload")


@nox.session
def black(session: nox.Session) -> None:
    session.install("black")
    session.run("black", "mars")


@nox.session
def isort(session: nox.Session) -> None:
    session.install("isort")
    session.run("isort", "mars")


@nox.session(name="black-check")
def black_check(session: nox.Session) -> None:
    session.install("black")
    session.run("black", "--check", "mars")


@nox.session(name="isort-check")
def isort_check(session: nox.Session) -> None:
    session.install("isort")
    session.run("isort", "--check-only", "mars")


@nox.session(name="flake8")
def flake8(session: nox.Session) -> None:
    session.install("flake8")
    session.run("flake8", "mars")
