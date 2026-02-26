from __future__ import annotations

import asyncio
import inspect
import sys
import types

import pytest


def _install_plotly_stub() -> None:
    class _Bar(dict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

    class _Figure:
        def __init__(self) -> None:
            self.data: list[dict] = []
            self.layout: dict[str, object] = {}

        def add_trace(self, trace: dict) -> None:
            self.data.append(trace)

        def update_layout(self, **kwargs) -> None:
            self.layout.update(kwargs)

    graph_objects = types.SimpleNamespace(Figure=_Figure, Bar=_Bar)
    plotly_pkg = types.SimpleNamespace(graph_objects=graph_objects)
    sys.modules.setdefault("plotly", plotly_pkg)
    sys.modules.setdefault("plotly.graph_objects", graph_objects)


try:
    import plotly.graph_objects  # type: ignore  # noqa: F401
except Exception:
    _install_plotly_stub()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "asyncio: run test function in asyncio event loop")


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    test_func = pyfuncitem.obj
    if not inspect.iscoroutinefunction(test_func):
        return None

    kwargs = {name: pyfuncitem.funcargs[name] for name in pyfuncitem._fixtureinfo.argnames}
    asyncio.run(test_func(**kwargs))
    return True
