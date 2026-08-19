from types import SimpleNamespace

from arxiv_downloader.daemon import main as daemon_main


def test_daemon_debug_option_enables_debug_logging(monkeypatch):
    settings = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=8765))
    app = object()
    captured = {}

    monkeypatch.setattr(daemon_main, "load_settings", lambda: settings)

    def fake_create_app(received_settings, *, debug):
        captured["settings"] = received_settings
        captured["debug"] = debug
        return app

    def fake_run(received_app, **kwargs):
        captured["app"] = received_app
        captured["uvicorn"] = kwargs

    monkeypatch.setattr(daemon_main, "create_app", fake_create_app)
    monkeypatch.setattr(daemon_main.uvicorn, "run", fake_run)

    daemon_main.main(["--debug"])

    assert captured["settings"] is settings
    assert captured["debug"] is True
    assert captured["app"] is app
    assert captured["uvicorn"]["log_level"] == "debug"


def test_daemon_defaults_to_info_logging(monkeypatch):
    settings = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=8765))
    captured = {}

    monkeypatch.setattr(daemon_main, "load_settings", lambda: settings)
    monkeypatch.setattr(
        daemon_main,
        "create_app",
        lambda received_settings, *, debug: captured.setdefault("debug", debug) or object(),
    )
    monkeypatch.setattr(
        daemon_main.uvicorn,
        "run",
        lambda received_app, **kwargs: captured.setdefault("uvicorn", kwargs),
    )

    daemon_main.main([])

    assert captured["debug"] is False
    assert captured["uvicorn"]["log_level"] == "info"
