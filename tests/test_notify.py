import json

from spark.common import notify


def test_demo_fallbacks(monkeypatch, capsys):
    monkeypatch.setattr(notify.shutil, "which", lambda _: None)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    notify.notify_failure("ops@example.com", "#ops", detail="dq_check failed")
    out = capsys.readouterr().out
    assert "(demo) would email ops@example.com: [Albertsons ETL] nightly_batch task failure" in out
    assert "(demo) would post to #ops" in out


def test_slack_payload(monkeypatch):
    sent = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout):
        sent["url"] = req.full_url
        sent["body"] = json.loads(req.data)
        return _Resp()

    monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
    assert notify.post_slack("#ops", "hello", webhook_url="https://hooks.example/x")
    assert sent == {"url": "https://hooks.example/x", "body": {"channel": "#ops", "text": "hello"}}


def test_cli_usage():
    assert notify.main([]) == 1
