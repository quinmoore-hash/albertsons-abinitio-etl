"""Tests for the failure notification hook ported from run/notify.ksh."""

from __future__ import annotations

import notify


def test_email_failure_does_not_suppress_slack(monkeypatch, capsys):
    sent = []

    def boom(body):
        raise OSError("smtp down")

    monkeypatch.setattr(notify, "send_email", boom)
    monkeypatch.setattr(notify, "send_slack", lambda body: sent.append(body))

    notify.alert_store_data_ops({"task_instance": None})

    assert len(sent) == 1
    assert "boom failed: smtp down" in capsys.readouterr().out


def test_demo_mode_logs_both_transports(monkeypatch, capsys):
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    notify.alert_store_data_ops(None)

    out = capsys.readouterr().out
    assert "would email store-data-ops@albertsons.com" in out
    assert "would post to #store-data-ops" in out
