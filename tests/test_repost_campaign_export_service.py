from app.repost_campaign_export_service import (
    build_campaign_post_stats_csv,
    build_campaign_post_stats_txt,
    build_campaign_run_report_csv,
    build_campaign_run_report_txt,
)


def test_build_campaign_run_report_csv_contains_bom_headers_and_values():
    report = {
        "ok": True,
        "run_id": 15,
        "saved_post_id": 26,
        "items": [{"target_title": "A;B", "target_id": "-1001", "send_status": "sent", "delete_status": "deleted", "views": 123, "views_status": "ok", "views_source": "live", "error_text": "line1\nline2", "sent_message_ids": [1, 2]}],
    }
    data = build_campaign_run_report_csv(report)
    assert isinstance(data, bytes)
    assert data.startswith(b"\xef\xbb\xbf")
    text = data.decode("utf-8-sig")
    assert "run_id;saved_post_id;target_title" in text
    assert '"A;B"' in text
    assert "123;ok;live" in text


def test_build_campaign_run_report_txt_contains_summary_and_channels():
    report = {"run_id": 15, "saved_post_id": 26, "views_total": 9815, "views_available": 1, "items": [{"target_title": "Wiki", "views": 1289, "views_status": "ok", "delete_status": "deleted"}]}
    text = build_campaign_run_report_txt(report)
    assert "Запуск: #15" in text
    assert "Пост: #26" in text
    assert "Всего просмотров: 9 815" in text
    assert "Wiki — 1 289 просмотров" in text


def test_build_campaign_post_stats_csv_contains_columns_and_values():
    stats = {"saved_post_id": 26, "channels_stats": [{"target_title": "A", "target_id": "-100", "views_total": 50, "views_status": "ok", "views_source": "final_snapshot", "runs_count": 2, "unavailable_count": 1}]}
    data = build_campaign_post_stats_csv(stats)
    text = data.decode("utf-8-sig")
    assert "saved_post_id;target_title;target_id;views_total" in text
    assert "26;A;-100;50;ok;final_snapshot;2;1;" in text


def test_build_campaign_post_stats_txt_contains_human_text_and_no_data():
    stats = {"saved_post_id": 26, "views_total": 9815, "runs_count": 3, "placements_sent": 4, "channels_stats": [{"target_title": "Channel", "views_status": "unavailable"}]}
    text = build_campaign_post_stats_txt(stats)
    assert "Статистика рекламного поста" in text
    assert "Каналы:" in text
    assert "Channel — нет данных" in text
