from pathlib import Path

from src.nepse_trading_dashboard.scraper import BrokerDashboardScraper, DashboardCapture, SectionCapture


def test_export_capture_writes_json_and_csv(tmp_path: Path) -> None:
    scraper = BrokerDashboardScraper(
        email="a",
        password="b",
        output_dir=tmp_path,
        headless=True,
    )
    capture = DashboardCapture(
        run_id="20260330T120000Z",
        started_at="2026-03-30T12:00:00Z",
        sections=[
            SectionCapture(
                name="Broker Summary",
                slug="broker-summary",
                url="https://nepsetrading.com/dashboard/broker-analysis/broker-summary",
                scraped_at="2026-03-30T12:01:00Z",
                tables=[{"headers": ["Broker", "Volume"], "rows": [{"Broker": "44", "Volume": "100000"}]}],
                cards=[{"label": "Total", "value": "100000"}],
            )
        ],
    )

    scraper._export_capture(capture)

    assert (tmp_path / "broker_dashboard_20260330T120000Z.json").exists()
    assert (tmp_path / "broker-summary_table_1_20260330T120000Z.csv").exists()
