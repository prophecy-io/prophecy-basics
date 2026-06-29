"""Convert dbt run_results.json into a self-contained HTML report."""

import json
import sys
from datetime import datetime
from pathlib import Path

def status_badge(status: str) -> str:
    colors = {
        "pass": ("#15803d", "#dcfce7"),
        "success": ("#15803d", "#dcfce7"),
        "fail": ("#b91c1c", "#fee2e2"),
        "error": ("#b91c1c", "#fee2e2"),
        "warn": ("#a16207", "#fef9c3"),
        "skip": ("#6b7280", "#f3f4f6"),
    }
    fg, bg = colors.get(status.lower(), ("#374151", "#f3f4f6"))
    return (
        f'<span style="background:{bg};color:{fg};padding:3px 10px;'
        f'border-radius:12px;font-weight:600;font-size:0.85em;">'
        f"{status.upper()}</span>"
    )


def friendly_name(unique_id: str) -> str:
    """test.prophecy_basics.snowflake_sql.FindDuplicates.test_find_unique_records -> test_find_unique_records"""
    return unique_id.rsplit(".", 1)[-1] if "." in unique_id else unique_id


def build_html(data: dict, suite_name: str) -> str:
    meta = data.get("metadata", {})
    dbt_version = meta.get("dbt_version", "N/A")
    generated_at = meta.get("generated_at", "N/A")

    try:
        dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        generated_at = dt.strftime("%B %d, %Y  %H:%M:%S UTC")
    except Exception:
        pass

    results = data.get("results", [])
    total = len(results)
    passed = sum(1 for r in results if r.get("status") in ("pass", "success"))
    failed = total - passed

    if failed:
        summary_color, summary_text = "#b91c1c", f"{failed} of {total} failed"
    else:
        summary_color, summary_text = "#15803d", f"All {total} passed"

    rows = []
    for i, r in enumerate(results, 1):
        uid = r.get("unique_id", "")
        name = friendly_name(uid)
        status = r.get("status", "unknown")
        exec_time = r.get("execution_time", 0)
        message = r.get("message", "") or ""
        failures = r.get("failures", 0) or 0

        rows.append(
            f"<tr>"
            f'<td style="text-align:center;">{i}</td>'
            f"<td><strong>{name}</strong>"
            f'<div style="color:#6b7280;font-size:0.8em;margin-top:2px;">{uid}</div></td>'
            f"<td>{status_badge(status)}</td>"
            f"<td>{exec_time:.2f}s</td>"
            f'<td style="font-size:0.85em;">{message}</td>'
            f"</tr>"
        )

    table_rows = "\n".join(rows) if rows else '<tr><td colspan="5">No test results found.</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{suite_name} - dbt Test Report</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background:#f8fafc; color:#1e293b; padding:24px; }}
  .container {{ max-width:960px; margin:0 auto; }}
  .header {{ background:linear-gradient(135deg,#1e293b,#334155); color:#fff;
             padding:28px 32px; border-radius:12px; margin-bottom:24px; }}
  .header h1 {{ font-size:1.5em; margin-bottom:8px; }}
  .meta {{ display:flex; gap:24px; flex-wrap:wrap; font-size:0.9em; opacity:0.85; }}
  .summary {{ display:flex; gap:16px; margin-bottom:20px; }}
  .card {{ flex:1; background:#fff; border-radius:10px; padding:18px 22px;
           box-shadow:0 1px 3px rgba(0,0,0,.08); text-align:center; }}
  .card .num {{ font-size:1.8em; font-weight:700; }}
  .card .label {{ font-size:0.85em; color:#64748b; margin-top:4px; }}
  table {{ width:100%; border-collapse:collapse; background:#fff;
           border-radius:10px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,.08); }}
  th {{ background:#f1f5f9; text-align:left; padding:12px 14px; font-size:0.85em;
       color:#475569; text-transform:uppercase; letter-spacing:0.5px; }}
  td {{ padding:12px 14px; border-top:1px solid #f1f5f9; vertical-align:top; }}
  tr:hover td {{ background:#f8fafc; }}
  .footer {{ text-align:center; margin-top:24px; font-size:0.8em; color:#94a3b8; }}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>{suite_name} &mdash; Test Report</h1>
    <div class="meta">
      <span>dbt {dbt_version}</span>
      <span>{generated_at}</span>
    </div>
  </div>

  <div class="summary">
    <div class="card">
      <div class="num">{total}</div>
      <div class="label">Total Tests</div>
    </div>
    <div class="card">
      <div class="num" style="color:#15803d;">{passed}</div>
      <div class="label">Passed</div>
    </div>
    <div class="card">
      <div class="num" style="color:#b91c1c;">{failed}</div>
      <div class="label">Failed</div>
    </div>
    <div class="card">
      <div class="num" style="color:{summary_color};">{summary_text}</div>
      <div class="label">Result</div>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th style="width:40px;">#</th>
        <th>Test Case</th>
        <th style="width:90px;">Status</th>
        <th style="width:80px;">Time</th>
        <th>Message</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>

  <div class="footer">Generated by dbt_report.py</div>
</div>
</body>
</html>"""


def main():
    if len(sys.argv) < 3:
        print("Usage: dbt_report.py <run_results.json> <output.html> [suite_name]")
        sys.exit(1)

    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])
    suite = sys.argv[3] if len(sys.argv) > 3 else "dbt"

    if not src.exists():
        print(f"Warning: {src} not found — writing placeholder report.")
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(
            f"<html><body><h2>{suite} Test Report</h2>"
            f"<p>No run_results.json found — tests may not have executed.</p></body></html>"
        )
        return

    with open(src) as f:
        data = json.load(f)

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(build_html(data, suite))
    print(f"HTML report written to {dst}")


if __name__ == "__main__":
    main()
