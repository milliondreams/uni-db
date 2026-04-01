#!/usr/bin/env python3
"""Prepare deterministic snapshot data for the cyber exposure Locy flagship notebook."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path

SNAPSHOT_DATE = "2026-02-20"
SOURCES = {
    "cisa_kev": "https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
    "nvd": "https://nvd.nist.gov/",
    "epss": "https://www.first.org/epss/",
    "mitre_attack": "https://attack.mitre.org/",
}

ASSETS: list[dict[str, object]] = [
    {
        "asset_id": "OT-ENG-01",
        "asset_name": "Engineering Historian",
        "owner_team": "Manufacturing-OT",
        "site": "fab-a",
        "env": "prod",
        "business_criticality": 5,
        "internet_exposed": 1,
    },
    {
        "asset_id": "OT-MES-01",
        "asset_name": "MES Core",
        "owner_team": "Manufacturing-IT",
        "site": "fab-a",
        "env": "prod",
        "business_criticality": 5,
        "internet_exposed": 0,
    },
    {
        "asset_id": "OT-PLC-07",
        "asset_name": "Line PLC 07",
        "owner_team": "Manufacturing-OT",
        "site": "fab-a",
        "env": "prod",
        "business_criticality": 4,
        "internet_exposed": 0,
    },
    {
        "asset_id": "IT-IAM-01",
        "asset_name": "Identity Gateway",
        "owner_team": "Platform-Security",
        "site": "corp",
        "env": "prod",
        "business_criticality": 5,
        "internet_exposed": 1,
    },
    {
        "asset_id": "IT-VPN-01",
        "asset_name": "Remote Access VPN",
        "owner_team": "Platform-Security",
        "site": "corp",
        "env": "prod",
        "business_criticality": 4,
        "internet_exposed": 1,
    },
    {
        "asset_id": "IT-EDR-01",
        "asset_name": "Endpoint Security Manager",
        "owner_team": "SecOps",
        "site": "corp",
        "env": "prod",
        "business_criticality": 4,
        "internet_exposed": 0,
    },
    {
        "asset_id": "IT-ERP-01",
        "asset_name": "ERP Connector",
        "owner_team": "Business-Apps",
        "site": "corp",
        "env": "prod",
        "business_criticality": 4,
        "internet_exposed": 0,
    },
    {
        "asset_id": "OT-ENG-DR",
        "asset_name": "Engineering DR Node",
        "owner_team": "Manufacturing-OT",
        "site": "fab-b",
        "env": "dr",
        "business_criticality": 3,
        "internet_exposed": 0,
    },
    {
        "asset_id": "IT-BUILD-01",
        "asset_name": "Build Orchestrator",
        "owner_team": "DevInfra",
        "site": "corp",
        "env": "prod",
        "business_criticality": 3,
        "internet_exposed": 1,
    },
    {
        "asset_id": "IT-REPO-01",
        "asset_name": "Artifact Registry",
        "owner_team": "DevInfra",
        "site": "corp",
        "env": "prod",
        "business_criticality": 3,
        "internet_exposed": 0,
    },
    {
        "asset_id": "OT-QA-01",
        "asset_name": "QA Release Workstation",
        "owner_team": "Quality",
        "site": "fab-a",
        "env": "prod",
        "business_criticality": 3,
        "internet_exposed": 0,
    },
    {
        "asset_id": "IT-SIEM-01",
        "asset_name": "Security Data Lake",
        "owner_team": "SecOps",
        "site": "corp",
        "env": "prod",
        "business_criticality": 5,
        "internet_exposed": 0,
    },
]

VULNS: list[dict[str, object]] = [
    {
        "cve_id": "CVE-2024-3400",
        "cwe": "CWE-77",
        "vendor": "Palo Alto",
        "product_family": "Firewall",
        "base_severity": 10.0,
        "attack_surface": "network",
    },
    {
        "cve_id": "CVE-2023-3519",
        "cwe": "CWE-94",
        "vendor": "Citrix",
        "product_family": "Gateway",
        "base_severity": 9.8,
        "attack_surface": "network",
    },
    {
        "cve_id": "CVE-2023-4966",
        "cwe": "CWE-287",
        "vendor": "Citrix",
        "product_family": "NetScaler",
        "base_severity": 9.4,
        "attack_surface": "network",
    },
    {
        "cve_id": "CVE-2021-44228",
        "cwe": "CWE-502",
        "vendor": "Apache",
        "product_family": "Log4j",
        "base_severity": 10.0,
        "attack_surface": "app",
    },
    {
        "cve_id": "CVE-2022-1388",
        "cwe": "CWE-918",
        "vendor": "F5",
        "product_family": "BIG-IP",
        "base_severity": 9.8,
        "attack_surface": "network",
    },
    {
        "cve_id": "CVE-2023-22515",
        "cwe": "CWE-285",
        "vendor": "Atlassian",
        "product_family": "Confluence",
        "base_severity": 9.8,
        "attack_surface": "app",
    },
    {
        "cve_id": "CVE-2024-21762",
        "cwe": "CWE-89",
        "vendor": "Fortinet",
        "product_family": "FortiOS",
        "base_severity": 9.6,
        "attack_surface": "network",
    },
    {
        "cve_id": "CVE-2023-46604",
        "cwe": "CWE-94",
        "vendor": "Apache",
        "product_family": "ActiveMQ",
        "base_severity": 10.0,
        "attack_surface": "app",
    },
]

KEV: list[dict[str, object]] = [
    {"cve_id": "CVE-2024-3400", "kev": 1, "added_date": "2024-04-12"},
    {"cve_id": "CVE-2023-3519", "kev": 1, "added_date": "2023-07-20"},
    {"cve_id": "CVE-2023-4966", "kev": 1, "added_date": "2023-10-16"},
    {"cve_id": "CVE-2021-44228", "kev": 1, "added_date": "2021-12-11"},
    {"cve_id": "CVE-2022-1388", "kev": 1, "added_date": "2022-05-09"},
    {"cve_id": "CVE-2023-22515", "kev": 1, "added_date": "2023-10-04"},
    {"cve_id": "CVE-2024-21762", "kev": 1, "added_date": "2024-02-09"},
    {"cve_id": "CVE-2023-46604", "kev": 1, "added_date": "2023-10-27"},
]

EPSS: list[dict[str, object]] = [
    {"cve_id": "CVE-2024-3400", "epss": 0.98},
    {"cve_id": "CVE-2023-3519", "epss": 0.97},
    {"cve_id": "CVE-2023-4966", "epss": 0.95},
    {"cve_id": "CVE-2021-44228", "epss": 0.93},
    {"cve_id": "CVE-2022-1388", "epss": 0.92},
    {"cve_id": "CVE-2023-22515", "epss": 0.90},
    {"cve_id": "CVE-2024-21762", "epss": 0.89},
    {"cve_id": "CVE-2023-46604", "epss": 0.91},
]

FINDINGS: list[dict[str, object]] = [
    {
        "asset_id": "IT-IAM-01",
        "cve_id": "CVE-2024-3400",
        "scan_ts": "2026-02-15T09:15:00Z",
        "exploit_evidence": 1.0,
        "patch_sla_hours": 24,
    },
    {
        "asset_id": "IT-VPN-01",
        "cve_id": "CVE-2023-3519",
        "scan_ts": "2026-02-15T09:30:00Z",
        "exploit_evidence": 1.0,
        "patch_sla_hours": 24,
    },
    {
        "asset_id": "IT-VPN-01",
        "cve_id": "CVE-2023-4966",
        "scan_ts": "2026-02-15T09:31:00Z",
        "exploit_evidence": 0.8,
        "patch_sla_hours": 24,
    },
    {
        "asset_id": "OT-ENG-01",
        "cve_id": "CVE-2021-44228",
        "scan_ts": "2026-02-16T01:22:00Z",
        "exploit_evidence": 0.7,
        "patch_sla_hours": 36,
    },
    {
        "asset_id": "OT-MES-01",
        "cve_id": "CVE-2023-46604",
        "scan_ts": "2026-02-16T01:40:00Z",
        "exploit_evidence": 0.8,
        "patch_sla_hours": 36,
    },
    {
        "asset_id": "OT-PLC-07",
        "cve_id": "CVE-2022-1388",
        "scan_ts": "2026-02-16T02:10:00Z",
        "exploit_evidence": 0.6,
        "patch_sla_hours": 48,
    },
    {
        "asset_id": "IT-BUILD-01",
        "cve_id": "CVE-2023-22515",
        "scan_ts": "2026-02-16T04:20:00Z",
        "exploit_evidence": 0.6,
        "patch_sla_hours": 72,
    },
    {
        "asset_id": "IT-REPO-01",
        "cve_id": "CVE-2023-46604",
        "scan_ts": "2026-02-16T05:00:00Z",
        "exploit_evidence": 0.5,
        "patch_sla_hours": 72,
    },
    {
        "asset_id": "OT-QA-01",
        "cve_id": "CVE-2021-44228",
        "scan_ts": "2026-02-16T06:12:00Z",
        "exploit_evidence": 0.5,
        "patch_sla_hours": 96,
    },
    {
        "asset_id": "OT-ENG-DR",
        "cve_id": "CVE-2022-1388",
        "scan_ts": "2026-02-16T07:10:00Z",
        "exploit_evidence": 0.4,
        "patch_sla_hours": 96,
    },
    {
        "asset_id": "IT-ERP-01",
        "cve_id": "CVE-2023-22515",
        "scan_ts": "2026-02-16T08:44:00Z",
        "exploit_evidence": 0.4,
        "patch_sla_hours": 72,
    },
    {
        "asset_id": "IT-SIEM-01",
        "cve_id": "CVE-2024-21762",
        "scan_ts": "2026-02-16T10:04:00Z",
        "exploit_evidence": 0.7,
        "patch_sla_hours": 48,
    },
]

DEPENDENCIES: list[dict[str, object]] = [
    {
        "src_asset_id": "IT-IAM-01",
        "dst_asset_id": "OT-ENG-01",
        "propagation_risk": 0.35,
    },
    {
        "src_asset_id": "IT-IAM-01",
        "dst_asset_id": "OT-MES-01",
        "propagation_risk": 0.34,
    },
    {
        "src_asset_id": "IT-VPN-01",
        "dst_asset_id": "OT-ENG-01",
        "propagation_risk": 0.31,
    },
    {
        "src_asset_id": "OT-ENG-01",
        "dst_asset_id": "OT-MES-01",
        "propagation_risk": 0.26,
    },
    {
        "src_asset_id": "OT-MES-01",
        "dst_asset_id": "OT-PLC-07",
        "propagation_risk": 0.24,
    },
    {"src_asset_id": "OT-MES-01", "dst_asset_id": "OT-QA-01", "propagation_risk": 0.19},
    {
        "src_asset_id": "IT-BUILD-01",
        "dst_asset_id": "IT-REPO-01",
        "propagation_risk": 0.21,
    },
    {
        "src_asset_id": "IT-REPO-01",
        "dst_asset_id": "OT-MES-01",
        "propagation_risk": 0.17,
    },
    {
        "src_asset_id": "IT-ERP-01",
        "dst_asset_id": "OT-MES-01",
        "propagation_risk": 0.16,
    },
    {
        "src_asset_id": "IT-SIEM-01",
        "dst_asset_id": "IT-IAM-01",
        "propagation_risk": 0.14,
    },
    {
        "src_asset_id": "OT-ENG-01",
        "dst_asset_id": "OT-ENG-DR",
        "propagation_risk": 0.12,
    },
]

REMEDIATIONS: list[dict[str, object]] = [
    {
        "action_id": "CVE-2024-3400::hotfix",
        "cve_id": "CVE-2024-3400",
        "action_type": "hotfix_patch",
        "cost_index": 8.0,
        "downtime_hours": 2.0,
        "risk_reduction": 0.80,
    },
    {
        "action_id": "CVE-2024-3400::virtual",
        "cve_id": "CVE-2024-3400",
        "action_type": "virtual_patch",
        "cost_index": 3.0,
        "downtime_hours": 0.5,
        "risk_reduction": 0.52,
    },
    {
        "action_id": "CVE-2023-3519::hotfix",
        "cve_id": "CVE-2023-3519",
        "action_type": "hotfix_patch",
        "cost_index": 9.0,
        "downtime_hours": 2.5,
        "risk_reduction": 0.84,
    },
    {
        "action_id": "CVE-2023-3519::acl",
        "cve_id": "CVE-2023-3519",
        "action_type": "acl_lockdown",
        "cost_index": 2.8,
        "downtime_hours": 0.3,
        "risk_reduction": 0.46,
    },
    {
        "action_id": "CVE-2023-4966::hotfix",
        "cve_id": "CVE-2023-4966",
        "action_type": "hotfix_patch",
        "cost_index": 8.5,
        "downtime_hours": 2.5,
        "risk_reduction": 0.80,
    },
    {
        "action_id": "CVE-2023-4966::token",
        "cve_id": "CVE-2023-4966",
        "action_type": "session_key_rotate",
        "cost_index": 3.1,
        "downtime_hours": 0.8,
        "risk_reduction": 0.44,
    },
    {
        "action_id": "CVE-2021-44228::patch",
        "cve_id": "CVE-2021-44228",
        "action_type": "library_upgrade",
        "cost_index": 5.5,
        "downtime_hours": 1.0,
        "risk_reduction": 0.76,
    },
    {
        "action_id": "CVE-2021-44228::waf",
        "cve_id": "CVE-2021-44228",
        "action_type": "waf_rule",
        "cost_index": 2.2,
        "downtime_hours": 0.1,
        "risk_reduction": 0.38,
    },
    {
        "action_id": "CVE-2022-1388::patch",
        "cve_id": "CVE-2022-1388",
        "action_type": "firmware_patch",
        "cost_index": 7.2,
        "downtime_hours": 1.8,
        "risk_reduction": 0.71,
    },
    {
        "action_id": "CVE-2022-1388::isolate",
        "cve_id": "CVE-2022-1388",
        "action_type": "network_isolation",
        "cost_index": 3.4,
        "downtime_hours": 0.6,
        "risk_reduction": 0.49,
    },
    {
        "action_id": "CVE-2023-22515::patch",
        "cve_id": "CVE-2023-22515",
        "action_type": "hotfix_patch",
        "cost_index": 5.9,
        "downtime_hours": 1.2,
        "risk_reduction": 0.67,
    },
    {
        "action_id": "CVE-2023-22515::hardening",
        "cve_id": "CVE-2023-22515",
        "action_type": "config_hardening",
        "cost_index": 1.9,
        "downtime_hours": 0.2,
        "risk_reduction": 0.32,
    },
    {
        "action_id": "CVE-2024-21762::patch",
        "cve_id": "CVE-2024-21762",
        "action_type": "hotfix_patch",
        "cost_index": 7.6,
        "downtime_hours": 1.6,
        "risk_reduction": 0.74,
    },
    {
        "action_id": "CVE-2024-21762::geo",
        "cve_id": "CVE-2024-21762",
        "action_type": "geo_fencing",
        "cost_index": 2.6,
        "downtime_hours": 0.0,
        "risk_reduction": 0.35,
    },
    {
        "action_id": "CVE-2023-46604::patch",
        "cve_id": "CVE-2023-46604",
        "action_type": "broker_patch",
        "cost_index": 6.2,
        "downtime_hours": 1.4,
        "risk_reduction": 0.69,
    },
    {
        "action_id": "CVE-2023-46604::egress",
        "cve_id": "CVE-2023-46604",
        "action_type": "egress_restrict",
        "cost_index": 2.3,
        "downtime_hours": 0.0,
        "risk_reduction": 0.33,
    },
]

KNOWLEDGE_DOCS: list[dict[str, object]] = [
    {
        "doc_id": "DOC-001",
        "doc_type": "kev",
        "title": "CISA KEV alert for PAN-OS auth bypass",
        "content": "CVE-2024-3400 active exploitation observed in perimeter gateways. Prioritize internet exposed identity and VPN assets.",
        "cve_id": "CVE-2024-3400",
        "embedding": [0.95, 0.12, 0.05, 0.88],
    },
    {
        "doc_id": "DOC-002",
        "doc_type": "advisory",
        "title": "Citrix ADC emergency mitigation",
        "content": "CVE-2023-3519 and CVE-2023-4966 enable remote execution or session theft. Apply hotfix and revoke active sessions.",
        "cve_id": "CVE-2023-3519",
        "embedding": [0.91, 0.18, 0.07, 0.82],
    },
    {
        "doc_id": "DOC-003",
        "doc_type": "runbook",
        "title": "Virtual patch for edge gateways",
        "content": "Deploy virtual patch rules for internet-facing gateways before full maintenance window.",
        "cve_id": "CVE-2024-21762",
        "embedding": [0.86, 0.20, 0.09, 0.73],
    },
    {
        "doc_id": "DOC-004",
        "doc_type": "runbook",
        "title": "Log4Shell isolation in OT middleware",
        "content": "CVE-2021-44228 in middleware connected to MES should be isolated and patched with urgency.",
        "cve_id": "CVE-2021-44228",
        "embedding": [0.70, 0.90, 0.15, 0.46],
    },
    {
        "doc_id": "DOC-005",
        "doc_type": "advisory",
        "title": "F5 iControl REST exploitation notes",
        "content": "CVE-2022-1388 is used for pre-auth command execution. Restrict management interfaces and patch quickly.",
        "cve_id": "CVE-2022-1388",
        "embedding": [0.77, 0.52, 0.11, 0.66],
    },
    {
        "doc_id": "DOC-006",
        "doc_type": "advisory",
        "title": "Confluence auth bypass containment",
        "content": "CVE-2023-22515 requires rapid patching and hardening. Restrict external access while patching.",
        "cve_id": "CVE-2023-22515",
        "embedding": [0.55, 0.49, 0.42, 0.57],
    },
    {
        "doc_id": "DOC-007",
        "doc_type": "advisory",
        "title": "ActiveMQ deserialization exploit response",
        "content": "CVE-2023-46604 exploited in broker systems. Restrict egress and patch broker services.",
        "cve_id": "CVE-2023-46604",
        "embedding": [0.64, 0.84, 0.21, 0.62],
    },
    {
        "doc_id": "DOC-008",
        "doc_type": "policy",
        "title": "Patch prioritization policy for KEV assets",
        "content": "KEV-listed findings on critical assets must be remediated within one patch cycle.",
        "cve_id": "",
        "embedding": [0.88, 0.10, 0.52, 0.79],
    },
]

NOTEBOOK_CASES: list[dict[str, object]] = [
    {"asset_id": "IT-IAM-01", "reason": "internet_exposed + kev + very_high_epss"},
    {"asset_id": "IT-VPN-01", "reason": "dual_kev_findings_on_edge_asset"},
    {"asset_id": "OT-ENG-01", "reason": "critical_ot_asset_with_log4j"},
    {"asset_id": "OT-MES-01", "reason": "mes_core_with_activemq_rce"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_cyber_exposure_twin"),
        help="Directory for generated notebook data files.",
    )
    return parser.parse_args()


def _format_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.8f}".rstrip("0").rstrip(".")
    if isinstance(value, list):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _write_csv(
    path: Path, fieldnames: list[str], rows: list[dict[str, object]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {name: _format_value(row.get(name, "")) for name in fieldnames}
            )


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        output_dir / "assets.csv",
        [
            "asset_id",
            "asset_name",
            "owner_team",
            "site",
            "env",
            "business_criticality",
            "internet_exposed",
        ],
        ASSETS,
    )
    _write_csv(
        output_dir / "vulnerabilities.csv",
        [
            "cve_id",
            "cwe",
            "vendor",
            "product_family",
            "base_severity",
            "attack_surface",
        ],
        VULNS,
    )
    _write_csv(output_dir / "kev_snapshot.csv", ["cve_id", "kev", "added_date"], KEV)
    _write_csv(output_dir / "epss_snapshot.csv", ["cve_id", "epss"], EPSS)
    _write_csv(
        output_dir / "vuln_findings.csv",
        ["asset_id", "cve_id", "scan_ts", "exploit_evidence", "patch_sla_hours"],
        FINDINGS,
    )
    _write_csv(
        output_dir / "asset_dependencies.csv",
        ["src_asset_id", "dst_asset_id", "propagation_risk"],
        DEPENDENCIES,
    )
    _write_csv(
        output_dir / "remediation_actions.csv",
        [
            "action_id",
            "cve_id",
            "action_type",
            "cost_index",
            "downtime_hours",
            "risk_reduction",
        ],
        REMEDIATIONS,
    )
    _write_csv(
        output_dir / "knowledge_docs.csv",
        ["doc_id", "doc_type", "title", "content", "cve_id", "embedding"],
        KNOWLEDGE_DOCS,
    )
    _write_csv(
        output_dir / "notebook_cases.csv", ["asset_id", "reason"], NOTEBOOK_CASES
    )

    manifest = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "snapshot_date": SNAPSHOT_DATE,
        "source": {
            "description": "Curated deterministic snapshot inspired by KEV, EPSS, NVD, and ATT&CK references for docs/CI reproducibility.",
            "urls": SOURCES,
            "license_note": "Derived educational sample for documentation demos.",
        },
        "shape": {
            "assets": len(ASSETS),
            "vulnerabilities": len(VULNS),
            "findings": len(FINDINGS),
            "dependencies": len(DEPENDENCIES),
            "remediation_actions": len(REMEDIATIONS),
            "knowledge_docs": len(KNOWLEDGE_DOCS),
            "notebook_cases": len(NOTEBOOK_CASES),
        },
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    print(f"wrote {output_dir / 'assets.csv'} ({len(ASSETS)} rows)")
    print(f"wrote {output_dir / 'vulnerabilities.csv'} ({len(VULNS)} rows)")
    print(f"wrote {output_dir / 'kev_snapshot.csv'} ({len(KEV)} rows)")
    print(f"wrote {output_dir / 'epss_snapshot.csv'} ({len(EPSS)} rows)")
    print(f"wrote {output_dir / 'vuln_findings.csv'} ({len(FINDINGS)} rows)")
    print(f"wrote {output_dir / 'asset_dependencies.csv'} ({len(DEPENDENCIES)} rows)")
    print(f"wrote {output_dir / 'remediation_actions.csv'} ({len(REMEDIATIONS)} rows)")
    print(f"wrote {output_dir / 'knowledge_docs.csv'} ({len(KNOWLEDGE_DOCS)} rows)")
    print(f"wrote {output_dir / 'notebook_cases.csv'} ({len(NOTEBOOK_CASES)} rows)")
    print(f"wrote {output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
