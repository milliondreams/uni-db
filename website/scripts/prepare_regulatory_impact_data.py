#!/usr/bin/env python3
"""Prepare deterministic snapshot data for the regulatory change-impact Locy flagship notebook."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path

SNAPSHOT_DATE = "2026-03-01"
SOURCES = {
    "nist_800_53": "https://csrc.nist.gov/publications/detail/sp/800-53/rev-5/final",
    "dora_regulation": "https://eur-lex.europa.eu/eli/reg/2022/2554",
    "gdpr": "https://gdpr.eu",
    "sox": "https://www.sec.gov/spotlight/sarbanes-oxley.htm",
}

# ---------------------------------------------------------------------------
# Node tables
# ---------------------------------------------------------------------------

REGULATIONS: list[dict[str, object]] = [
    {"reg_id": "REG-GDPR", "name": "GDPR", "jurisdiction": "EU", "effective_date": "2018-05-25", "penalty_factor": 0.85},
    {"reg_id": "REG-SOX", "name": "SOX", "jurisdiction": "US", "effective_date": "2002-07-30", "penalty_factor": 0.70},
    {"reg_id": "REG-DORA", "name": "DORA", "jurisdiction": "EU", "effective_date": "2025-01-17", "penalty_factor": 0.90},
    {"reg_id": "REG-HIPAA", "name": "HIPAA", "jurisdiction": "US", "effective_date": "1996-08-21", "penalty_factor": 0.65},
    {"reg_id": "REG-PCI", "name": "PCI-DSS", "jurisdiction": "Global", "effective_date": "2004-12-15", "penalty_factor": 0.75},
]

OBLIGATIONS: list[dict[str, object]] = [
    # DORA obligations (8)
    {"obl_id": "OBL-DORA-01", "reg_id": "REG-DORA", "text": "ICT risk management framework", "category": "risk_management", "severity": "critical", "weight": 1.0, "embedding": [0.91, 0.30, -0.15, 0.82]},
    {"obl_id": "OBL-DORA-02", "reg_id": "REG-DORA", "text": "ICT incident reporting", "category": "reporting", "severity": "critical", "weight": 0.95, "embedding": [0.45, 0.88, -0.10, 0.30]},
    {"obl_id": "OBL-DORA-03", "reg_id": "REG-DORA", "text": "Digital operational resilience testing", "category": "testing", "severity": "critical", "weight": 0.95, "embedding": [0.85, 0.15, 0.70, -0.20]},
    {"obl_id": "OBL-DORA-04", "reg_id": "REG-DORA", "text": "ICT third-party risk management", "category": "third_party", "severity": "high", "weight": 0.90, "embedding": [0.30, 0.20, -0.50, 0.75]},
    {"obl_id": "OBL-DORA-05", "reg_id": "REG-DORA", "text": "Information sharing", "category": "reporting", "severity": "medium", "weight": 0.70, "embedding": [0.40, 0.55, -0.30, 0.20]},
    {"obl_id": "OBL-DORA-06", "reg_id": "REG-DORA", "text": "Business continuity", "category": "risk_management", "severity": "critical", "weight": 1.0, "embedding": [0.75, 0.10, 0.25, 0.65]},
    {"obl_id": "OBL-DORA-07", "reg_id": "REG-DORA", "text": "Audit trail requirements", "category": "governance", "severity": "high", "weight": 0.85, "embedding": [0.20, 0.90, 0.10, 0.15]},
    {"obl_id": "OBL-DORA-08", "reg_id": "REG-DORA", "text": "Threat-led penetration testing", "category": "testing", "severity": "high", "weight": 0.90, "embedding": [0.80, 0.10, 0.75, -0.25]},
    # GDPR obligations (4)
    {"obl_id": "OBL-GDPR-01", "reg_id": "REG-GDPR", "text": "Data protection impact assessment", "category": "governance", "severity": "high", "weight": 0.85, "embedding": [0.55, 0.30, -0.40, 0.60]},
    {"obl_id": "OBL-GDPR-02", "reg_id": "REG-GDPR", "text": "Right to erasure", "category": "data_protection", "severity": "high", "weight": 0.80, "embedding": [0.10, 0.20, -0.60, 0.45]},
    {"obl_id": "OBL-GDPR-03", "reg_id": "REG-GDPR", "text": "Breach notification within 72h", "category": "reporting", "severity": "critical", "weight": 0.95, "embedding": [0.50, 0.85, -0.15, 0.25]},
    {"obl_id": "OBL-GDPR-04", "reg_id": "REG-GDPR", "text": "Data protection officer appointment", "category": "governance", "severity": "medium", "weight": 0.70, "embedding": [0.35, 0.40, -0.20, 0.50]},
    # SOX obligations (3)
    {"obl_id": "OBL-SOX-01", "reg_id": "REG-SOX", "text": "Internal controls over financial reporting", "category": "governance", "severity": "critical", "weight": 1.0, "embedding": [0.60, 0.50, 0.10, 0.40]},
    {"obl_id": "OBL-SOX-02", "reg_id": "REG-SOX", "text": "CEO/CFO certification", "category": "governance", "severity": "high", "weight": 0.85, "embedding": [0.50, 0.45, 0.05, 0.35]},
    {"obl_id": "OBL-SOX-03", "reg_id": "REG-SOX", "text": "Audit committee independence", "category": "governance", "severity": "high", "weight": 0.80, "embedding": [0.25, 0.70, 0.15, 0.20]},
    # HIPAA obligations (3)
    {"obl_id": "OBL-HIPAA-01", "reg_id": "REG-HIPAA", "text": "PHI encryption at rest", "category": "data_protection", "severity": "critical", "weight": 0.95, "embedding": [0.15, 0.10, -0.70, 0.80]},
    {"obl_id": "OBL-HIPAA-02", "reg_id": "REG-HIPAA", "text": "Access control and audit logs", "category": "access_control", "severity": "high", "weight": 0.85, "embedding": [0.25, 0.85, 0.05, 0.20]},
    {"obl_id": "OBL-HIPAA-03", "reg_id": "REG-HIPAA", "text": "Business associate agreements", "category": "third_party", "severity": "high", "weight": 0.80, "embedding": [0.30, 0.25, -0.45, 0.70]},
    # PCI-DSS obligations (2)
    {"obl_id": "OBL-PCI-01", "reg_id": "REG-PCI", "text": "Cardholder data encryption", "category": "data_protection", "severity": "critical", "weight": 1.0, "embedding": [0.10, 0.15, -0.75, 0.85]},
    {"obl_id": "OBL-PCI-02", "reg_id": "REG-PCI", "text": "Network segmentation and monitoring", "category": "access_control", "severity": "high", "weight": 0.85, "embedding": [0.70, 0.25, 0.30, 0.10]},
]

CONTROLS: list[dict[str, object]] = [
    # Implemented (effectiveness 0.7-0.95)
    {"ctrl_id": "CTRL-01", "name": "Firewall rules", "nist_family": "SC", "status": "implemented", "effectiveness": 0.90, "embedding": [0.72, 0.20, 0.35, 0.08]},
    {"ctrl_id": "CTRL-02", "name": "Encryption at rest", "nist_family": "SC", "status": "implemented", "effectiveness": 0.92, "embedding": [0.12, 0.12, -0.72, 0.83]},
    {"ctrl_id": "CTRL-03", "name": "Multi-factor authentication", "nist_family": "IA", "status": "implemented", "effectiveness": 0.88, "embedding": [0.28, 0.18, -0.48, 0.72]},
    {"ctrl_id": "CTRL-04", "name": "SIEM monitoring", "nist_family": "AU", "status": "implemented", "effectiveness": 0.85, "embedding": [0.22, 0.88, 0.08, 0.18]},
    {"ctrl_id": "CTRL-05", "name": "Backup and recovery", "nist_family": "CP", "status": "implemented", "effectiveness": 0.87, "embedding": [0.73, 0.12, 0.22, 0.62]},
    {"ctrl_id": "CTRL-06", "name": "Network segmentation", "nist_family": "SC", "status": "implemented", "effectiveness": 0.58, "embedding": [0.68, 0.22, 0.32, 0.12]},
    {"ctrl_id": "CTRL-07", "name": "Vulnerability scanning", "nist_family": "RA", "status": "implemented", "effectiveness": 0.52, "embedding": [0.82, 0.18, 0.68, -0.18]},
    # Partial (effectiveness 0.3-0.6)
    {"ctrl_id": "CTRL-08", "name": "Incident response plan", "nist_family": "IR", "status": "partial", "effectiveness": 0.55, "embedding": [0.48, 0.85, -0.12, 0.28]},
    {"ctrl_id": "CTRL-09", "name": "Third-party risk assessment", "nist_family": "SA", "status": "partial", "effectiveness": 0.45, "embedding": [0.32, 0.22, -0.48, 0.72]},
    {"ctrl_id": "CTRL-10", "name": "Penetration testing", "nist_family": "CA", "status": "partial", "effectiveness": 0.50, "embedding": [0.78, 0.12, 0.72, -0.22]},
    {"ctrl_id": "CTRL-11", "name": "Data classification", "nist_family": "RA", "status": "partial", "effectiveness": 0.40, "embedding": [0.52, 0.28, -0.38, 0.58]},
    {"ctrl_id": "CTRL-12", "name": "Privacy impact assessment", "nist_family": "PL", "status": "partial", "effectiveness": 0.42, "embedding": [0.53, 0.32, -0.42, 0.58]},
    {"ctrl_id": "CTRL-13", "name": "Change management", "nist_family": "CM", "status": "partial", "effectiveness": 0.65, "embedding": [0.58, 0.48, 0.08, 0.38]},
    {"ctrl_id": "CTRL-14", "name": "Access review process", "nist_family": "AC", "status": "partial", "effectiveness": 0.48, "embedding": [0.22, 0.82, 0.12, 0.22]},
    {"ctrl_id": "CTRL-15", "name": "Data erasure procedures", "nist_family": "PM", "status": "partial", "effectiveness": 0.35, "embedding": [0.12, 0.22, -0.58, 0.48]},
    {"ctrl_id": "CTRL-16", "name": "DPO governance framework", "nist_family": "PM", "status": "partial", "effectiveness": 0.50, "embedding": [0.38, 0.42, -0.22, 0.52]},
    {"ctrl_id": "CTRL-17", "name": "Financial reporting controls", "nist_family": "PL", "status": "partial", "effectiveness": 0.68, "embedding": [0.58, 0.48, 0.12, 0.38]},
    {"ctrl_id": "CTRL-18", "name": "Audit committee procedures", "nist_family": "AU", "status": "partial", "effectiveness": 0.55, "embedding": [0.28, 0.68, 0.18, 0.22]},
    # Gap (effectiveness 0.0-0.2)
    {"ctrl_id": "CTRL-19", "name": "ICT resilience testing", "nist_family": "CA", "status": "gap", "effectiveness": 0.10, "embedding": [0.83, 0.13, 0.68, -0.18]},
    {"ctrl_id": "CTRL-20", "name": "Threat-led penetration testing (TLPT)", "nist_family": "CA", "status": "gap", "effectiveness": 0.05, "embedding": [0.78, 0.08, 0.73, -0.23]},
    {"ctrl_id": "CTRL-21", "name": "ICT supply chain mapping", "nist_family": "SA", "status": "gap", "effectiveness": 0.08, "embedding": [0.28, 0.18, -0.52, 0.73]},
    {"ctrl_id": "CTRL-22", "name": "Digital operational resilience dashboard", "nist_family": "PM", "status": "gap", "effectiveness": 0.05, "embedding": [0.88, 0.28, -0.12, 0.80]},
    {"ctrl_id": "CTRL-23", "name": "Automated compliance reporting", "nist_family": "AU", "status": "gap", "effectiveness": 0.12, "embedding": [0.42, 0.52, -0.28, 0.18]},
    {"ctrl_id": "CTRL-24", "name": "Executive certification workflow", "nist_family": "PL", "status": "gap", "effectiveness": 0.15, "embedding": [0.48, 0.42, 0.08, 0.32]},
    {"ctrl_id": "CTRL-25", "name": "Cardholder data environment isolation", "nist_family": "SC", "status": "gap", "effectiveness": 0.18, "embedding": [0.08, 0.12, -0.72, 0.82]},
]

PROCESSES: list[dict[str, object]] = [
    {"proc_id": "PROC-01", "name": "Payment processing", "department": "operations", "criticality": 0.98},
    {"proc_id": "PROC-02", "name": "Customer onboarding (KYC)", "department": "compliance", "criticality": 0.90},
    {"proc_id": "PROC-03", "name": "Trade execution", "department": "trading", "criticality": 0.95},
    {"proc_id": "PROC-04", "name": "Risk reporting", "department": "risk", "criticality": 0.85},
    {"proc_id": "PROC-05", "name": "Regulatory filing", "department": "compliance", "criticality": 0.88},
    {"proc_id": "PROC-06", "name": "IT change management", "department": "IT", "criticality": 0.75},
    {"proc_id": "PROC-07", "name": "Vendor management", "department": "legal", "criticality": 0.72},
    {"proc_id": "PROC-08", "name": "Data analytics", "department": "risk", "criticality": 0.70},
    {"proc_id": "PROC-09", "name": "Customer service", "department": "operations", "criticality": 0.65},
    {"proc_id": "PROC-10", "name": "Fraud detection", "department": "risk", "criticality": 0.92},
]

SYSTEMS: list[dict[str, object]] = [
    {"sys_id": "SYS-01", "name": "ERP Core", "env": "prod", "tier": 1},
    {"sys_id": "SYS-02", "name": "Trading Platform", "env": "prod", "tier": 1},
    {"sys_id": "SYS-03", "name": "CRM System", "env": "prod", "tier": 2},
    {"sys_id": "SYS-04", "name": "Data Warehouse", "env": "prod", "tier": 2},
    {"sys_id": "SYS-05", "name": "Email Gateway", "env": "prod", "tier": 3},
    {"sys_id": "SYS-06", "name": "HR Portal", "env": "prod", "tier": 3},
    {"sys_id": "SYS-07", "name": "Dev/Test Environment", "env": "non-prod", "tier": 4},
    {"sys_id": "SYS-08", "name": "Disaster Recovery", "env": "dr", "tier": 2},
]

VENDORS: list[dict[str, object]] = [
    {"vendor_id": "VND-01", "name": "CloudOps Inc", "soc2": 1, "risk_rating": 0.25},
    {"vendor_id": "VND-02", "name": "DataVault Solutions", "soc2": 1, "risk_rating": 0.15},
    {"vendor_id": "VND-03", "name": "NetSecure Systems", "soc2": 1, "risk_rating": 0.20},
    {"vendor_id": "VND-04", "name": "QuickDeploy Ltd", "soc2": 0, "risk_rating": 0.55},
    {"vendor_id": "VND-05", "name": "LegacyTech Corp", "soc2": 0, "risk_rating": 0.65},
    {"vendor_id": "VND-06", "name": "OffshoreIT Services", "soc2": 0, "risk_rating": 0.70},
]

CONTRACTS: list[dict[str, object]] = [
    {"contract_id": "CTR-01", "vendor_id": "VND-01", "renewal_date": "2026-11-15", "annual_value": 1800000.0},
    {"contract_id": "CTR-02", "vendor_id": "VND-02", "renewal_date": "2027-03-01", "annual_value": 950000.0},
    {"contract_id": "CTR-03", "vendor_id": "VND-03", "renewal_date": "2026-08-20", "annual_value": 420000.0},
    {"contract_id": "CTR-04", "vendor_id": "VND-04", "renewal_date": "2026-06-30", "annual_value": 150000.0},
    {"contract_id": "CTR-05", "vendor_id": "VND-05", "renewal_date": "2027-01-15", "annual_value": 280000.0},
    {"contract_id": "CTR-06", "vendor_id": "VND-06", "renewal_date": "2026-09-10", "annual_value": 75000.0},
]

# ---------------------------------------------------------------------------
# Edge tables
# ---------------------------------------------------------------------------

REQUIRES: list[dict[str, object]] = [
    # DORA
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-01", "priority": "critical"},
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-02", "priority": "critical"},
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-03", "priority": "critical"},
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-04", "priority": "high"},
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-05", "priority": "medium"},
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-06", "priority": "critical"},
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-07", "priority": "high"},
    {"reg_id": "REG-DORA", "obl_id": "OBL-DORA-08", "priority": "high"},
    # GDPR
    {"reg_id": "REG-GDPR", "obl_id": "OBL-GDPR-01", "priority": "high"},
    {"reg_id": "REG-GDPR", "obl_id": "OBL-GDPR-02", "priority": "high"},
    {"reg_id": "REG-GDPR", "obl_id": "OBL-GDPR-03", "priority": "critical"},
    {"reg_id": "REG-GDPR", "obl_id": "OBL-GDPR-04", "priority": "medium"},
    # SOX
    {"reg_id": "REG-SOX", "obl_id": "OBL-SOX-01", "priority": "critical"},
    {"reg_id": "REG-SOX", "obl_id": "OBL-SOX-02", "priority": "high"},
    {"reg_id": "REG-SOX", "obl_id": "OBL-SOX-03", "priority": "high"},
    # HIPAA
    {"reg_id": "REG-HIPAA", "obl_id": "OBL-HIPAA-01", "priority": "critical"},
    {"reg_id": "REG-HIPAA", "obl_id": "OBL-HIPAA-02", "priority": "high"},
    {"reg_id": "REG-HIPAA", "obl_id": "OBL-HIPAA-03", "priority": "high"},
    # PCI-DSS
    {"reg_id": "REG-PCI", "obl_id": "OBL-PCI-01", "priority": "critical"},
    {"reg_id": "REG-PCI", "obl_id": "OBL-PCI-02", "priority": "high"},
]

SATISFIED_BY: list[dict[str, object]] = [
    # DORA-01 ICT risk mgmt framework → partial/gap controls
    {"obl_id": "OBL-DORA-01", "ctrl_id": "CTRL-22", "coverage": 0.10},  # gap: resilience dashboard
    {"obl_id": "OBL-DORA-01", "ctrl_id": "CTRL-07", "coverage": 0.35},  # implemented: vuln scanning
    # DORA-02 ICT incident reporting → partial incident response
    {"obl_id": "OBL-DORA-02", "ctrl_id": "CTRL-08", "coverage": 0.40},  # partial: incident response
    {"obl_id": "OBL-DORA-02", "ctrl_id": "CTRL-04", "coverage": 0.50},  # implemented: SIEM
    # DORA-03 resilience testing → gap controls
    {"obl_id": "OBL-DORA-03", "ctrl_id": "CTRL-19", "coverage": 0.10},  # gap: ICT resilience testing
    {"obl_id": "OBL-DORA-03", "ctrl_id": "CTRL-10", "coverage": 0.30},  # partial: pen testing
    # DORA-04 third-party risk → gap/partial
    {"obl_id": "OBL-DORA-04", "ctrl_id": "CTRL-09", "coverage": 0.35},  # partial: third-party assessment
    {"obl_id": "OBL-DORA-04", "ctrl_id": "CTRL-21", "coverage": 0.08},  # gap: supply chain mapping
    # DORA-05 info sharing → partial
    {"obl_id": "OBL-DORA-05", "ctrl_id": "CTRL-23", "coverage": 0.15},  # gap: automated reporting
    # DORA-06 business continuity → mixed
    {"obl_id": "OBL-DORA-06", "ctrl_id": "CTRL-05", "coverage": 0.70},  # implemented: backup/recovery
    {"obl_id": "OBL-DORA-06", "ctrl_id": "CTRL-22", "coverage": 0.10},  # gap: resilience dashboard
    # DORA-07 audit trail → implemented + partial
    {"obl_id": "OBL-DORA-07", "ctrl_id": "CTRL-04", "coverage": 0.65},  # implemented: SIEM
    {"obl_id": "OBL-DORA-07", "ctrl_id": "CTRL-14", "coverage": 0.40},  # partial: access review
    # DORA-08 TLPT → gap
    {"obl_id": "OBL-DORA-08", "ctrl_id": "CTRL-20", "coverage": 0.05},  # gap: TLPT
    {"obl_id": "OBL-DORA-08", "ctrl_id": "CTRL-10", "coverage": 0.25},  # partial: pen testing
    # GDPR-01 DPIA → partial
    {"obl_id": "OBL-GDPR-01", "ctrl_id": "CTRL-12", "coverage": 0.45},  # partial: privacy impact
    {"obl_id": "OBL-GDPR-01", "ctrl_id": "CTRL-11", "coverage": 0.30},  # partial: data classification
    # GDPR-02 right to erasure → partial
    {"obl_id": "OBL-GDPR-02", "ctrl_id": "CTRL-15", "coverage": 0.35},  # partial: data erasure
    # GDPR-03 breach notification → partial incident response
    {"obl_id": "OBL-GDPR-03", "ctrl_id": "CTRL-08", "coverage": 0.40},  # partial: incident response
    {"obl_id": "OBL-GDPR-03", "ctrl_id": "CTRL-04", "coverage": 0.55},  # implemented: SIEM
    # GDPR-04 DPO → partial
    {"obl_id": "OBL-GDPR-04", "ctrl_id": "CTRL-16", "coverage": 0.50},  # partial: DPO framework
    # SOX-01 internal controls → partial/gap
    {"obl_id": "OBL-SOX-01", "ctrl_id": "CTRL-17", "coverage": 0.55},  # partial: financial reporting
    {"obl_id": "OBL-SOX-01", "ctrl_id": "CTRL-13", "coverage": 0.40},  # partial: change mgmt
    # SOX-02 CEO/CFO cert → gap
    {"obl_id": "OBL-SOX-02", "ctrl_id": "CTRL-24", "coverage": 0.15},  # gap: exec certification
    {"obl_id": "OBL-SOX-02", "ctrl_id": "CTRL-17", "coverage": 0.45},  # partial: financial reporting
    # SOX-03 audit committee → partial
    {"obl_id": "OBL-SOX-03", "ctrl_id": "CTRL-18", "coverage": 0.50},  # partial: audit procedures
    # HIPAA-01 PHI encryption → implemented
    {"obl_id": "OBL-HIPAA-01", "ctrl_id": "CTRL-02", "coverage": 0.85},  # implemented: encryption
    # HIPAA-02 access control + audit → implemented
    {"obl_id": "OBL-HIPAA-02", "ctrl_id": "CTRL-03", "coverage": 0.70},  # implemented: MFA
    {"obl_id": "OBL-HIPAA-02", "ctrl_id": "CTRL-04", "coverage": 0.60},  # implemented: SIEM
    # HIPAA-03 BAA → partial
    {"obl_id": "OBL-HIPAA-03", "ctrl_id": "CTRL-09", "coverage": 0.40},  # partial: third-party
    # PCI-01 cardholder encryption → implemented + gap
    {"obl_id": "OBL-PCI-01", "ctrl_id": "CTRL-02", "coverage": 0.80},  # implemented: encryption
    {"obl_id": "OBL-PCI-01", "ctrl_id": "CTRL-25", "coverage": 0.15},  # gap: CDE isolation
    # PCI-02 network segmentation → implemented
    {"obl_id": "OBL-PCI-02", "ctrl_id": "CTRL-06", "coverage": 0.75},  # implemented: segmentation
    {"obl_id": "OBL-PCI-02", "ctrl_id": "CTRL-01", "coverage": 0.65},  # implemented: firewall
]

PROTECTS: list[dict[str, object]] = [
    # Firewall
    {"ctrl_id": "CTRL-01", "proc_id": "PROC-01", "relevance": 0.90},  # payment processing
    {"ctrl_id": "CTRL-01", "proc_id": "PROC-03", "relevance": 0.85},  # trade execution
    {"ctrl_id": "CTRL-01", "proc_id": "PROC-10", "relevance": 0.80},  # fraud detection
    # Encryption at rest
    {"ctrl_id": "CTRL-02", "proc_id": "PROC-01", "relevance": 0.92},  # payment processing
    {"ctrl_id": "CTRL-02", "proc_id": "PROC-08", "relevance": 0.75},  # data analytics
    {"ctrl_id": "CTRL-02", "proc_id": "PROC-02", "relevance": 0.80},  # KYC
    # MFA
    {"ctrl_id": "CTRL-03", "proc_id": "PROC-02", "relevance": 0.85},  # KYC
    {"ctrl_id": "CTRL-03", "proc_id": "PROC-03", "relevance": 0.88},  # trade execution
    {"ctrl_id": "CTRL-03", "proc_id": "PROC-05", "relevance": 0.75},  # regulatory filing
    # SIEM
    {"ctrl_id": "CTRL-04", "proc_id": "PROC-04", "relevance": 0.85},  # risk reporting
    {"ctrl_id": "CTRL-04", "proc_id": "PROC-10", "relevance": 0.90},  # fraud detection
    {"ctrl_id": "CTRL-04", "proc_id": "PROC-01", "relevance": 0.70},  # payment processing
    # Backup/recovery
    {"ctrl_id": "CTRL-05", "proc_id": "PROC-01", "relevance": 0.88},  # payment processing
    {"ctrl_id": "CTRL-05", "proc_id": "PROC-03", "relevance": 0.85},  # trade execution
    # Network segmentation
    {"ctrl_id": "CTRL-06", "proc_id": "PROC-01", "relevance": 0.88},  # payment processing
    {"ctrl_id": "CTRL-06", "proc_id": "PROC-10", "relevance": 0.82},  # fraud detection
    # Vulnerability scanning
    {"ctrl_id": "CTRL-07", "proc_id": "PROC-06", "relevance": 0.70},  # IT change mgmt
    {"ctrl_id": "CTRL-07", "proc_id": "PROC-03", "relevance": 0.65},  # trade execution
    # Incident response
    {"ctrl_id": "CTRL-08", "proc_id": "PROC-04", "relevance": 0.80},  # risk reporting
    {"ctrl_id": "CTRL-08", "proc_id": "PROC-09", "relevance": 0.60},  # customer service
    # Third-party risk
    {"ctrl_id": "CTRL-09", "proc_id": "PROC-07", "relevance": 0.85},  # vendor mgmt
    {"ctrl_id": "CTRL-09", "proc_id": "PROC-02", "relevance": 0.55},  # KYC
    # Pen testing
    {"ctrl_id": "CTRL-10", "proc_id": "PROC-06", "relevance": 0.72},  # IT change mgmt
    {"ctrl_id": "CTRL-10", "proc_id": "PROC-03", "relevance": 0.68},  # trade execution
    # Change management
    {"ctrl_id": "CTRL-13", "proc_id": "PROC-06", "relevance": 0.90},  # IT change mgmt
    {"ctrl_id": "CTRL-13", "proc_id": "PROC-05", "relevance": 0.65},  # regulatory filing
    # ICT resilience testing (gap) - still maps to processes for impact tracing
    {"ctrl_id": "CTRL-19", "proc_id": "PROC-01", "relevance": 0.85},  # payment processing
    {"ctrl_id": "CTRL-19", "proc_id": "PROC-03", "relevance": 0.80},  # trade execution
    # TLPT (gap)
    {"ctrl_id": "CTRL-20", "proc_id": "PROC-03", "relevance": 0.78},  # trade execution
    {"ctrl_id": "CTRL-20", "proc_id": "PROC-10", "relevance": 0.75},  # fraud detection
]

RUNS_ON: list[dict[str, object]] = [
    {"proc_id": "PROC-01", "sys_id": "SYS-01", "dependency": 0.95},  # payment → ERP Core
    {"proc_id": "PROC-01", "sys_id": "SYS-08", "dependency": 0.40},  # payment → DR
    {"proc_id": "PROC-02", "sys_id": "SYS-03", "dependency": 0.85},  # KYC → CRM
    {"proc_id": "PROC-02", "sys_id": "SYS-01", "dependency": 0.50},  # KYC → ERP Core
    {"proc_id": "PROC-03", "sys_id": "SYS-02", "dependency": 0.90},  # trade → Trading Platform
    {"proc_id": "PROC-03", "sys_id": "SYS-01", "dependency": 0.55},  # trade → ERP Core
    {"proc_id": "PROC-04", "sys_id": "SYS-04", "dependency": 0.80},  # risk reporting → DW
    {"proc_id": "PROC-04", "sys_id": "SYS-01", "dependency": 0.45},  # risk reporting → ERP Core
    {"proc_id": "PROC-05", "sys_id": "SYS-01", "dependency": 0.70},  # regulatory filing → ERP
    {"proc_id": "PROC-06", "sys_id": "SYS-07", "dependency": 0.60},  # IT change → Dev/Test
    {"proc_id": "PROC-07", "sys_id": "SYS-03", "dependency": 0.55},  # vendor mgmt → CRM
    {"proc_id": "PROC-08", "sys_id": "SYS-04", "dependency": 0.85},  # analytics → DW
    {"proc_id": "PROC-09", "sys_id": "SYS-03", "dependency": 0.75},  # customer service → CRM
    {"proc_id": "PROC-10", "sys_id": "SYS-02", "dependency": 0.80},  # fraud → Trading Platform
    {"proc_id": "PROC-10", "sys_id": "SYS-04", "dependency": 0.65},  # fraud → DW
]

OPERATED_BY: list[dict[str, object]] = [
    {"sys_id": "SYS-01", "vendor_id": "VND-01", "criticality": 0.90},  # ERP Core → CloudOps
    {"sys_id": "SYS-02", "vendor_id": "VND-02", "criticality": 0.85},  # Trading → DataVault
    {"sys_id": "SYS-03", "vendor_id": "VND-03", "criticality": 0.70},  # CRM → NetSecure
    {"sys_id": "SYS-04", "vendor_id": "VND-02", "criticality": 0.75},  # DW → DataVault
    {"sys_id": "SYS-05", "vendor_id": "VND-03", "criticality": 0.40},  # Email → NetSecure
    {"sys_id": "SYS-06", "vendor_id": "VND-05", "criticality": 0.60},  # HR Portal → LegacyTech
    {"sys_id": "SYS-07", "vendor_id": "VND-04", "criticality": 0.30},  # Dev/Test → QuickDeploy
    {"sys_id": "SYS-08", "vendor_id": "VND-06", "criticality": 0.65},  # DR → OffshoreIT
]

GOVERNED_BY: list[dict[str, object]] = [
    {"vendor_id": "VND-01", "contract_id": "CTR-01"},
    {"vendor_id": "VND-02", "contract_id": "CTR-02"},
    {"vendor_id": "VND-03", "contract_id": "CTR-03"},
    {"vendor_id": "VND-04", "contract_id": "CTR-04"},
    {"vendor_id": "VND-05", "contract_id": "CTR-05"},
    {"vendor_id": "VND-06", "contract_id": "CTR-06"},
]

NOTEBOOK_CASES: list[dict[str, object]] = [
    {"sys_id": "SYS-01", "reason": "ERP Core: multi-regulation exposure via DORA+SOX+PCI obligation chains"},
    {"sys_id": "SYS-02", "reason": "Trading Platform: DORA resilience gaps propagate through trade execution and fraud detection"},
    {"sys_id": "SYS-03", "reason": "CRM System: GDPR data protection obligations with partial control coverage"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_regulatory_impact"),
        help="Directory for generated notebook data files.",
    )
    return parser.parse_args()


def _format_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.8f}".rstrip("0").rstrip(".")
    if isinstance(value, list):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: _format_value(row.get(name, "")) for name in fieldnames})


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Node CSVs
    _write_csv(
        output_dir / "regulations.csv",
        ["reg_id", "name", "jurisdiction", "effective_date", "penalty_factor"],
        REGULATIONS,
    )
    _write_csv(
        output_dir / "obligations.csv",
        ["obl_id", "reg_id", "text", "category", "severity", "weight", "embedding"],
        OBLIGATIONS,
    )
    _write_csv(
        output_dir / "controls.csv",
        ["ctrl_id", "name", "nist_family", "status", "effectiveness", "embedding"],
        CONTROLS,
    )
    _write_csv(
        output_dir / "processes.csv",
        ["proc_id", "name", "department", "criticality"],
        PROCESSES,
    )
    _write_csv(
        output_dir / "systems.csv",
        ["sys_id", "name", "env", "tier"],
        SYSTEMS,
    )
    _write_csv(
        output_dir / "vendors.csv",
        ["vendor_id", "name", "soc2", "risk_rating"],
        VENDORS,
    )
    _write_csv(
        output_dir / "contracts.csv",
        ["contract_id", "vendor_id", "renewal_date", "annual_value"],
        CONTRACTS,
    )

    # Edge CSVs
    _write_csv(
        output_dir / "requires.csv",
        ["reg_id", "obl_id", "priority"],
        REQUIRES,
    )
    _write_csv(
        output_dir / "satisfied_by.csv",
        ["obl_id", "ctrl_id", "coverage"],
        SATISFIED_BY,
    )
    _write_csv(
        output_dir / "protects.csv",
        ["ctrl_id", "proc_id", "relevance"],
        PROTECTS,
    )
    _write_csv(
        output_dir / "runs_on.csv",
        ["proc_id", "sys_id", "dependency"],
        RUNS_ON,
    )
    _write_csv(
        output_dir / "operated_by.csv",
        ["sys_id", "vendor_id", "criticality"],
        OPERATED_BY,
    )
    _write_csv(
        output_dir / "governed_by.csv",
        ["vendor_id", "contract_id"],
        GOVERNED_BY,
    )
    _write_csv(
        output_dir / "notebook_cases.csv",
        ["sys_id", "reason"],
        NOTEBOOK_CASES,
    )

    manifest = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "snapshot_date": SNAPSHOT_DATE,
        "source": {
            "description": "Synthetic regulatory compliance data for change impact analysis demo. Modeled on financial services GRC requirements across GDPR, SOX, DORA, HIPAA, and PCI-DSS.",
            "urls": SOURCES,
            "license_note": "All data is synthetic and generated for demonstration purposes only. Not compliance advice.",
        },
        "shape": {
            "regulations": len(REGULATIONS),
            "obligations": len(OBLIGATIONS),
            "controls": len(CONTROLS),
            "processes": len(PROCESSES),
            "systems": len(SYSTEMS),
            "vendors": len(VENDORS),
            "contracts": len(CONTRACTS),
            "requires": len(REQUIRES),
            "satisfied_by": len(SATISFIED_BY),
            "protects": len(PROTECTS),
            "runs_on": len(RUNS_ON),
            "operated_by": len(OPERATED_BY),
            "governed_by": len(GOVERNED_BY),
            "notebook_cases": len(NOTEBOOK_CASES),
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {output_dir / 'regulations.csv'} ({len(REGULATIONS)} rows)")
    print(f"wrote {output_dir / 'obligations.csv'} ({len(OBLIGATIONS)} rows)")
    print(f"wrote {output_dir / 'controls.csv'} ({len(CONTROLS)} rows)")
    print(f"wrote {output_dir / 'processes.csv'} ({len(PROCESSES)} rows)")
    print(f"wrote {output_dir / 'systems.csv'} ({len(SYSTEMS)} rows)")
    print(f"wrote {output_dir / 'vendors.csv'} ({len(VENDORS)} rows)")
    print(f"wrote {output_dir / 'contracts.csv'} ({len(CONTRACTS)} rows)")
    print(f"wrote {output_dir / 'requires.csv'} ({len(REQUIRES)} rows)")
    print(f"wrote {output_dir / 'satisfied_by.csv'} ({len(SATISFIED_BY)} rows)")
    print(f"wrote {output_dir / 'protects.csv'} ({len(PROTECTS)} rows)")
    print(f"wrote {output_dir / 'runs_on.csv'} ({len(RUNS_ON)} rows)")
    print(f"wrote {output_dir / 'operated_by.csv'} ({len(OPERATED_BY)} rows)")
    print(f"wrote {output_dir / 'governed_by.csv'} ({len(GOVERNED_BY)} rows)")
    print(f"wrote {output_dir / 'notebook_cases.csv'} ({len(NOTEBOOK_CASES)} rows)")
    print(f"wrote {output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
