#!/usr/bin/env python3
"""Prepare deterministic snapshot data for the drug repurposing Locy flagship notebook."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path

SNAPSHOT_DATE = "2026-03-01"
SOURCES = {
    "hetionet": "https://het.io",
    "drugbank": "https://go.drugbank.com",
    "string_db": "https://string-db.org",
    "disgenet": "https://www.disgenet.org",
    "opentargets": "https://platform.opentargets.org",
}

# ---------------------------------------------------------------------------
# Node tables
# ---------------------------------------------------------------------------

DRUGS: list[dict[str, object]] = [
    # --- Focus cohort (4 drugs with known repurposing stories) ---
    {"drug_id": "DRUG_001", "name": "Metformin", "drug_class": "biguanide", "approval_status": "approved", "fingerprint": [0.82, -0.15, 0.34, 0.61]},
    {"drug_id": "DRUG_002", "name": "Thalidomide", "drug_class": "immunomodulator", "approval_status": "withdrawn_reintroduced", "fingerprint": [-0.45, 0.78, 0.22, -0.31]},
    {"drug_id": "DRUG_003", "name": "Sildenafil", "drug_class": "PDE5_inhibitor", "approval_status": "approved", "fingerprint": [0.11, 0.63, -0.52, 0.74]},
    {"drug_id": "DRUG_004", "name": "Baricitinib", "drug_class": "JAK_inhibitor", "approval_status": "approved", "fingerprint": [-0.33, 0.49, 0.71, -0.18]},
    # --- Extended set (26 more drugs) ---
    {"drug_id": "DRUG_005", "name": "Aspirin", "drug_class": "NSAID", "approval_status": "approved", "fingerprint": [0.72, -0.21, 0.44, 0.53]},
    {"drug_id": "DRUG_006", "name": "Ibuprofen", "drug_class": "NSAID", "approval_status": "approved", "fingerprint": [0.69, -0.24, 0.41, 0.50]},
    {"drug_id": "DRUG_007", "name": "Rapamycin", "drug_class": "mTOR_inhibitor", "approval_status": "approved", "fingerprint": [0.58, 0.42, -0.31, 0.27]},
    {"drug_id": "DRUG_008", "name": "Minoxidil", "drug_class": "vasodilator", "approval_status": "approved", "fingerprint": [0.15, -0.62, 0.38, 0.81]},
    {"drug_id": "DRUG_009", "name": "Celecoxib", "drug_class": "COX2_inhibitor", "approval_status": "approved", "fingerprint": [0.66, -0.18, 0.49, 0.47]},
    {"drug_id": "DRUG_010", "name": "Tamoxifen", "drug_class": "SERM", "approval_status": "approved", "fingerprint": [-0.22, 0.57, 0.33, -0.44]},
    {"drug_id": "DRUG_011", "name": "Propranolol", "drug_class": "beta_blocker", "approval_status": "approved", "fingerprint": [0.28, -0.53, 0.17, 0.69]},
    {"drug_id": "DRUG_012", "name": "Valproic acid", "drug_class": "anticonvulsant", "approval_status": "approved", "fingerprint": [0.41, 0.13, -0.66, 0.22]},
    {"drug_id": "DRUG_013", "name": "Disulfiram", "drug_class": "immunomodulator", "approval_status": "approved", "fingerprint": [-0.38, 0.72, 0.28, -0.25]},
    {"drug_id": "DRUG_014", "name": "Niclosamide", "drug_class": "anthelmintic", "approval_status": "approved", "fingerprint": [-0.51, 0.34, 0.62, -0.17]},
    {"drug_id": "DRUG_015", "name": "Chloroquine", "drug_class": "antimalarial", "approval_status": "approved", "fingerprint": [-0.29, 0.65, 0.18, -0.42]},
    {"drug_id": "DRUG_016", "name": "Hydroxychloroquine", "drug_class": "antimalarial", "approval_status": "approved", "fingerprint": [-0.26, 0.62, 0.21, -0.39]},
    {"drug_id": "DRUG_017", "name": "Doxycycline", "drug_class": "tetracycline", "approval_status": "approved", "fingerprint": [0.37, -0.44, 0.59, 0.14]},
    {"drug_id": "DRUG_018", "name": "Minocycline", "drug_class": "tetracycline", "approval_status": "approved", "fingerprint": [0.34, -0.47, 0.56, 0.11]},
    {"drug_id": "DRUG_019", "name": "Losartan", "drug_class": "ARB", "approval_status": "approved", "fingerprint": [0.22, -0.58, 0.43, 0.76]},
    {"drug_id": "DRUG_020", "name": "Telmisartan", "drug_class": "ARB", "approval_status": "approved", "fingerprint": [0.19, -0.55, 0.46, 0.73]},
    {"drug_id": "DRUG_021", "name": "Pioglitazone", "drug_class": "thiazolidinedione", "approval_status": "approved", "fingerprint": [0.75, -0.11, 0.29, 0.64]},
    {"drug_id": "DRUG_022", "name": "Riluzole", "drug_class": "glutamate_modulator", "approval_status": "approved", "fingerprint": [0.33, 0.21, -0.58, 0.45]},
    {"drug_id": "DRUG_023", "name": "Memantine", "drug_class": "glutamate_modulator", "approval_status": "approved", "fingerprint": [0.36, 0.18, -0.55, 0.42]},
    {"drug_id": "DRUG_024", "name": "Donepezil", "drug_class": "cholinesterase_inhibitor", "approval_status": "approved", "fingerprint": [0.48, 0.31, -0.42, 0.56]},
    {"drug_id": "DRUG_025", "name": "Galantamine", "drug_class": "cholinesterase_inhibitor", "approval_status": "approved", "fingerprint": [0.45, 0.28, -0.45, 0.53]},
    {"drug_id": "DRUG_026", "name": "Rivastigmine", "drug_class": "cholinesterase_inhibitor", "approval_status": "approved", "fingerprint": [0.42, 0.34, -0.39, 0.59]},
    {"drug_id": "DRUG_027", "name": "Levetiracetam", "drug_class": "anticonvulsant", "approval_status": "approved", "fingerprint": [0.38, 0.16, -0.63, 0.25]},
    {"drug_id": "DRUG_028", "name": "Topiramate", "drug_class": "anticonvulsant", "approval_status": "approved", "fingerprint": [0.44, 0.10, -0.69, 0.19]},
    {"drug_id": "DRUG_029", "name": "Gabapentin", "drug_class": "calcium_channel", "approval_status": "approved", "fingerprint": [0.31, 0.24, -0.48, 0.37]},
    {"drug_id": "DRUG_030", "name": "Pregabalin", "drug_class": "calcium_channel", "approval_status": "approved", "fingerprint": [0.28, 0.27, -0.51, 0.34]},
]

PROTEINS: list[dict[str, object]] = [
    {"protein_id": "PROT_001", "name": "AMP-activated protein kinase", "gene_symbol": "AMPK", "family": "kinase"},
    {"protein_id": "PROT_002", "name": "Mechanistic target of rapamycin", "gene_symbol": "mTOR", "family": "kinase"},
    {"protein_id": "PROT_003", "name": "Epidermal growth factor receptor", "gene_symbol": "EGFR", "family": "kinase"},
    {"protein_id": "PROT_004", "name": "Janus kinase 1", "gene_symbol": "JAK1", "family": "kinase"},
    {"protein_id": "PROT_005", "name": "Janus kinase 2", "gene_symbol": "JAK2", "family": "kinase"},
    {"protein_id": "PROT_006", "name": "Janus kinase 3", "gene_symbol": "JAK3", "family": "kinase"},
    {"protein_id": "PROT_007", "name": "Phosphodiesterase 5A", "gene_symbol": "PDE5A", "family": "phosphodiesterase"},
    {"protein_id": "PROT_008", "name": "Cereblon", "gene_symbol": "CRBN", "family": "E3_ligase"},
    {"protein_id": "PROT_009", "name": "Ikaros family zinc finger 1", "gene_symbol": "IKZF1", "family": "transcription_factor"},
    {"protein_id": "PROT_010", "name": "Ikaros family zinc finger 3", "gene_symbol": "IKZF3", "family": "transcription_factor"},
    {"protein_id": "PROT_011", "name": "AP2-associated kinase 1", "gene_symbol": "AAK1", "family": "kinase"},
    {"protein_id": "PROT_012", "name": "Angiotensin-converting enzyme 2", "gene_symbol": "ACE2", "family": "enzyme"},
    {"protein_id": "PROT_013", "name": "Interleukin 6", "gene_symbol": "IL6", "family": "receptor"},
    {"protein_id": "PROT_014", "name": "Tumor necrosis factor", "gene_symbol": "TNF", "family": "receptor"},
    {"protein_id": "PROT_015", "name": "Vascular endothelial growth factor receptor 2", "gene_symbol": "VEGFR2", "family": "kinase"},
    {"protein_id": "PROT_016", "name": "Cyclooxygenase 1", "gene_symbol": "COX1", "family": "enzyme"},
    {"protein_id": "PROT_017", "name": "Cyclooxygenase 2", "gene_symbol": "COX2", "family": "enzyme"},
    {"protein_id": "PROT_018", "name": "B-Raf proto-oncogene", "gene_symbol": "BRAF", "family": "kinase"},
    {"protein_id": "PROT_019", "name": "Phosphoinositide 3-kinase", "gene_symbol": "PI3K", "family": "kinase"},
    {"protein_id": "PROT_020", "name": "Protein kinase B", "gene_symbol": "AKT", "family": "kinase"},
    {"protein_id": "PROT_021", "name": "Mitogen-activated protein kinase 1", "gene_symbol": "MAPK1", "family": "kinase"},
    {"protein_id": "PROT_022", "name": "Signal transducer and activator of transcription 3", "gene_symbol": "STAT3", "family": "transcription_factor"},
    {"protein_id": "PROT_023", "name": "Nuclear factor kappa-B", "gene_symbol": "NFKB1", "family": "transcription_factor"},
    {"protein_id": "PROT_024", "name": "Peroxisome proliferator-activated receptor gamma", "gene_symbol": "PPARG", "family": "receptor"},
    {"protein_id": "PROT_025", "name": "N-methyl-D-aspartate receptor subunit 1", "gene_symbol": "GRIN1", "family": "ion_channel"},
    {"protein_id": "PROT_026", "name": "Acetylcholinesterase", "gene_symbol": "ACHE", "family": "enzyme"},
    {"protein_id": "PROT_027", "name": "Butyrylcholinesterase", "gene_symbol": "BCHE", "family": "enzyme"},
    {"protein_id": "PROT_028", "name": "Gamma-aminobutyric acid receptor subunit alpha-1", "gene_symbol": "GABRA1", "family": "ion_channel"},
    {"protein_id": "PROT_029", "name": "Voltage-gated calcium channel subunit alpha-2/delta-1", "gene_symbol": "CACNA2D1", "family": "ion_channel"},
    {"protein_id": "PROT_030", "name": "Estrogen receptor alpha", "gene_symbol": "ESR1", "family": "receptor"},
    {"protein_id": "PROT_031", "name": "Angiotensin II receptor type 1", "gene_symbol": "AGTR1", "family": "receptor"},
    {"protein_id": "PROT_032", "name": "Beta-2 adrenergic receptor", "gene_symbol": "ADRB2", "family": "receptor"},
    {"protein_id": "PROT_033", "name": "Histone deacetylase 1", "gene_symbol": "HDAC1", "family": "enzyme"},
    {"protein_id": "PROT_034", "name": "Matrix metalloproteinase 9", "gene_symbol": "MMP9", "family": "protease"},
    {"protein_id": "PROT_035", "name": "Caspase 3", "gene_symbol": "CASP3", "family": "protease"},
    {"protein_id": "PROT_036", "name": "Hypoxia-inducible factor 1-alpha", "gene_symbol": "HIF1A", "family": "transcription_factor"},
    {"protein_id": "PROT_037", "name": "Sirtuin 1", "gene_symbol": "SIRT1", "family": "enzyme"},
    {"protein_id": "PROT_038", "name": "Glycogen synthase kinase 3 beta", "gene_symbol": "GSK3B", "family": "kinase"},
    {"protein_id": "PROT_039", "name": "Tumor protein p53", "gene_symbol": "TP53", "family": "transcription_factor"},
    {"protein_id": "PROT_040", "name": "BCL2 apoptosis regulator", "gene_symbol": "BCL2", "family": "enzyme"},
    {"protein_id": "PROT_041", "name": "Interleukin 1 beta", "gene_symbol": "IL1B", "family": "receptor"},
    {"protein_id": "PROT_042", "name": "Transforming growth factor beta 1", "gene_symbol": "TGFB1", "family": "receptor"},
    {"protein_id": "PROT_043", "name": "Solute carrier family 6 member 3", "gene_symbol": "SLC6A3", "family": "transporter"},
    {"protein_id": "PROT_044", "name": "Solute carrier family 6 member 4", "gene_symbol": "SLC6A4", "family": "transporter"},
    {"protein_id": "PROT_045", "name": "Transmembrane serine protease 2", "gene_symbol": "TMPRSS2", "family": "protease"},
    {"protein_id": "PROT_046", "name": "Cathepsin L", "gene_symbol": "CTSL", "family": "protease"},
    {"protein_id": "PROT_047", "name": "Dihydrofolate reductase", "gene_symbol": "DHFR", "family": "enzyme"},
    {"protein_id": "PROT_048", "name": "Aldehyde dehydrogenase 2", "gene_symbol": "ALDH2", "family": "enzyme"},
    {"protein_id": "PROT_049", "name": "Wnt signaling pathway component DVL1", "gene_symbol": "DVL1", "family": "enzyme"},
    {"protein_id": "PROT_050", "name": "Cyclin-dependent kinase 4", "gene_symbol": "CDK4", "family": "kinase"},
]

DISEASES: list[dict[str, object]] = [
    {"disease_id": "DIS_001", "name": "Type 2 diabetes", "therapeutic_area": "metabolic"},
    {"disease_id": "DIS_002", "name": "Rheumatoid arthritis", "therapeutic_area": "autoimmune"},
    {"disease_id": "DIS_003", "name": "Erectile dysfunction", "therapeutic_area": "sexual_health"},
    {"disease_id": "DIS_004", "name": "COVID-19", "therapeutic_area": "infectious"},
    {"disease_id": "DIS_005", "name": "Multiple myeloma", "therapeutic_area": "oncology"},
    {"disease_id": "DIS_006", "name": "Alzheimer disease", "therapeutic_area": "neurodegenerative"},
    {"disease_id": "DIS_007", "name": "Parkinson disease", "therapeutic_area": "neurodegenerative"},
    {"disease_id": "DIS_008", "name": "Breast cancer", "therapeutic_area": "oncology"},
    {"disease_id": "DIS_009", "name": "Lung cancer", "therapeutic_area": "oncology"},
    {"disease_id": "DIS_010", "name": "Colorectal cancer", "therapeutic_area": "oncology"},
    {"disease_id": "DIS_011", "name": "Glioblastoma", "therapeutic_area": "oncology"},
    {"disease_id": "DIS_012", "name": "Hypertension", "therapeutic_area": "cardiovascular"},
    {"disease_id": "DIS_013", "name": "Pulmonary arterial hypertension", "therapeutic_area": "pulmonary"},
    {"disease_id": "DIS_014", "name": "Epilepsy", "therapeutic_area": "neurological"},
    {"disease_id": "DIS_015", "name": "Neuropathic pain", "therapeutic_area": "neurological"},
    {"disease_id": "DIS_016", "name": "Depression", "therapeutic_area": "psychiatric"},
    {"disease_id": "DIS_017", "name": "Psoriasis", "therapeutic_area": "autoimmune"},
    {"disease_id": "DIS_018", "name": "Crohn disease", "therapeutic_area": "gastrointestinal"},
    {"disease_id": "DIS_019", "name": "Ulcerative colitis", "therapeutic_area": "gastrointestinal"},
    {"disease_id": "DIS_020", "name": "Idiopathic pulmonary fibrosis", "therapeutic_area": "pulmonary"},
]

SIDE_EFFECTS: list[dict[str, object]] = [
    {"se_id": "SE_001", "name": "Lactic acidosis", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_002", "name": "Teratogenicity", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_003", "name": "Hypotension", "severity": "moderate", "severity_weight": 0.5},
    {"se_id": "SE_004", "name": "Immunosuppression", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_005", "name": "Nausea", "severity": "mild", "severity_weight": 0.2},
    {"se_id": "SE_006", "name": "Hepatotoxicity", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_007", "name": "QT prolongation", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_008", "name": "Nephrotoxicity", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_009", "name": "Neutropenia", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_010", "name": "Peripheral neuropathy", "severity": "moderate", "severity_weight": 0.5},
    {"se_id": "SE_011", "name": "Drowsiness", "severity": "mild", "severity_weight": 0.2},
    {"se_id": "SE_012", "name": "Weight gain", "severity": "moderate", "severity_weight": 0.5},
    {"se_id": "SE_013", "name": "GI bleeding", "severity": "severe", "severity_weight": 0.9},
    {"se_id": "SE_014", "name": "Photosensitivity", "severity": "mild", "severity_weight": 0.2},
    {"se_id": "SE_015", "name": "Rash", "severity": "mild", "severity_weight": 0.2},
]

# ---------------------------------------------------------------------------
# Edge tables
# ---------------------------------------------------------------------------

BINDS: list[dict[str, object]] = [
    # --- Metformin (DRUG_001) bindings ---
    {"drug_id": "DRUG_001", "protein_id": "PROT_001", "affinity_nm": 18.0, "confidence": 0.94},      # Metformin → AMPK (primary)
    {"drug_id": "DRUG_001", "protein_id": "PROT_002", "affinity_nm": 420.0, "confidence": 0.72},     # Metformin → mTOR (indirect)
    {"drug_id": "DRUG_001", "protein_id": "PROT_037", "affinity_nm": 310.0, "confidence": 0.65},     # Metformin → SIRT1
    # --- Thalidomide (DRUG_002) bindings ---
    {"drug_id": "DRUG_002", "protein_id": "PROT_008", "affinity_nm": 3.5, "confidence": 0.97},       # Thalidomide → CRBN (primary)
    {"drug_id": "DRUG_002", "protein_id": "PROT_014", "affinity_nm": 85.0, "confidence": 0.78},      # Thalidomide → TNF
    {"drug_id": "DRUG_002", "protein_id": "PROT_015", "affinity_nm": 220.0, "confidence": 0.61},     # Thalidomide → VEGFR2
    # --- Sildenafil (DRUG_003) bindings ---
    {"drug_id": "DRUG_003", "protein_id": "PROT_007", "affinity_nm": 3.7, "confidence": 0.98},       # Sildenafil → PDE5A (primary)
    {"drug_id": "DRUG_003", "protein_id": "PROT_015", "affinity_nm": 480.0, "confidence": 0.55},     # Sildenafil → VEGFR2
    # --- Baricitinib (DRUG_004) bindings ---
    {"drug_id": "DRUG_004", "protein_id": "PROT_004", "affinity_nm": 5.9, "confidence": 0.96},       # Baricitinib → JAK1 (primary)
    {"drug_id": "DRUG_004", "protein_id": "PROT_005", "affinity_nm": 5.7, "confidence": 0.96},       # Baricitinib → JAK2
    {"drug_id": "DRUG_004", "protein_id": "PROT_006", "affinity_nm": 560.0, "confidence": 0.52},     # Baricitinib → JAK3
    {"drug_id": "DRUG_004", "protein_id": "PROT_011", "affinity_nm": 17.0, "confidence": 0.88},      # Baricitinib → AAK1 (key repurposing target)
    # --- Aspirin / Ibuprofen → COX ---
    {"drug_id": "DRUG_005", "protein_id": "PROT_016", "affinity_nm": 4.5, "confidence": 0.95},       # Aspirin → COX1
    {"drug_id": "DRUG_005", "protein_id": "PROT_017", "affinity_nm": 50.0, "confidence": 0.82},      # Aspirin → COX2
    {"drug_id": "DRUG_005", "protein_id": "PROT_023", "affinity_nm": 850.0, "confidence": 0.45},     # Aspirin → NFKB1
    {"drug_id": "DRUG_006", "protein_id": "PROT_016", "affinity_nm": 12.0, "confidence": 0.93},      # Ibuprofen → COX1
    {"drug_id": "DRUG_006", "protein_id": "PROT_017", "affinity_nm": 8.5, "confidence": 0.94},       # Ibuprofen → COX2
    # --- Rapamycin → mTOR ---
    {"drug_id": "DRUG_007", "protein_id": "PROT_002", "affinity_nm": 0.2, "confidence": 0.99},       # Rapamycin → mTOR (very high)
    {"drug_id": "DRUG_007", "protein_id": "PROT_019", "affinity_nm": 340.0, "confidence": 0.58},     # Rapamycin → PI3K
    # --- Minoxidil ---
    {"drug_id": "DRUG_008", "protein_id": "PROT_015", "affinity_nm": 280.0, "confidence": 0.62},     # Minoxidil → VEGFR2
    {"drug_id": "DRUG_008", "protein_id": "PROT_031", "affinity_nm": 190.0, "confidence": 0.67},     # Minoxidil → AGTR1
    # --- Celecoxib → COX2 ---
    {"drug_id": "DRUG_009", "protein_id": "PROT_017", "affinity_nm": 1.2, "confidence": 0.98},       # Celecoxib → COX2
    {"drug_id": "DRUG_009", "protein_id": "PROT_020", "affinity_nm": 620.0, "confidence": 0.48},     # Celecoxib → AKT
    # --- Tamoxifen → ESR1 ---
    {"drug_id": "DRUG_010", "protein_id": "PROT_030", "affinity_nm": 0.9, "confidence": 0.99},       # Tamoxifen → ESR1
    {"drug_id": "DRUG_010", "protein_id": "PROT_050", "affinity_nm": 380.0, "confidence": 0.54},     # Tamoxifen → CDK4
    # --- Propranolol → ADRB2 ---
    {"drug_id": "DRUG_011", "protein_id": "PROT_032", "affinity_nm": 1.5, "confidence": 0.97},       # Propranolol → ADRB2
    # --- Valproic acid → HDAC1, GABRA1 ---
    {"drug_id": "DRUG_012", "protein_id": "PROT_033", "affinity_nm": 45.0, "confidence": 0.85},      # Valproic acid → HDAC1
    {"drug_id": "DRUG_012", "protein_id": "PROT_028", "affinity_nm": 120.0, "confidence": 0.74},     # Valproic acid → GABRA1
    # --- Disulfiram → ALDH2 ---
    {"drug_id": "DRUG_013", "protein_id": "PROT_048", "affinity_nm": 7.2, "confidence": 0.93},       # Disulfiram → ALDH2
    {"drug_id": "DRUG_013", "protein_id": "PROT_023", "affinity_nm": 550.0, "confidence": 0.51},     # Disulfiram → NFKB1
    # --- Niclosamide → STAT3, mTOR ---
    {"drug_id": "DRUG_014", "protein_id": "PROT_022", "affinity_nm": 32.0, "confidence": 0.86},      # Niclosamide → STAT3
    {"drug_id": "DRUG_014", "protein_id": "PROT_002", "affinity_nm": 140.0, "confidence": 0.71},     # Niclosamide → mTOR
    # --- Chloroquine / Hydroxychloroquine ---
    {"drug_id": "DRUG_015", "protein_id": "PROT_046", "affinity_nm": 25.0, "confidence": 0.84},      # Chloroquine → CTSL
    {"drug_id": "DRUG_015", "protein_id": "PROT_012", "affinity_nm": 320.0, "confidence": 0.59},     # Chloroquine → ACE2
    {"drug_id": "DRUG_016", "protein_id": "PROT_046", "affinity_nm": 30.0, "confidence": 0.82},      # Hydroxychloroquine → CTSL
    {"drug_id": "DRUG_016", "protein_id": "PROT_012", "affinity_nm": 350.0, "confidence": 0.57},     # Hydroxychloroquine → ACE2
    {"drug_id": "DRUG_016", "protein_id": "PROT_014", "affinity_nm": 190.0, "confidence": 0.68},     # Hydroxychloroquine → TNF
    # --- Doxycycline / Minocycline → MMP9 ---
    {"drug_id": "DRUG_017", "protein_id": "PROT_034", "affinity_nm": 38.0, "confidence": 0.83},      # Doxycycline → MMP9
    {"drug_id": "DRUG_018", "protein_id": "PROT_034", "affinity_nm": 42.0, "confidence": 0.81},      # Minocycline → MMP9
    {"drug_id": "DRUG_018", "protein_id": "PROT_035", "affinity_nm": 280.0, "confidence": 0.58},     # Minocycline → CASP3
    # --- Losartan / Telmisartan → AGTR1 ---
    {"drug_id": "DRUG_019", "protein_id": "PROT_031", "affinity_nm": 2.8, "confidence": 0.97},       # Losartan → AGTR1
    {"drug_id": "DRUG_019", "protein_id": "PROT_024", "affinity_nm": 720.0, "confidence": 0.42},     # Losartan → PPARG
    {"drug_id": "DRUG_020", "protein_id": "PROT_031", "affinity_nm": 1.8, "confidence": 0.98},       # Telmisartan → AGTR1
    {"drug_id": "DRUG_020", "protein_id": "PROT_024", "affinity_nm": 65.0, "confidence": 0.87},      # Telmisartan → PPARG
    # --- Pioglitazone → PPARG ---
    {"drug_id": "DRUG_021", "protein_id": "PROT_024", "affinity_nm": 0.5, "confidence": 0.99},       # Pioglitazone → PPARG
    {"drug_id": "DRUG_021", "protein_id": "PROT_001", "affinity_nm": 580.0, "confidence": 0.49},     # Pioglitazone → AMPK
    # --- Riluzole / Memantine → GRIN1 ---
    {"drug_id": "DRUG_022", "protein_id": "PROT_025", "affinity_nm": 15.0, "confidence": 0.89},      # Riluzole → GRIN1
    {"drug_id": "DRUG_023", "protein_id": "PROT_025", "affinity_nm": 8.0, "confidence": 0.92},       # Memantine → GRIN1
    # --- Donepezil / Galantamine / Rivastigmine → ACHE, BCHE ---
    {"drug_id": "DRUG_024", "protein_id": "PROT_026", "affinity_nm": 6.7, "confidence": 0.95},       # Donepezil → ACHE
    {"drug_id": "DRUG_025", "protein_id": "PROT_026", "affinity_nm": 11.0, "confidence": 0.91},      # Galantamine → ACHE
    {"drug_id": "DRUG_025", "protein_id": "PROT_027", "affinity_nm": 45.0, "confidence": 0.79},      # Galantamine → BCHE
    {"drug_id": "DRUG_026", "protein_id": "PROT_026", "affinity_nm": 4.3, "confidence": 0.96},       # Rivastigmine → ACHE
    {"drug_id": "DRUG_026", "protein_id": "PROT_027", "affinity_nm": 8.5, "confidence": 0.93},       # Rivastigmine → BCHE
    # --- Levetiracetam ---
    {"drug_id": "DRUG_027", "protein_id": "PROT_028", "affinity_nm": 95.0, "confidence": 0.73},      # Levetiracetam → GABRA1
    # --- Topiramate → GABRA1 ---
    {"drug_id": "DRUG_028", "protein_id": "PROT_028", "affinity_nm": 55.0, "confidence": 0.80},      # Topiramate → GABRA1
    {"drug_id": "DRUG_028", "protein_id": "PROT_017", "affinity_nm": 710.0, "confidence": 0.43},     # Topiramate → COX2
    # --- Gabapentin / Pregabalin → CACNA2D1 ---
    {"drug_id": "DRUG_029", "protein_id": "PROT_029", "affinity_nm": 22.0, "confidence": 0.90},      # Gabapentin → CACNA2D1
    {"drug_id": "DRUG_030", "protein_id": "PROT_029", "affinity_nm": 6.0, "confidence": 0.95},       # Pregabalin → CACNA2D1
    # --- Additional cross-class bindings for richer graph ---
    {"drug_id": "DRUG_001", "protein_id": "PROT_019", "affinity_nm": 680.0, "confidence": 0.44},     # Metformin → PI3K (weak)
    {"drug_id": "DRUG_002", "protein_id": "PROT_023", "affinity_nm": 150.0, "confidence": 0.69},     # Thalidomide → NFKB1
    {"drug_id": "DRUG_003", "protein_id": "PROT_032", "affinity_nm": 750.0, "confidence": 0.41},     # Sildenafil → ADRB2 (weak)
    {"drug_id": "DRUG_004", "protein_id": "PROT_022", "affinity_nm": 180.0, "confidence": 0.66},     # Baricitinib → STAT3 (via JAK pathway)
    {"drug_id": "DRUG_007", "protein_id": "PROT_036", "affinity_nm": 260.0, "confidence": 0.62},     # Rapamycin → HIF1A
    {"drug_id": "DRUG_009", "protein_id": "PROT_023", "affinity_nm": 410.0, "confidence": 0.53},     # Celecoxib → NFKB1
    {"drug_id": "DRUG_010", "protein_id": "PROT_019", "affinity_nm": 520.0, "confidence": 0.50},     # Tamoxifen → PI3K
    {"drug_id": "DRUG_011", "protein_id": "PROT_036", "affinity_nm": 490.0, "confidence": 0.52},     # Propranolol → HIF1A
    {"drug_id": "DRUG_012", "protein_id": "PROT_038", "affinity_nm": 180.0, "confidence": 0.70},     # Valproic acid → GSK3B
    {"drug_id": "DRUG_013", "protein_id": "PROT_039", "affinity_nm": 350.0, "confidence": 0.56},     # Disulfiram → TP53
    {"drug_id": "DRUG_014", "protein_id": "PROT_049", "affinity_nm": 240.0, "confidence": 0.63},     # Niclosamide → DVL1 (Wnt)
    {"drug_id": "DRUG_017", "protein_id": "PROT_023", "affinity_nm": 470.0, "confidence": 0.50},     # Doxycycline → NFKB1
    {"drug_id": "DRUG_021", "protein_id": "PROT_023", "affinity_nm": 390.0, "confidence": 0.55},     # Pioglitazone → NFKB1
    {"drug_id": "DRUG_022", "protein_id": "PROT_038", "affinity_nm": 320.0, "confidence": 0.57},     # Riluzole → GSK3B
    {"drug_id": "DRUG_023", "protein_id": "PROT_038", "affinity_nm": 290.0, "confidence": 0.60},     # Memantine → GSK3B
]

INTERACTS: list[dict[str, object]] = [
    # --- JAK-STAT pathway: JAK1 → STAT3 → NFKB1 ---
    {"src_protein_id": "PROT_004", "dst_protein_id": "PROT_022", "string_score": 0.96},   # JAK1 → STAT3
    {"src_protein_id": "PROT_005", "dst_protein_id": "PROT_022", "string_score": 0.95},   # JAK2 → STAT3
    {"src_protein_id": "PROT_006", "dst_protein_id": "PROT_022", "string_score": 0.91},   # JAK3 → STAT3
    {"src_protein_id": "PROT_022", "dst_protein_id": "PROT_023", "string_score": 0.88},   # STAT3 → NFKB1
    {"src_protein_id": "PROT_022", "dst_protein_id": "PROT_013", "string_score": 0.87},   # STAT3 → IL6
    {"src_protein_id": "PROT_023", "dst_protein_id": "PROT_014", "string_score": 0.92},   # NFKB1 → TNF
    {"src_protein_id": "PROT_023", "dst_protein_id": "PROT_013", "string_score": 0.90},   # NFKB1 → IL6
    {"src_protein_id": "PROT_023", "dst_protein_id": "PROT_041", "string_score": 0.89},   # NFKB1 → IL1B
    # --- AMPK-mTOR pathway: AMPK → mTOR → PI3K → AKT ---
    {"src_protein_id": "PROT_001", "dst_protein_id": "PROT_002", "string_score": 0.94},   # AMPK → mTOR
    {"src_protein_id": "PROT_002", "dst_protein_id": "PROT_019", "string_score": 0.93},   # mTOR → PI3K
    {"src_protein_id": "PROT_019", "dst_protein_id": "PROT_020", "string_score": 0.95},   # PI3K → AKT
    {"src_protein_id": "PROT_020", "dst_protein_id": "PROT_002", "string_score": 0.91},   # AKT → mTOR (feedback)
    {"src_protein_id": "PROT_020", "dst_protein_id": "PROT_038", "string_score": 0.86},   # AKT → GSK3B
    {"src_protein_id": "PROT_020", "dst_protein_id": "PROT_039", "string_score": 0.83},   # AKT → TP53
    # --- CRBN ubiquitin pathway: CRBN → IKZF1, CRBN → IKZF3 ---
    {"src_protein_id": "PROT_008", "dst_protein_id": "PROT_009", "string_score": 0.97},   # CRBN → IKZF1
    {"src_protein_id": "PROT_008", "dst_protein_id": "PROT_010", "string_score": 0.96},   # CRBN → IKZF3
    {"src_protein_id": "PROT_009", "dst_protein_id": "PROT_023", "string_score": 0.78},   # IKZF1 → NFKB1
    {"src_protein_id": "PROT_010", "dst_protein_id": "PROT_022", "string_score": 0.76},   # IKZF3 → STAT3
    # --- AAK1-ACE2 viral entry: AAK1 → ACE2 → TMPRSS2 ---
    {"src_protein_id": "PROT_011", "dst_protein_id": "PROT_012", "string_score": 0.85},   # AAK1 → ACE2
    {"src_protein_id": "PROT_012", "dst_protein_id": "PROT_045", "string_score": 0.92},   # ACE2 → TMPRSS2
    {"src_protein_id": "PROT_045", "dst_protein_id": "PROT_046", "string_score": 0.79},   # TMPRSS2 → CTSL
    {"src_protein_id": "PROT_012", "dst_protein_id": "PROT_031", "string_score": 0.88},   # ACE2 → AGTR1
    # --- MAPK/RAS pathway ---
    {"src_protein_id": "PROT_003", "dst_protein_id": "PROT_021", "string_score": 0.94},   # EGFR → MAPK1
    {"src_protein_id": "PROT_003", "dst_protein_id": "PROT_019", "string_score": 0.92},   # EGFR → PI3K
    {"src_protein_id": "PROT_021", "dst_protein_id": "PROT_018", "string_score": 0.87},   # MAPK1 → BRAF
    {"src_protein_id": "PROT_018", "dst_protein_id": "PROT_021", "string_score": 0.89},   # BRAF → MAPK1
    {"src_protein_id": "PROT_021", "dst_protein_id": "PROT_022", "string_score": 0.82},   # MAPK1 → STAT3
    # --- Angiogenesis: VEGFR2 → HIF1A ---
    {"src_protein_id": "PROT_015", "dst_protein_id": "PROT_036", "string_score": 0.84},   # VEGFR2 → HIF1A
    {"src_protein_id": "PROT_036", "dst_protein_id": "PROT_015", "string_score": 0.81},   # HIF1A → VEGFR2 (feedback)
    {"src_protein_id": "PROT_036", "dst_protein_id": "PROT_002", "string_score": 0.78},   # HIF1A → mTOR
    # --- Estrogen receptor signaling ---
    {"src_protein_id": "PROT_030", "dst_protein_id": "PROT_019", "string_score": 0.80},   # ESR1 → PI3K
    {"src_protein_id": "PROT_030", "dst_protein_id": "PROT_050", "string_score": 0.82},   # ESR1 → CDK4
    {"src_protein_id": "PROT_050", "dst_protein_id": "PROT_039", "string_score": 0.85},   # CDK4 → TP53
    # --- Inflammatory mediator interactions ---
    {"src_protein_id": "PROT_014", "dst_protein_id": "PROT_023", "string_score": 0.93},   # TNF → NFKB1
    {"src_protein_id": "PROT_013", "dst_protein_id": "PROT_004", "string_score": 0.90},   # IL6 → JAK1
    {"src_protein_id": "PROT_013", "dst_protein_id": "PROT_005", "string_score": 0.88},   # IL6 → JAK2
    {"src_protein_id": "PROT_041", "dst_protein_id": "PROT_023", "string_score": 0.87},   # IL1B → NFKB1
    {"src_protein_id": "PROT_041", "dst_protein_id": "PROT_035", "string_score": 0.74},   # IL1B → CASP3
    # --- PDE5 → cGMP-related (connect PDE5A to cardiovascular pathways) ---
    {"src_protein_id": "PROT_007", "dst_protein_id": "PROT_015", "string_score": 0.73},   # PDE5A → VEGFR2
    {"src_protein_id": "PROT_007", "dst_protein_id": "PROT_032", "string_score": 0.68},   # PDE5A → ADRB2
    # --- COX-mediated links ---
    {"src_protein_id": "PROT_017", "dst_protein_id": "PROT_023", "string_score": 0.79},   # COX2 → NFKB1
    {"src_protein_id": "PROT_016", "dst_protein_id": "PROT_017", "string_score": 0.91},   # COX1 → COX2
    # --- Neurodegeneration links ---
    {"src_protein_id": "PROT_026", "dst_protein_id": "PROT_025", "string_score": 0.72},   # ACHE → GRIN1
    {"src_protein_id": "PROT_027", "dst_protein_id": "PROT_026", "string_score": 0.81},   # BCHE → ACHE
    {"src_protein_id": "PROT_025", "dst_protein_id": "PROT_035", "string_score": 0.75},   # GRIN1 → CASP3
    {"src_protein_id": "PROT_038", "dst_protein_id": "PROT_039", "string_score": 0.84},   # GSK3B → TP53
    {"src_protein_id": "PROT_037", "dst_protein_id": "PROT_001", "string_score": 0.82},   # SIRT1 → AMPK
    {"src_protein_id": "PROT_037", "dst_protein_id": "PROT_039", "string_score": 0.77},   # SIRT1 → TP53
    # --- Fibrosis / TGF-beta ---
    {"src_protein_id": "PROT_042", "dst_protein_id": "PROT_022", "string_score": 0.83},   # TGFB1 → STAT3
    {"src_protein_id": "PROT_042", "dst_protein_id": "PROT_034", "string_score": 0.80},   # TGFB1 → MMP9
    {"src_protein_id": "PROT_042", "dst_protein_id": "PROT_023", "string_score": 0.78},   # TGFB1 → NFKB1
    # --- PPARG links ---
    {"src_protein_id": "PROT_024", "dst_protein_id": "PROT_023", "string_score": 0.76},   # PPARG → NFKB1
    {"src_protein_id": "PROT_024", "dst_protein_id": "PROT_001", "string_score": 0.74},   # PPARG → AMPK
    # --- DVL1 / Wnt ---
    {"src_protein_id": "PROT_049", "dst_protein_id": "PROT_038", "string_score": 0.85},   # DVL1 → GSK3B
    {"src_protein_id": "PROT_049", "dst_protein_id": "PROT_040", "string_score": 0.72},   # DVL1 → BCL2
    # --- Apoptosis cascade ---
    {"src_protein_id": "PROT_039", "dst_protein_id": "PROT_040", "string_score": 0.90},   # TP53 → BCL2
    {"src_protein_id": "PROT_040", "dst_protein_id": "PROT_035", "string_score": 0.88},   # BCL2 → CASP3
    {"src_protein_id": "PROT_035", "dst_protein_id": "PROT_034", "string_score": 0.71},   # CASP3 → MMP9
    # --- Ion channel cross-talk ---
    {"src_protein_id": "PROT_028", "dst_protein_id": "PROT_025", "string_score": 0.70},   # GABRA1 → GRIN1
    {"src_protein_id": "PROT_029", "dst_protein_id": "PROT_025", "string_score": 0.69},   # CACNA2D1 → GRIN1
    {"src_protein_id": "PROT_029", "dst_protein_id": "PROT_028", "string_score": 0.66},   # CACNA2D1 → GABRA1
    # --- Transporter links ---
    {"src_protein_id": "PROT_043", "dst_protein_id": "PROT_025", "string_score": 0.65},   # SLC6A3 → GRIN1
    {"src_protein_id": "PROT_044", "dst_protein_id": "PROT_025", "string_score": 0.64},   # SLC6A4 → GRIN1
    # --- HDAC / epigenetic links ---
    {"src_protein_id": "PROT_033", "dst_protein_id": "PROT_039", "string_score": 0.83},   # HDAC1 → TP53
    {"src_protein_id": "PROT_033", "dst_protein_id": "PROT_023", "string_score": 0.79},   # HDAC1 → NFKB1
    # --- Additional AMPK downstream ---
    {"src_protein_id": "PROT_001", "dst_protein_id": "PROT_037", "string_score": 0.80},   # AMPK → SIRT1
    {"src_protein_id": "PROT_001", "dst_protein_id": "PROT_024", "string_score": 0.72},   # AMPK → PPARG
    # --- AGTR1 cardiovascular links ---
    {"src_protein_id": "PROT_031", "dst_protein_id": "PROT_023", "string_score": 0.81},   # AGTR1 → NFKB1
    {"src_protein_id": "PROT_031", "dst_protein_id": "PROT_042", "string_score": 0.77},   # AGTR1 → TGFB1
    # --- ALDH2 links ---
    {"src_protein_id": "PROT_048", "dst_protein_id": "PROT_039", "string_score": 0.68},   # ALDH2 → TP53
    # --- MMP9 to angiogenesis ---
    {"src_protein_id": "PROT_034", "dst_protein_id": "PROT_015", "string_score": 0.76},   # MMP9 → VEGFR2
    # --- DHFR to cell cycle ---
    {"src_protein_id": "PROT_047", "dst_protein_id": "PROT_039", "string_score": 0.67},   # DHFR → TP53
]

ASSOCIATED_WITH: list[dict[str, object]] = [
    # --- Metabolic ---
    {"protein_id": "PROT_001", "disease_id": "DIS_001", "gda_score": 0.92},   # AMPK → Type 2 diabetes
    {"protein_id": "PROT_024", "disease_id": "DIS_001", "gda_score": 0.85},   # PPARG → Type 2 diabetes
    {"protein_id": "PROT_002", "disease_id": "DIS_001", "gda_score": 0.71},   # mTOR → Type 2 diabetes
    # --- Autoimmune / inflammatory ---
    {"protein_id": "PROT_004", "disease_id": "DIS_002", "gda_score": 0.93},   # JAK1 → RA
    {"protein_id": "PROT_005", "disease_id": "DIS_002", "gda_score": 0.91},   # JAK2 → RA
    {"protein_id": "PROT_014", "disease_id": "DIS_002", "gda_score": 0.89},   # TNF → RA
    {"protein_id": "PROT_013", "disease_id": "DIS_002", "gda_score": 0.86},   # IL6 → RA
    {"protein_id": "PROT_023", "disease_id": "DIS_017", "gda_score": 0.82},   # NFKB1 → Psoriasis
    {"protein_id": "PROT_014", "disease_id": "DIS_017", "gda_score": 0.80},   # TNF → Psoriasis
    # --- Sexual health / cardiovascular ---
    {"protein_id": "PROT_007", "disease_id": "DIS_003", "gda_score": 0.95},   # PDE5A → ED
    {"protein_id": "PROT_015", "disease_id": "DIS_013", "gda_score": 0.84},   # VEGFR2 → PAH
    {"protein_id": "PROT_031", "disease_id": "DIS_012", "gda_score": 0.90},   # AGTR1 → Hypertension
    {"protein_id": "PROT_032", "disease_id": "DIS_012", "gda_score": 0.78},   # ADRB2 → Hypertension
    # --- COVID-19 (key for Baricitinib repurposing story) ---
    {"protein_id": "PROT_022", "disease_id": "DIS_004", "gda_score": 0.88},   # STAT3 → COVID-19
    {"protein_id": "PROT_012", "disease_id": "DIS_004", "gda_score": 0.94},   # ACE2 → COVID-19
    {"protein_id": "PROT_045", "disease_id": "DIS_004", "gda_score": 0.91},   # TMPRSS2 → COVID-19
    {"protein_id": "PROT_013", "disease_id": "DIS_004", "gda_score": 0.83},   # IL6 → COVID-19
    {"protein_id": "PROT_046", "disease_id": "DIS_004", "gda_score": 0.76},   # CTSL → COVID-19
    # --- Oncology ---
    {"protein_id": "PROT_009", "disease_id": "DIS_005", "gda_score": 0.93},   # IKZF1 → Multiple myeloma
    {"protein_id": "PROT_010", "disease_id": "DIS_005", "gda_score": 0.91},   # IKZF3 → Multiple myeloma
    {"protein_id": "PROT_030", "disease_id": "DIS_008", "gda_score": 0.94},   # ESR1 → Breast cancer
    {"protein_id": "PROT_050", "disease_id": "DIS_008", "gda_score": 0.81},   # CDK4 → Breast cancer
    {"protein_id": "PROT_003", "disease_id": "DIS_009", "gda_score": 0.95},   # EGFR → Lung cancer
    {"protein_id": "PROT_021", "disease_id": "DIS_009", "gda_score": 0.82},   # MAPK1 → Lung cancer
    {"protein_id": "PROT_017", "disease_id": "DIS_010", "gda_score": 0.79},   # COX2 → Colorectal cancer
    {"protein_id": "PROT_023", "disease_id": "DIS_010", "gda_score": 0.74},   # NFKB1 → Colorectal cancer
    {"protein_id": "PROT_036", "disease_id": "DIS_011", "gda_score": 0.87},   # HIF1A → Glioblastoma
    {"protein_id": "PROT_002", "disease_id": "DIS_011", "gda_score": 0.83},   # mTOR → Glioblastoma
    {"protein_id": "PROT_039", "disease_id": "DIS_009", "gda_score": 0.88},   # TP53 → Lung cancer
    {"protein_id": "PROT_039", "disease_id": "DIS_008", "gda_score": 0.86},   # TP53 → Breast cancer
    # --- Neurodegenerative ---
    {"protein_id": "PROT_002", "disease_id": "DIS_006", "gda_score": 0.80},   # mTOR → Alzheimer
    {"protein_id": "PROT_026", "disease_id": "DIS_006", "gda_score": 0.91},   # ACHE → Alzheimer
    {"protein_id": "PROT_038", "disease_id": "DIS_006", "gda_score": 0.85},   # GSK3B → Alzheimer
    {"protein_id": "PROT_025", "disease_id": "DIS_006", "gda_score": 0.78},   # GRIN1 → Alzheimer
    {"protein_id": "PROT_043", "disease_id": "DIS_007", "gda_score": 0.89},   # SLC6A3 → Parkinson
    {"protein_id": "PROT_035", "disease_id": "DIS_007", "gda_score": 0.77},   # CASP3 → Parkinson
    # --- Neurological ---
    {"protein_id": "PROT_028", "disease_id": "DIS_014", "gda_score": 0.88},   # GABRA1 → Epilepsy
    {"protein_id": "PROT_029", "disease_id": "DIS_015", "gda_score": 0.86},   # CACNA2D1 → Neuropathic pain
    {"protein_id": "PROT_025", "disease_id": "DIS_014", "gda_score": 0.75},   # GRIN1 → Epilepsy
    # --- Psychiatric ---
    {"protein_id": "PROT_044", "disease_id": "DIS_016", "gda_score": 0.87},   # SLC6A4 → Depression
    # --- GI ---
    {"protein_id": "PROT_023", "disease_id": "DIS_018", "gda_score": 0.84},   # NFKB1 → Crohn
    {"protein_id": "PROT_014", "disease_id": "DIS_018", "gda_score": 0.81},   # TNF → Crohn
    {"protein_id": "PROT_023", "disease_id": "DIS_019", "gda_score": 0.82},   # NFKB1 → UC
    {"protein_id": "PROT_013", "disease_id": "DIS_019", "gda_score": 0.79},   # IL6 → UC
    # --- Pulmonary fibrosis ---
    {"protein_id": "PROT_042", "disease_id": "DIS_020", "gda_score": 0.90},   # TGFB1 → IPF
    {"protein_id": "PROT_034", "disease_id": "DIS_020", "gda_score": 0.76},   # MMP9 → IPF
]

INDICATED_FOR: list[dict[str, object]] = [
    # Focus cohort original indications
    {"drug_id": "DRUG_001", "disease_id": "DIS_001", "evidence": "phase_4_approved"},          # Metformin → T2D
    {"drug_id": "DRUG_002", "disease_id": "DIS_005", "evidence": "phase_4_approved"},          # Thalidomide → Multiple myeloma
    {"drug_id": "DRUG_003", "disease_id": "DIS_003", "evidence": "phase_4_approved"},          # Sildenafil → ED
    {"drug_id": "DRUG_003", "disease_id": "DIS_013", "evidence": "phase_4_approved"},          # Sildenafil → PAH
    {"drug_id": "DRUG_004", "disease_id": "DIS_002", "evidence": "phase_4_approved"},          # Baricitinib → RA
    {"drug_id": "DRUG_004", "disease_id": "DIS_004", "evidence": "emergency_use_auth"},        # Baricitinib → COVID-19
    # Other known indications
    {"drug_id": "DRUG_005", "disease_id": "DIS_012", "evidence": "phase_4_approved"},          # Aspirin → Hypertension (CV prevention)
    {"drug_id": "DRUG_006", "disease_id": "DIS_002", "evidence": "phase_4_approved"},          # Ibuprofen → RA
    {"drug_id": "DRUG_007", "disease_id": "DIS_011", "evidence": "phase_2_trial"},             # Rapamycin → Glioblastoma (investigational)
    {"drug_id": "DRUG_009", "disease_id": "DIS_002", "evidence": "phase_4_approved"},          # Celecoxib → RA
    {"drug_id": "DRUG_010", "disease_id": "DIS_008", "evidence": "phase_4_approved"},          # Tamoxifen → Breast cancer
    {"drug_id": "DRUG_012", "disease_id": "DIS_014", "evidence": "phase_4_approved"},          # Valproic acid → Epilepsy
    {"drug_id": "DRUG_019", "disease_id": "DIS_012", "evidence": "phase_4_approved"},          # Losartan → Hypertension
    {"drug_id": "DRUG_020", "disease_id": "DIS_012", "evidence": "phase_4_approved"},          # Telmisartan → Hypertension
    {"drug_id": "DRUG_021", "disease_id": "DIS_001", "evidence": "phase_4_approved"},          # Pioglitazone → T2D
    {"drug_id": "DRUG_023", "disease_id": "DIS_006", "evidence": "phase_4_approved"},          # Memantine → Alzheimer
    {"drug_id": "DRUG_024", "disease_id": "DIS_006", "evidence": "phase_4_approved"},          # Donepezil → Alzheimer
    {"drug_id": "DRUG_025", "disease_id": "DIS_006", "evidence": "phase_4_approved"},          # Galantamine → Alzheimer
    {"drug_id": "DRUG_026", "disease_id": "DIS_006", "evidence": "phase_4_approved"},          # Rivastigmine → Alzheimer
    {"drug_id": "DRUG_027", "disease_id": "DIS_014", "evidence": "phase_4_approved"},          # Levetiracetam → Epilepsy
    {"drug_id": "DRUG_029", "disease_id": "DIS_015", "evidence": "phase_4_approved"},          # Gabapentin → Neuropathic pain
    {"drug_id": "DRUG_030", "disease_id": "DIS_015", "evidence": "phase_4_approved"},          # Pregabalin → Neuropathic pain
]

CAUSES_ADR: list[dict[str, object]] = [
    # Metformin
    {"drug_id": "DRUG_001", "se_id": "SE_001", "frequency": 0.003},   # Lactic acidosis (rare)
    {"drug_id": "DRUG_001", "se_id": "SE_005", "frequency": 0.25},    # Nausea
    # Thalidomide
    {"drug_id": "DRUG_002", "se_id": "SE_002", "frequency": 0.95},    # Teratogenicity (very high)
    {"drug_id": "DRUG_002", "se_id": "SE_010", "frequency": 0.30},    # Peripheral neuropathy
    {"drug_id": "DRUG_002", "se_id": "SE_011", "frequency": 0.45},    # Drowsiness
    {"drug_id": "DRUG_002", "se_id": "SE_009", "frequency": 0.18},    # Neutropenia
    # Sildenafil
    {"drug_id": "DRUG_003", "se_id": "SE_003", "frequency": 0.12},    # Hypotension
    {"drug_id": "DRUG_003", "se_id": "SE_005", "frequency": 0.08},    # Nausea
    # Baricitinib
    {"drug_id": "DRUG_004", "se_id": "SE_004", "frequency": 0.15},    # Immunosuppression
    {"drug_id": "DRUG_004", "se_id": "SE_009", "frequency": 0.05},    # Neutropenia
    {"drug_id": "DRUG_004", "se_id": "SE_006", "frequency": 0.03},    # Hepatotoxicity
    # Aspirin
    {"drug_id": "DRUG_005", "se_id": "SE_013", "frequency": 0.04},    # GI bleeding
    {"drug_id": "DRUG_005", "se_id": "SE_005", "frequency": 0.10},    # Nausea
    # Ibuprofen
    {"drug_id": "DRUG_006", "se_id": "SE_013", "frequency": 0.03},    # GI bleeding
    {"drug_id": "DRUG_006", "se_id": "SE_008", "frequency": 0.02},    # Nephrotoxicity
    # Rapamycin
    {"drug_id": "DRUG_007", "se_id": "SE_004", "frequency": 0.22},    # Immunosuppression
    {"drug_id": "DRUG_007", "se_id": "SE_005", "frequency": 0.18},    # Nausea
    # Celecoxib
    {"drug_id": "DRUG_009", "se_id": "SE_007", "frequency": 0.02},    # QT prolongation
    {"drug_id": "DRUG_009", "se_id": "SE_008", "frequency": 0.04},    # Nephrotoxicity
    # Valproic acid
    {"drug_id": "DRUG_012", "se_id": "SE_006", "frequency": 0.08},    # Hepatotoxicity
    {"drug_id": "DRUG_012", "se_id": "SE_002", "frequency": 0.85},    # Teratogenicity
    {"drug_id": "DRUG_012", "se_id": "SE_012", "frequency": 0.35},    # Weight gain
    # Chloroquine
    {"drug_id": "DRUG_015", "se_id": "SE_007", "frequency": 0.06},    # QT prolongation
    {"drug_id": "DRUG_015", "se_id": "SE_015", "frequency": 0.12},    # Rash
    # Hydroxychloroquine
    {"drug_id": "DRUG_016", "se_id": "SE_007", "frequency": 0.04},    # QT prolongation
    {"drug_id": "DRUG_016", "se_id": "SE_015", "frequency": 0.09},    # Rash
    # Doxycycline
    {"drug_id": "DRUG_017", "se_id": "SE_014", "frequency": 0.15},    # Photosensitivity
    {"drug_id": "DRUG_017", "se_id": "SE_005", "frequency": 0.20},    # Nausea
    # Pioglitazone
    {"drug_id": "DRUG_021", "se_id": "SE_012", "frequency": 0.28},    # Weight gain
    {"drug_id": "DRUG_021", "se_id": "SE_006", "frequency": 0.02},    # Hepatotoxicity
    # Gabapentin
    {"drug_id": "DRUG_029", "se_id": "SE_011", "frequency": 0.22},    # Drowsiness
    {"drug_id": "DRUG_029", "se_id": "SE_012", "frequency": 0.14},    # Weight gain
    # Pregabalin
    {"drug_id": "DRUG_030", "se_id": "SE_011", "frequency": 0.28},    # Drowsiness
    {"drug_id": "DRUG_030", "se_id": "SE_012", "frequency": 0.18},    # Weight gain
]

NOTEBOOK_CASES: list[dict[str, object]] = [
    {"drug_id": "DRUG_001", "reason": "metformin_ampk_mtor_anticancer_longevity_repurposing"},
    {"drug_id": "DRUG_002", "reason": "thalidomide_crbn_ikzf_myeloma_repurposing_from_withdrawn"},
    {"drug_id": "DRUG_003", "reason": "sildenafil_pde5_pah_repurposing_from_angina_failure"},
    {"drug_id": "DRUG_004", "reason": "baricitinib_jak_aak1_covid19_repurposing_via_ai_prediction"},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_drug_repurposing"),
        help="Directory for generated notebook data files.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        output_dir / "drugs.csv",
        ["drug_id", "name", "drug_class", "approval_status", "fingerprint"],
        DRUGS,
    )
    _write_csv(
        output_dir / "proteins.csv",
        ["protein_id", "name", "gene_symbol", "family"],
        PROTEINS,
    )
    _write_csv(
        output_dir / "diseases.csv",
        ["disease_id", "name", "therapeutic_area"],
        DISEASES,
    )
    _write_csv(
        output_dir / "side_effects.csv",
        ["se_id", "name", "severity", "severity_weight"],
        SIDE_EFFECTS,
    )
    _write_csv(
        output_dir / "binds.csv",
        ["drug_id", "protein_id", "affinity_nm", "confidence"],
        BINDS,
    )
    _write_csv(
        output_dir / "interacts.csv",
        ["src_protein_id", "dst_protein_id", "string_score"],
        INTERACTS,
    )
    _write_csv(
        output_dir / "associated_with.csv",
        ["protein_id", "disease_id", "gda_score"],
        ASSOCIATED_WITH,
    )
    _write_csv(
        output_dir / "indicated_for.csv",
        ["drug_id", "disease_id", "evidence"],
        INDICATED_FOR,
    )
    _write_csv(
        output_dir / "causes_adr.csv",
        ["drug_id", "se_id", "frequency"],
        CAUSES_ADR,
    )
    _write_csv(
        output_dir / "notebook_cases.csv",
        ["drug_id", "reason"],
        NOTEBOOK_CASES,
    )

    manifest = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "snapshot_date": SNAPSHOT_DATE,
        "source": {
            "description": "Synthetic biomedical knowledge graph for drug repurposing demo. Inspired by Hetionet, DrugBank, STRING, DisGeNET, and OpenTargets but fully synthetic.",
            "urls": SOURCES,
            "license_note": "All data is synthetic and generated for demonstration purposes only. Not for clinical use.",
        },
        "shape": {
            "drugs": len(DRUGS),
            "proteins": len(PROTEINS),
            "diseases": len(DISEASES),
            "side_effects": len(SIDE_EFFECTS),
            "binds": len(BINDS),
            "interacts": len(INTERACTS),
            "associated_with": len(ASSOCIATED_WITH),
            "indicated_for": len(INDICATED_FOR),
            "causes_adr": len(CAUSES_ADR),
            "notebook_cases": len(NOTEBOOK_CASES),
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {output_dir / 'drugs.csv'} ({len(DRUGS)} rows)")
    print(f"wrote {output_dir / 'proteins.csv'} ({len(PROTEINS)} rows)")
    print(f"wrote {output_dir / 'diseases.csv'} ({len(DISEASES)} rows)")
    print(f"wrote {output_dir / 'side_effects.csv'} ({len(SIDE_EFFECTS)} rows)")
    print(f"wrote {output_dir / 'binds.csv'} ({len(BINDS)} rows)")
    print(f"wrote {output_dir / 'interacts.csv'} ({len(INTERACTS)} rows)")
    print(f"wrote {output_dir / 'associated_with.csv'} ({len(ASSOCIATED_WITH)} rows)")
    print(f"wrote {output_dir / 'indicated_for.csv'} ({len(INDICATED_FOR)} rows)")
    print(f"wrote {output_dir / 'causes_adr.csv'} ({len(CAUSES_ADR)} rows)")
    print(f"wrote {output_dir / 'notebook_cases.csv'} ({len(NOTEBOOK_CASES)} rows)")
    print(f"wrote {output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
