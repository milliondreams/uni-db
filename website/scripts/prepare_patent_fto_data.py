#!/usr/bin/env python3
"""Prepare deterministic snapshot data for the patent freedom-to-operate Locy flagship notebook."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path

SNAPSHOT_DATE = "2026-03-01"
SOURCES = {
    "uspto": "https://www.uspto.gov",
    "epo": "https://www.epo.org",
    "google_patents": "https://patents.google.com",
}

PATENTS: list[dict[str, object]] = [
    {"patent_id": "US-11234567", "title": "Wireless Sensor Network with Adaptive Mesh Routing and Low-Power Operation", "assignee": "SensorTech Corp", "priority_date": "2021-06-15", "status": "active", "jurisdiction": "US"},
    {"patent_id": "US-11234568", "title": "Low-Power Wireless Sensor Node with BLE Mesh Relay Capability", "assignee": "SensorTech Corp", "priority_date": "2021-06-15", "status": "active", "jurisdiction": "US"},
    {"patent_id": "EP-3456789", "title": "Wireless Sensor Network with Adaptive Mesh Routing and Low-Power Operation", "assignee": "SensorTech Corp", "priority_date": "2021-06-15", "status": "active", "jurisdiction": "EP"},
    {"patent_id": "CN-112345678", "title": "Wireless Sensor Network with Adaptive Mesh Routing and Low-Power Operation", "assignee": "SensorTech Corp", "priority_date": "2021-06-15", "status": "pending", "jurisdiction": "CN"},
    {"patent_id": "US-10987654", "title": "IoT Data Aggregation System with Edge-Based Sensor Fusion", "assignee": "IoT Solutions Ltd", "priority_date": "2020-09-22", "status": "active", "jurisdiction": "US"},
    {"patent_id": "US-10987655", "title": "Enhanced IoT Data Aggregation with Machine Learning Inference at the Edge", "assignee": "IoT Solutions Ltd", "priority_date": "2020-09-22", "status": "active", "jurisdiction": "US"},
    {"patent_id": "JP-2024567890", "title": "IoT Data Aggregation System with Edge-Based Sensor Fusion", "assignee": "IoT Solutions Ltd", "priority_date": "2020-09-22", "status": "active", "jurisdiction": "JP"},
    {"patent_id": "KR-1023456789", "title": "IoT Data Aggregation System with Edge-Based Sensor Fusion", "assignee": "IoT Solutions Ltd", "priority_date": "2020-09-22", "status": "pending", "jurisdiction": "KR"},
]

CLAIMS: list[dict[str, object]] = [
    # --- Family 1: SensorTech Corp (US-11234567) ---
    {"claim_id": "Pat1-C1", "patent_id": "US-11234567", "claim_type": "independent", "claim_text": "A wireless sensor network system comprising: a plurality of sensor nodes each having a wireless transceiver operating in the 2.4 GHz ISM band; a mesh networking protocol with adaptive routing that dynamically selects relay paths based on link quality metrics; and a low-power sleep controller that transitions each node between active and dormant states based on a configurable duty cycle.", "parent_claim_id": "", "embedding": [0.91, 0.34, -0.22, 0.78]},
    {"claim_id": "Pat1-C2", "patent_id": "US-11234567", "claim_type": "dependent", "claim_text": "The system of claim 1, wherein the wireless transceiver implements Bluetooth Low Energy 5.0 with coded PHY for extended range operation.", "parent_claim_id": "Pat1-C1", "embedding": [0.88, 0.31, -0.19, 0.82]},
    {"claim_id": "Pat1-C3", "patent_id": "US-11234567", "claim_type": "independent", "claim_text": "A method for operating a wireless sensor mesh network comprising: receiving sensor data from a plurality of distributed nodes via a BLE mesh transport layer; aggregating sensor readings using a sliding window algorithm with configurable window size and overlap; and transmitting aggregated results to a gateway node using multi-hop adaptive routing.", "parent_claim_id": "", "embedding": [0.85, 0.40, -0.18, 0.72]},
    # --- Family 1: SensorTech Corp (US-11234568, continuation) ---
    {"claim_id": "Pat2-C1", "patent_id": "US-11234568", "claim_type": "independent", "claim_text": "A low-power sensor node comprising: a BLE 5.0 radio module configured for mesh relay operation; an energy harvesting circuit coupled to a rechargeable power source; and a microcontroller executing a duty-cycle scheduler that maintains mesh connectivity during sleep intervals.", "parent_claim_id": "", "embedding": [0.82, 0.28, -0.30, 0.75]},
    {"claim_id": "Pat2-C2", "patent_id": "US-11234568", "claim_type": "dependent", "claim_text": "The sensor node of claim 1, further comprising a sensor fusion module that combines accelerometer and gyroscope data using a complementary filter to produce orientation estimates.", "parent_claim_id": "Pat2-C1", "embedding": [0.79, 0.25, -0.33, 0.71]},
    # --- Family 1: SensorTech Corp (EP-3456789) ---
    {"claim_id": "Pat3-C1", "patent_id": "EP-3456789", "claim_type": "independent", "claim_text": "A wireless sensor network system comprising: a plurality of sensor nodes each having a wireless transceiver operating in the 2.4 GHz ISM band; a mesh networking protocol with adaptive routing that dynamically selects relay paths based on link quality metrics; and a low-power sleep controller that transitions each node between active and dormant states based on a configurable duty cycle.", "parent_claim_id": "", "embedding": [0.90, 0.33, -0.21, 0.77]},
    {"claim_id": "Pat3-C2", "patent_id": "EP-3456789", "claim_type": "dependent", "claim_text": "The system of claim 1, wherein the adaptive routing protocol employs a Received Signal Strength Indicator threshold for link quality assessment.", "parent_claim_id": "Pat3-C1", "embedding": [0.87, 0.30, -0.25, 0.80]},
    # --- Family 1: SensorTech Corp (CN-112345678) ---
    {"claim_id": "Pat4-C1", "patent_id": "CN-112345678", "claim_type": "independent", "claim_text": "A wireless sensor network system comprising: a plurality of sensor nodes each having a wireless transceiver operating in the 2.4 GHz ISM band; a mesh networking protocol with adaptive routing; and a low-power sleep controller implementing a configurable duty cycle.", "parent_claim_id": "", "embedding": [0.89, 0.32, -0.20, 0.76]},
    # --- Family 2: IoT Solutions Ltd (US-10987654) ---
    {"claim_id": "Pat5-C1", "patent_id": "US-10987654", "claim_type": "independent", "claim_text": "An IoT data aggregation system comprising: a sensor fusion module that combines heterogeneous sensor inputs using weighted Kalman filtering; an edge processing unit that performs real-time statistical analysis on fused sensor streams; a data aggregation pipeline that compresses and batches processed readings for periodic uplink transmission; and a secure communication channel using TLS 1.3 with certificate pinning.", "parent_claim_id": "", "embedding": [0.45, 0.72, -0.15, 0.38]},
    {"claim_id": "Pat5-C2", "patent_id": "US-10987654", "claim_type": "dependent", "claim_text": "The system of claim 1, wherein the sensor fusion module implements an Extended Kalman Filter with adaptive noise covariance estimation.", "parent_claim_id": "Pat5-C1", "embedding": [0.42, 0.75, -0.12, 0.35]},
    {"claim_id": "Pat5-C3", "patent_id": "US-10987654", "claim_type": "dependent", "claim_text": "The system of claim 1, wherein the data aggregation pipeline applies delta encoding followed by LZ4 compression to reduce transmission bandwidth.", "parent_claim_id": "Pat5-C1", "embedding": [0.40, 0.70, -0.18, 0.42]},
    # --- Family 2: IoT Solutions Ltd (US-10987655, continuation-in-part) ---
    {"claim_id": "Pat6-C1", "patent_id": "US-10987655", "claim_type": "independent", "claim_text": "A method for edge-based machine learning inference in an IoT sensor network comprising: collecting sensor data from a plurality of heterogeneous sensors; preprocessing collected data using a feature extraction pipeline; executing a quantized neural network model on an edge processor to generate predictions; and transmitting prediction results alongside raw sensor metadata to a cloud endpoint.", "parent_claim_id": "", "embedding": [0.48, 0.68, -0.20, 0.55]},
    {"claim_id": "Pat6-C2", "patent_id": "US-10987655", "claim_type": "dependent", "claim_text": "The method of claim 1, wherein the quantized neural network model is an INT8-quantized convolutional neural network optimized for microcontroller deployment.", "parent_claim_id": "Pat6-C1", "embedding": [0.46, 0.65, -0.22, 0.52]},
    # --- Family 2: IoT Solutions Ltd (JP-2024567890) ---
    {"claim_id": "Pat7-C1", "patent_id": "JP-2024567890", "claim_type": "independent", "claim_text": "An IoT data aggregation system comprising: a sensor fusion module that combines heterogeneous sensor inputs using weighted Kalman filtering; an edge processing unit that performs real-time statistical analysis; a data aggregation pipeline for compressed periodic uplink; and a secure communication channel using TLS 1.3.", "parent_claim_id": "", "embedding": [0.44, 0.71, -0.14, 0.37]},
    # --- Family 2: IoT Solutions Ltd (KR-1023456789) ---
    {"claim_id": "Pat8-C1", "patent_id": "KR-1023456789", "claim_type": "independent", "claim_text": "An IoT data aggregation system comprising: a sensor fusion module combining heterogeneous sensor inputs; an edge processing unit for real-time analysis; and a data aggregation pipeline with compressed uplink transmission.", "parent_claim_id": "", "embedding": [0.43, 0.69, -0.16, 0.39]},
    # --- Additional dependent claims for richer structure ---
    {"claim_id": "Pat1-C4", "patent_id": "US-11234567", "claim_type": "dependent", "claim_text": "The system of claim 1, wherein the mesh networking protocol supports at least 64 concurrent relay nodes with sub-100ms end-to-end latency.", "parent_claim_id": "Pat1-C1", "embedding": [0.86, 0.36, -0.24, 0.74]},
    {"claim_id": "Pat5-C4", "patent_id": "US-10987654", "claim_type": "dependent", "claim_text": "The system of claim 1, wherein the edge processing unit executes anomaly detection using a sliding window z-score algorithm.", "parent_claim_id": "Pat5-C1", "embedding": [0.41, 0.73, -0.11, 0.36]},
    {"claim_id": "Pat6-C3", "patent_id": "US-10987655", "claim_type": "dependent", "claim_text": "The method of claim 1, wherein the feature extraction pipeline applies fast Fourier transform to accelerometer time-series data prior to model inference.", "parent_claim_id": "Pat6-C1", "embedding": [0.47, 0.66, -0.21, 0.53]},
]

CLAIM_ELEMENTS: list[dict[str, object]] = [
    # --- Pat1-C1: 3 elements (SensorHub Pro should map to ALL with high confidence) ---
    {"element_id": "CE-Pat1-C1-01", "claim_id": "Pat1-C1", "element_text": "a plurality of sensor nodes each having a wireless transceiver operating in the 2.4 GHz ISM band"},
    {"element_id": "CE-Pat1-C1-02", "claim_id": "Pat1-C1", "element_text": "a mesh networking protocol with adaptive routing that dynamically selects relay paths based on link quality metrics"},
    {"element_id": "CE-Pat1-C1-03", "claim_id": "Pat1-C1", "element_text": "a low-power sleep controller that transitions each node between active and dormant states based on a configurable duty cycle"},
    # --- Pat1-C2: 2 elements ---
    {"element_id": "CE-Pat1-C2-01", "claim_id": "Pat1-C2", "element_text": "the wireless transceiver implements Bluetooth Low Energy 5.0"},
    {"element_id": "CE-Pat1-C2-02", "claim_id": "Pat1-C2", "element_text": "coded PHY for extended range operation"},
    # --- Pat1-C3: 3 elements ---
    {"element_id": "CE-Pat1-C3-01", "claim_id": "Pat1-C3", "element_text": "receiving sensor data from a plurality of distributed nodes via a BLE mesh transport layer"},
    {"element_id": "CE-Pat1-C3-02", "claim_id": "Pat1-C3", "element_text": "aggregating sensor readings using a sliding window algorithm with configurable window size and overlap"},
    {"element_id": "CE-Pat1-C3-03", "claim_id": "Pat1-C3", "element_text": "transmitting aggregated results to a gateway node using multi-hop adaptive routing"},
    # --- Pat1-C4: 2 elements ---
    {"element_id": "CE-Pat1-C4-01", "claim_id": "Pat1-C4", "element_text": "the mesh networking protocol supports at least 64 concurrent relay nodes"},
    {"element_id": "CE-Pat1-C4-02", "claim_id": "Pat1-C4", "element_text": "sub-100ms end-to-end latency"},
    # --- Pat2-C1: 3 elements ---
    {"element_id": "CE-Pat2-C1-01", "claim_id": "Pat2-C1", "element_text": "a BLE 5.0 radio module configured for mesh relay operation"},
    {"element_id": "CE-Pat2-C1-02", "claim_id": "Pat2-C1", "element_text": "an energy harvesting circuit coupled to a rechargeable power source"},
    {"element_id": "CE-Pat2-C1-03", "claim_id": "Pat2-C1", "element_text": "a microcontroller executing a duty-cycle scheduler that maintains mesh connectivity during sleep intervals"},
    # --- Pat2-C2: 2 elements ---
    {"element_id": "CE-Pat2-C2-01", "claim_id": "Pat2-C2", "element_text": "a sensor fusion module that combines accelerometer and gyroscope data"},
    {"element_id": "CE-Pat2-C2-02", "claim_id": "Pat2-C2", "element_text": "a complementary filter to produce orientation estimates"},
    # --- Pat3-C1: 3 elements (mirrors Pat1-C1 for EP family) ---
    {"element_id": "CE-Pat3-C1-01", "claim_id": "Pat3-C1", "element_text": "a plurality of sensor nodes each having a wireless transceiver operating in the 2.4 GHz ISM band"},
    {"element_id": "CE-Pat3-C1-02", "claim_id": "Pat3-C1", "element_text": "a mesh networking protocol with adaptive routing that dynamically selects relay paths based on link quality metrics"},
    {"element_id": "CE-Pat3-C1-03", "claim_id": "Pat3-C1", "element_text": "a low-power sleep controller that transitions each node between active and dormant states based on a configurable duty cycle"},
    # --- Pat3-C2: 2 elements ---
    {"element_id": "CE-Pat3-C2-01", "claim_id": "Pat3-C2", "element_text": "the adaptive routing protocol employs a Received Signal Strength Indicator threshold"},
    {"element_id": "CE-Pat3-C2-02", "claim_id": "Pat3-C2", "element_text": "link quality assessment based on RSSI"},
    # --- Pat4-C1: 3 elements ---
    {"element_id": "CE-Pat4-C1-01", "claim_id": "Pat4-C1", "element_text": "a plurality of sensor nodes each having a wireless transceiver operating in the 2.4 GHz ISM band"},
    {"element_id": "CE-Pat4-C1-02", "claim_id": "Pat4-C1", "element_text": "a mesh networking protocol with adaptive routing"},
    {"element_id": "CE-Pat4-C1-03", "claim_id": "Pat4-C1", "element_text": "a low-power sleep controller implementing a configurable duty cycle"},
    # --- Pat5-C1: 4 elements (SensorHub Pro maps to 3 of 4, missing TLS) ---
    {"element_id": "CE-Pat5-C1-01", "claim_id": "Pat5-C1", "element_text": "a sensor fusion module that combines heterogeneous sensor inputs using weighted Kalman filtering"},
    {"element_id": "CE-Pat5-C1-02", "claim_id": "Pat5-C1", "element_text": "an edge processing unit that performs real-time statistical analysis on fused sensor streams"},
    {"element_id": "CE-Pat5-C1-03", "claim_id": "Pat5-C1", "element_text": "a data aggregation pipeline that compresses and batches processed readings for periodic uplink transmission"},
    {"element_id": "CE-Pat5-C1-04", "claim_id": "Pat5-C1", "element_text": "a secure communication channel using TLS 1.3 with certificate pinning"},
    # --- Pat5-C2: 2 elements ---
    {"element_id": "CE-Pat5-C2-01", "claim_id": "Pat5-C2", "element_text": "an Extended Kalman Filter with adaptive noise covariance estimation"},
    {"element_id": "CE-Pat5-C2-02", "claim_id": "Pat5-C2", "element_text": "sensor fusion module implementing the Extended Kalman Filter"},
    # --- Pat5-C3: 2 elements ---
    {"element_id": "CE-Pat5-C3-01", "claim_id": "Pat5-C3", "element_text": "delta encoding applied to sensor readings"},
    {"element_id": "CE-Pat5-C3-02", "claim_id": "Pat5-C3", "element_text": "LZ4 compression to reduce transmission bandwidth"},
    # --- Pat5-C4: 2 elements ---
    {"element_id": "CE-Pat5-C4-01", "claim_id": "Pat5-C4", "element_text": "anomaly detection on the edge processing unit"},
    {"element_id": "CE-Pat5-C4-02", "claim_id": "Pat5-C4", "element_text": "a sliding window z-score algorithm for anomaly detection"},
    # --- Pat6-C1: 4 elements ---
    {"element_id": "CE-Pat6-C1-01", "claim_id": "Pat6-C1", "element_text": "collecting sensor data from a plurality of heterogeneous sensors"},
    {"element_id": "CE-Pat6-C1-02", "claim_id": "Pat6-C1", "element_text": "preprocessing collected data using a feature extraction pipeline"},
    {"element_id": "CE-Pat6-C1-03", "claim_id": "Pat6-C1", "element_text": "executing a quantized neural network model on an edge processor to generate predictions"},
    {"element_id": "CE-Pat6-C1-04", "claim_id": "Pat6-C1", "element_text": "transmitting prediction results alongside raw sensor metadata to a cloud endpoint"},
    # --- Pat6-C2: 2 elements ---
    {"element_id": "CE-Pat6-C2-01", "claim_id": "Pat6-C2", "element_text": "an INT8-quantized convolutional neural network"},
    {"element_id": "CE-Pat6-C2-02", "claim_id": "Pat6-C2", "element_text": "optimized for microcontroller deployment"},
    # --- Pat6-C3: 2 elements ---
    {"element_id": "CE-Pat6-C3-01", "claim_id": "Pat6-C3", "element_text": "fast Fourier transform applied to accelerometer time-series data"},
    {"element_id": "CE-Pat6-C3-02", "claim_id": "Pat6-C3", "element_text": "feature extraction prior to model inference"},
    # --- Pat7-C1: 4 elements ---
    {"element_id": "CE-Pat7-C1-01", "claim_id": "Pat7-C1", "element_text": "a sensor fusion module that combines heterogeneous sensor inputs using weighted Kalman filtering"},
    {"element_id": "CE-Pat7-C1-02", "claim_id": "Pat7-C1", "element_text": "an edge processing unit that performs real-time statistical analysis"},
    {"element_id": "CE-Pat7-C1-03", "claim_id": "Pat7-C1", "element_text": "a data aggregation pipeline for compressed periodic uplink"},
    {"element_id": "CE-Pat7-C1-04", "claim_id": "Pat7-C1", "element_text": "a secure communication channel using TLS 1.3"},
    # --- Pat8-C1: 3 elements ---
    {"element_id": "CE-Pat8-C1-01", "claim_id": "Pat8-C1", "element_text": "a sensor fusion module combining heterogeneous sensor inputs"},
    {"element_id": "CE-Pat8-C1-02", "claim_id": "Pat8-C1", "element_text": "an edge processing unit for real-time analysis"},
    {"element_id": "CE-Pat8-C1-03", "claim_id": "Pat8-C1", "element_text": "a data aggregation pipeline with compressed uplink transmission"},
]

PRODUCTS: list[dict[str, object]] = [
    {"product_id": "PROD-001", "name": "SensorHub Pro", "description": "Full-featured IoT gateway with BLE mesh networking, sensor fusion, data aggregation, and edge ML inference capabilities for industrial wireless sensor deployments."},
    {"product_id": "PROD-002", "name": "SensorLite", "description": "Basic low-power sensor node with BLE connectivity and minimal onboard processing for environmental monitoring applications."},
    {"product_id": "PROD-003", "name": "MeshBridge", "description": "Dedicated mesh relay device providing range extension and protocol bridging between BLE sensor networks and Wi-Fi backhaul infrastructure."},
]

FEATURES: list[dict[str, object]] = [
    # --- SensorHub Pro (6 features) ---
    {"feature_id": "FEAT-001", "product_id": "PROD-001", "name": "BLE Radio Module", "description": "Bluetooth Low Energy 5.0 transceiver operating at 2.4 GHz with support for coded PHY and extended advertising for long-range communication."},
    {"feature_id": "FEAT-002", "product_id": "PROD-001", "name": "Mesh Networking Stack", "description": "Adaptive mesh routing protocol that dynamically selects optimal relay paths based on RSSI link quality metrics, supporting up to 128 concurrent nodes."},
    {"feature_id": "FEAT-003", "product_id": "PROD-001", "name": "Sensor Fusion Engine", "description": "Multi-sensor data fusion using weighted Kalman filtering to combine accelerometer, gyroscope, magnetometer, and environmental sensor readings."},
    {"feature_id": "FEAT-004", "product_id": "PROD-001", "name": "Low-Power Sleep Controller", "description": "Duty-cycle management system that transitions between active and deep-sleep states with configurable wake intervals and mesh keepalive maintenance."},
    {"feature_id": "FEAT-005", "product_id": "PROD-001", "name": "Data Aggregation Pipeline", "description": "Sliding window aggregation with configurable window size, overlap, and delta-encoded compressed batch uplink to gateway nodes."},
    {"feature_id": "FEAT-006", "product_id": "PROD-001", "name": "Edge ML Inference", "description": "On-device neural network inference engine supporting INT8-quantized models for real-time anomaly detection and predictive maintenance."},
    # --- SensorLite (4 features) ---
    {"feature_id": "FEAT-007", "product_id": "PROD-002", "name": "BLE Radio (Basic)", "description": "Bluetooth Low Energy 5.0 transceiver at 2.4 GHz with standard advertising for short-range sensor data transmission."},
    {"feature_id": "FEAT-008", "product_id": "PROD-002", "name": "Environmental Sensor Array", "description": "Onboard temperature, humidity, and barometric pressure sensors with 12-bit ADC resolution."},
    {"feature_id": "FEAT-009", "product_id": "PROD-002", "name": "Sleep Mode Controller", "description": "Basic duty-cycle sleep management with fixed wake intervals to conserve battery power."},
    {"feature_id": "FEAT-010", "product_id": "PROD-002", "name": "OTA Update Module", "description": "Over-the-air firmware update capability using BLE DFU protocol with integrity verification."},
    # --- MeshBridge (5 features) ---
    {"feature_id": "FEAT-011", "product_id": "PROD-003", "name": "BLE Mesh Relay", "description": "High-throughput BLE 5.0 mesh relay with multi-path routing and automatic topology discovery."},
    {"feature_id": "FEAT-012", "product_id": "PROD-003", "name": "Wi-Fi Backhaul Bridge", "description": "Protocol bridge translating BLE mesh packets to Wi-Fi for cloud connectivity via TLS 1.3 encrypted channels."},
    {"feature_id": "FEAT-013", "product_id": "PROD-003", "name": "Packet Aggregation Buffer", "description": "Store-and-forward buffer that batches sensor packets for efficient uplink transmission with configurable flush intervals."},
    {"feature_id": "FEAT-014", "product_id": "PROD-003", "name": "Link Quality Monitor", "description": "Real-time RSSI and packet error rate monitoring for adaptive route selection across the mesh network."},
    {"feature_id": "FEAT-015", "product_id": "PROD-003", "name": "Power Management Unit", "description": "PoE and battery-backed power supply with intelligent load balancing for continuous relay operation."},
]

PUBLICATIONS: list[dict[str, object]] = [
    # Before patent priority dates (valid prior art)
    {"pub_id": "PUB-001", "title": "BLE Mesh Networking for Industrial IoT: A Survey", "pub_date": "2019-03-15", "embedding": [0.88, 0.30, -0.20, 0.74]},
    {"pub_id": "PUB-002", "title": "Adaptive Routing Protocols for Low-Power Wireless Sensor Networks", "pub_date": "2018-11-22", "embedding": [0.84, 0.38, -0.25, 0.70]},
    {"pub_id": "PUB-003", "title": "Energy-Efficient Duty Cycling in IEEE 802.15.4 Mesh Networks", "pub_date": "2020-01-10", "embedding": [0.80, 0.25, -0.28, 0.68]},
    {"pub_id": "PUB-004", "title": "Kalman Filter Approaches for Multi-Sensor Fusion in IoT Systems", "pub_date": "2019-07-08", "embedding": [0.42, 0.70, -0.14, 0.36]},
    {"pub_id": "PUB-005", "title": "Edge Computing for Real-Time Sensor Data Aggregation", "pub_date": "2020-05-18", "embedding": [0.46, 0.68, -0.18, 0.40]},
    {"pub_id": "PUB-006", "title": "Bluetooth 5.0 Specification: Mesh Profile Overview", "pub_date": "2017-07-13", "embedding": [0.90, 0.28, -0.15, 0.80]},
    # After patent priority dates (not valid prior art)
    {"pub_id": "PUB-007", "title": "Neural Network Quantization Techniques for Microcontroller-Based IoT", "pub_date": "2022-09-05", "embedding": [0.50, 0.62, -0.22, 0.55]},
    {"pub_id": "PUB-008", "title": "TLS 1.3 Implementation Challenges in Resource-Constrained IoT Devices", "pub_date": "2023-02-14", "embedding": [0.38, 0.58, -0.10, 0.32]},
    {"pub_id": "PUB-009", "title": "Federated Learning at the IoT Edge: Opportunities and Constraints", "pub_date": "2023-06-20", "embedding": [0.52, 0.60, -0.24, 0.48]},
    {"pub_id": "PUB-010", "title": "Compressed Sensing for Bandwidth-Efficient Wireless Sensor Networks", "pub_date": "2020-03-25", "embedding": [0.44, 0.65, -0.16, 0.42]},
]

# --- Edge tables ---

HAS_CLAIM: list[dict[str, object]] = [
    {"patent_id": "US-11234567", "claim_id": "Pat1-C1"},
    {"patent_id": "US-11234567", "claim_id": "Pat1-C2"},
    {"patent_id": "US-11234567", "claim_id": "Pat1-C3"},
    {"patent_id": "US-11234567", "claim_id": "Pat1-C4"},
    {"patent_id": "US-11234568", "claim_id": "Pat2-C1"},
    {"patent_id": "US-11234568", "claim_id": "Pat2-C2"},
    {"patent_id": "EP-3456789", "claim_id": "Pat3-C1"},
    {"patent_id": "EP-3456789", "claim_id": "Pat3-C2"},
    {"patent_id": "CN-112345678", "claim_id": "Pat4-C1"},
    {"patent_id": "US-10987654", "claim_id": "Pat5-C1"},
    {"patent_id": "US-10987654", "claim_id": "Pat5-C2"},
    {"patent_id": "US-10987654", "claim_id": "Pat5-C3"},
    {"patent_id": "US-10987654", "claim_id": "Pat5-C4"},
    {"patent_id": "US-10987655", "claim_id": "Pat6-C1"},
    {"patent_id": "US-10987655", "claim_id": "Pat6-C2"},
    {"patent_id": "US-10987655", "claim_id": "Pat6-C3"},
    {"patent_id": "JP-2024567890", "claim_id": "Pat7-C1"},
    {"patent_id": "KR-1023456789", "claim_id": "Pat8-C1"},
]

DEPENDS_ON: list[dict[str, object]] = [
    {"claim_id": "Pat1-C2", "parent_claim_id": "Pat1-C1"},
    {"claim_id": "Pat1-C4", "parent_claim_id": "Pat1-C1"},
    {"claim_id": "Pat2-C2", "parent_claim_id": "Pat2-C1"},
    {"claim_id": "Pat3-C2", "parent_claim_id": "Pat3-C1"},
    {"claim_id": "Pat5-C2", "parent_claim_id": "Pat5-C1"},
    {"claim_id": "Pat5-C3", "parent_claim_id": "Pat5-C1"},
    {"claim_id": "Pat5-C4", "parent_claim_id": "Pat5-C1"},
    {"claim_id": "Pat6-C2", "parent_claim_id": "Pat6-C1"},
    {"claim_id": "Pat6-C3", "parent_claim_id": "Pat6-C1"},
]

HAS_ELEMENT: list[dict[str, object]] = [
    {"claim_id": c["claim_id"], "element_id": c["element_id"]}
    for c in CLAIM_ELEMENTS
]

HAS_FEATURE: list[dict[str, object]] = [
    {"product_id": f["product_id"], "feature_id": f["feature_id"]}
    for f in FEATURES
]

READS_ON: list[dict[str, object]] = [
    # --- SensorHub Pro vs Pat1-C1 (ALL elements mapped, high confidence → MPROD high) ---
    {"feature_id": "FEAT-001", "element_id": "CE-Pat1-C1-01", "confidence": 0.95},  # BLE Radio → 2.4 GHz transceiver
    {"feature_id": "FEAT-002", "element_id": "CE-Pat1-C1-02", "confidence": 0.92},  # Mesh Stack → adaptive routing
    {"feature_id": "FEAT-004", "element_id": "CE-Pat1-C1-03", "confidence": 0.90},  # Sleep Controller → duty cycle
    # --- SensorHub Pro vs Pat1-C3 (all elements, high confidence) ---
    {"feature_id": "FEAT-001", "element_id": "CE-Pat1-C3-01", "confidence": 0.88},  # BLE Radio → BLE mesh transport
    {"feature_id": "FEAT-005", "element_id": "CE-Pat1-C3-02", "confidence": 0.91},  # Aggregation → sliding window
    {"feature_id": "FEAT-002", "element_id": "CE-Pat1-C3-03", "confidence": 0.87},  # Mesh Stack → multi-hop routing
    # --- SensorHub Pro vs Pat5-C1 (3 of 4 elements, MISSING TLS → MPROD lower) ---
    {"feature_id": "FEAT-003", "element_id": "CE-Pat5-C1-01", "confidence": 0.85},  # Sensor Fusion → Kalman filtering
    {"feature_id": "FEAT-006", "element_id": "CE-Pat5-C1-02", "confidence": 0.72},  # Edge ML → statistical analysis (partial)
    {"feature_id": "FEAT-005", "element_id": "CE-Pat5-C1-03", "confidence": 0.80},  # Aggregation → compress/batch
    # NOTE: No mapping to CE-Pat5-C1-04 (TLS 1.3 with cert pinning) — SensorHub Pro uses DTLS, not TLS 1.3
    # --- SensorHub Pro vs Pat6-C1 (partial, decent confidence) ---
    {"feature_id": "FEAT-003", "element_id": "CE-Pat6-C1-01", "confidence": 0.78},  # Sensor Fusion → heterogeneous sensors
    {"feature_id": "FEAT-006", "element_id": "CE-Pat6-C1-02", "confidence": 0.82},  # Edge ML → feature extraction
    {"feature_id": "FEAT-006", "element_id": "CE-Pat6-C1-03", "confidence": 0.88},  # Edge ML → quantized NN
    {"feature_id": "FEAT-005", "element_id": "CE-Pat6-C1-04", "confidence": 0.65},  # Aggregation → transmit to cloud (loose)
    # --- SensorLite vs Pat2-C1 (partial, lower confidence) ---
    {"feature_id": "FEAT-007", "element_id": "CE-Pat2-C1-01", "confidence": 0.80},  # BLE Basic → BLE mesh relay
    # NOTE: No mapping to CE-Pat2-C1-02 (energy harvesting) — SensorLite uses battery only
    {"feature_id": "FEAT-009", "element_id": "CE-Pat2-C1-03", "confidence": 0.60},  # Sleep Mode → duty-cycle (basic, not mesh-aware)
    # --- MeshBridge vs Pat1-C1 elements (partial) ---
    {"feature_id": "FEAT-011", "element_id": "CE-Pat1-C1-01", "confidence": 0.75},  # BLE Mesh Relay → 2.4 GHz
    {"feature_id": "FEAT-011", "element_id": "CE-Pat1-C1-02", "confidence": 0.85},  # BLE Mesh Relay → adaptive routing
    {"feature_id": "FEAT-015", "element_id": "CE-Pat1-C1-03", "confidence": 0.35},  # Power Mgmt → duty cycle (weak mapping)
    # --- MeshBridge vs Pat5-C1 elements (partial via backhaul) ---
    {"feature_id": "FEAT-013", "element_id": "CE-Pat5-C1-03", "confidence": 0.70},  # Packet Aggregation → compress/batch
    {"feature_id": "FEAT-012", "element_id": "CE-Pat5-C1-04", "confidence": 0.90},  # Wi-Fi Bridge → TLS 1.3 (MeshBridge has this!)
    # --- Cross-product weak mappings for richer data ---
    {"feature_id": "FEAT-014", "element_id": "CE-Pat3-C2-01", "confidence": 0.82},  # Link Quality Monitor → RSSI threshold
    {"feature_id": "FEAT-002", "element_id": "CE-Pat1-C4-01", "confidence": 0.70},  # Mesh Stack → 64 nodes (partial: supports 128)
    {"feature_id": "FEAT-002", "element_id": "CE-Pat1-C4-02", "confidence": 0.75},  # Mesh Stack → sub-100ms latency
    {"feature_id": "FEAT-003", "element_id": "CE-Pat5-C2-01", "confidence": 0.68},  # Sensor Fusion → EKF (partial match)
    {"feature_id": "FEAT-006", "element_id": "CE-Pat6-C2-01", "confidence": 0.85},  # Edge ML → INT8 quantized CNN
    {"feature_id": "FEAT-006", "element_id": "CE-Pat6-C2-02", "confidence": 0.80},  # Edge ML → MCU deployment
]

PRIOR_ART_FOR: list[dict[str, object]] = [
    # Strong prior art for Family 1 patents (mesh networking well-published before 2021)
    {"pub_id": "PUB-001", "patent_id": "US-11234567", "relevance": 0.88},  # BLE mesh survey
    {"pub_id": "PUB-002", "patent_id": "US-11234567", "relevance": 0.82},  # Adaptive routing
    {"pub_id": "PUB-003", "patent_id": "US-11234567", "relevance": 0.75},  # Duty cycling
    {"pub_id": "PUB-006", "patent_id": "US-11234567", "relevance": 0.90},  # BT 5.0 mesh spec
    {"pub_id": "PUB-006", "patent_id": "US-11234568", "relevance": 0.85},  # BT 5.0 mesh spec → continuation
    {"pub_id": "PUB-001", "patent_id": "EP-3456789", "relevance": 0.87},  # BLE mesh survey → EP family
    {"pub_id": "PUB-002", "patent_id": "EP-3456789", "relevance": 0.80},  # Adaptive routing → EP family
    # Weaker prior art for Family 2 patents (sensor fusion + edge ML less published before 2020)
    {"pub_id": "PUB-004", "patent_id": "US-10987654", "relevance": 0.65},  # Kalman filter survey
    {"pub_id": "PUB-005", "patent_id": "US-10987654", "relevance": 0.55},  # Edge aggregation (published after priority)
    {"pub_id": "PUB-010", "patent_id": "US-10987654", "relevance": 0.50},  # Compressed sensing (tangential)
    # Very weak / no prior art for US-10987655 (edge ML continuation-in-part)
    {"pub_id": "PUB-007", "patent_id": "US-10987655", "relevance": 0.40},  # NN quantization (published AFTER priority)
    # Cross-family tangential references
    {"pub_id": "PUB-003", "patent_id": "US-11234568", "relevance": 0.60},  # Duty cycling → continuation
    {"pub_id": "PUB-004", "patent_id": "JP-2024567890", "relevance": 0.62},  # Kalman → JP family
    {"pub_id": "PUB-008", "patent_id": "US-10987654", "relevance": 0.45},  # TLS challenges (published AFTER priority)
    {"pub_id": "PUB-010", "patent_id": "KR-1023456789", "relevance": 0.48},  # Compressed sensing → KR family
]

NOTEBOOK_CASES: list[dict[str, object]] = [
    {"product_id": "PROD-001", "reason": "full_featured_gateway_evaluated_against_both_patent_families"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_patent_fto"),
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

    _write_csv(
        output_dir / "patents.csv",
        ["patent_id", "title", "assignee", "priority_date", "status", "jurisdiction"],
        PATENTS,
    )
    _write_csv(
        output_dir / "claims.csv",
        ["claim_id", "patent_id", "claim_type", "claim_text", "parent_claim_id", "embedding"],
        CLAIMS,
    )
    _write_csv(
        output_dir / "claim_elements.csv",
        ["element_id", "claim_id", "element_text"],
        CLAIM_ELEMENTS,
    )
    _write_csv(
        output_dir / "products.csv",
        ["product_id", "name", "description"],
        PRODUCTS,
    )
    _write_csv(
        output_dir / "features.csv",
        ["feature_id", "product_id", "name", "description"],
        FEATURES,
    )
    _write_csv(
        output_dir / "publications.csv",
        ["pub_id", "title", "pub_date", "embedding"],
        PUBLICATIONS,
    )
    _write_csv(output_dir / "has_claim.csv", ["patent_id", "claim_id"], HAS_CLAIM)
    _write_csv(output_dir / "depends_on.csv", ["claim_id", "parent_claim_id"], DEPENDS_ON)
    _write_csv(output_dir / "has_element.csv", ["claim_id", "element_id"], HAS_ELEMENT)
    _write_csv(output_dir / "has_feature.csv", ["product_id", "feature_id"], HAS_FEATURE)
    _write_csv(
        output_dir / "reads_on.csv",
        ["feature_id", "element_id", "confidence"],
        READS_ON,
    )
    _write_csv(
        output_dir / "prior_art_for.csv",
        ["pub_id", "patent_id", "relevance"],
        PRIOR_ART_FOR,
    )
    _write_csv(output_dir / "notebook_cases.csv", ["product_id", "reason"], NOTEBOOK_CASES)

    manifest = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "snapshot_date": SNAPSHOT_DATE,
        "source": {
            "description": "Synthetic patent portfolio and product feature data for freedom-to-operate analysis demo. Domain: wireless IoT sensor networks.",
            "urls": SOURCES,
            "license_note": "All data is synthetic and generated for demonstration purposes only. Not legal advice.",
        },
        "shape": {
            "patents": len(PATENTS),
            "claims": len(CLAIMS),
            "claim_elements": len(CLAIM_ELEMENTS),
            "products": len(PRODUCTS),
            "features": len(FEATURES),
            "publications": len(PUBLICATIONS),
            "has_claim": len(HAS_CLAIM),
            "depends_on": len(DEPENDS_ON),
            "has_element": len(HAS_ELEMENT),
            "has_feature": len(HAS_FEATURE),
            "reads_on": len(READS_ON),
            "prior_art_for": len(PRIOR_ART_FOR),
            "notebook_cases": len(NOTEBOOK_CASES),
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {output_dir / 'patents.csv'} ({len(PATENTS)} rows)")
    print(f"wrote {output_dir / 'claims.csv'} ({len(CLAIMS)} rows)")
    print(f"wrote {output_dir / 'claim_elements.csv'} ({len(CLAIM_ELEMENTS)} rows)")
    print(f"wrote {output_dir / 'products.csv'} ({len(PRODUCTS)} rows)")
    print(f"wrote {output_dir / 'features.csv'} ({len(FEATURES)} rows)")
    print(f"wrote {output_dir / 'publications.csv'} ({len(PUBLICATIONS)} rows)")
    print(f"wrote {output_dir / 'has_claim.csv'} ({len(HAS_CLAIM)} rows)")
    print(f"wrote {output_dir / 'depends_on.csv'} ({len(DEPENDS_ON)} rows)")
    print(f"wrote {output_dir / 'has_element.csv'} ({len(HAS_ELEMENT)} rows)")
    print(f"wrote {output_dir / 'has_feature.csv'} ({len(HAS_FEATURE)} rows)")
    print(f"wrote {output_dir / 'reads_on.csv'} ({len(READS_ON)} rows)")
    print(f"wrote {output_dir / 'prior_art_for.csv'} ({len(PRIOR_ART_FOR)} rows)")
    print(f"wrote {output_dir / 'notebook_cases.csv'} ({len(NOTEBOOK_CASES)} rows)")
    print(f"wrote {output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
