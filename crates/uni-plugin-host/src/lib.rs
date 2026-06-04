// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! # uni-plugin-host
//!
//! Host-side runtime for the uni-db plugin framework. This crate holds the
//! reusable *engine* implementations that the `uni-db` API crate wires into
//! `Uni`/`Session`/`Transaction`:
//!
//! - trigger dispatch + mutation-event extraction (`triggers`)
//! - change-data-capture runtime (`cdc_runtime`)
//! - background-job scheduler (`scheduler`, `scheduler_persistence`)
//! - meta-plugin system-label persistence (`persistence`)
//! - synthetic declared-procedure host (`synthetic_procedure`)
//! - commit notifications + session hooks (`notifications`, `hooks`)
//! - OpenTelemetry layer (`observability`)
//!
//! It sits above the leaf `uni-plugin` trait crate and below `uni-db`. Logic
//! that genuinely needs the `Uni` lifecycle is inverted behind the
//! [`host::HostCypherExecutor`] trait, which `uni-db` implements.

pub mod cdc_runtime;
pub mod commit_result;
pub mod hooks;
pub mod host;
pub mod http_egress;
pub mod notifications;
pub mod observability;
pub mod persistence;
pub mod scheduler;
pub mod scheduler_persistence;
pub mod shutdown;
pub mod synthetic_procedure;
pub mod triggers;
