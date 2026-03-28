// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Conversion utilities between Python objects and Rust/Uni types.

use ::uni_db::Value;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::collections::HashMap;
use uni_common::value::TemporalValue;

/// Convert a Uni Value to a Python object.
pub fn value_to_py(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_py_any(py)?),
        Value::Int(i) => Ok(i.into_py_any(py)?),
        Value::Float(f) => Ok(f.into_py_any(py)?),
        Value::String(s) => Ok(s.into_py_any(py)?),
        Value::Bytes(b) => Ok(PyBytes::new(py, b).into()),
        Value::List(l) => {
            let list = PyList::empty(py);
            for item in l {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        Value::Map(m) => {
            let dict = PyDict::new(py);
            for (k, v) in m {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Vector(v) => Ok(v.clone().into_py_any(py)?),
        Value::Node(n) => {
            let dict = PyDict::new(py);
            dict.set_item("_id", n.vid.to_string())?;
            dict.set_item("_labels", &n.labels)?;
            for (k, v) in &n.properties {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Edge(e) => {
            let dict = PyDict::new(py);
            dict.set_item("_id", e.eid.as_u64())?;
            dict.set_item("_type", &e.edge_type)?;
            dict.set_item("_src", e.src.to_string())?;
            dict.set_item("_dst", e.dst.to_string())?;
            for (k, v) in &e.properties {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Path(p) => {
            let dict = PyDict::new(py);
            let nodes = PyList::empty(py);
            for n in p.nodes() {
                nodes.append(value_to_py(py, &Value::Node(n.clone()))?)?;
            }
            dict.set_item("nodes", nodes)?;

            let edges = PyList::empty(py);
            for e in p.edges() {
                edges.append(value_to_py(py, &Value::Edge(e.clone()))?)?;
            }
            dict.set_item("edges", edges)?;
            Ok(dict.into())
        }
        Value::Temporal(tv) => {
            let datetime_module = py.import("datetime")?;
            match tv {
                TemporalValue::Date { days_since_epoch } => {
                    let date_class = datetime_module.getattr("date")?;
                    let epoch_ordinal: i64 = 719163; // date(1970,1,1).toordinal()
                    let result = date_class
                        .call_method1("fromordinal", (epoch_ordinal + *days_since_epoch as i64,))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::LocalTime {
                    nanos_since_midnight,
                } => {
                    let total_micros = nanos_since_midnight / 1_000;
                    let hour = total_micros / 3_600_000_000;
                    let minute = (total_micros % 3_600_000_000) / 60_000_000;
                    let second = (total_micros % 60_000_000) / 1_000_000;
                    let microsecond = total_micros % 1_000_000;
                    let time_class = datetime_module.getattr("time")?;
                    let result = time_class.call1((hour, minute, second, microsecond))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::Time {
                    nanos_since_midnight,
                    offset_seconds,
                } => {
                    let total_micros = nanos_since_midnight / 1_000;
                    let hour = total_micros / 3_600_000_000;
                    let minute = (total_micros % 3_600_000_000) / 60_000_000;
                    let second = (total_micros % 60_000_000) / 1_000_000;
                    let microsecond = total_micros % 1_000_000;
                    let tz_class = datetime_module.getattr("timezone")?;
                    let td_class = datetime_module.getattr("timedelta")?;
                    let td = td_class.call1(pyo3::types::PyTuple::new(
                        py,
                        &[
                            0i32.into_pyobject(py)?.into_any(),
                            offset_seconds.into_pyobject(py)?.into_any(),
                        ],
                    )?)?;
                    let tz = tz_class.call1((td,))?;
                    let time_class = datetime_module.getattr("time")?;
                    let result = time_class.call1((hour, minute, second, microsecond, tz))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::LocalDateTime { nanos_since_epoch } => {
                    let secs = nanos_since_epoch / 1_000_000_000;
                    let micros = (nanos_since_epoch % 1_000_000_000) / 1_000;
                    let dt_class = datetime_module.getattr("datetime")?;
                    let result = dt_class.call_method1(
                        "fromtimestamp",
                        (secs as f64 + micros as f64 / 1_000_000.0,),
                    )?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::DateTime {
                    nanos_since_epoch,
                    offset_seconds,
                    ..
                } => {
                    let secs = nanos_since_epoch / 1_000_000_000;
                    let micros = (nanos_since_epoch % 1_000_000_000) / 1_000;
                    let tz_class = datetime_module.getattr("timezone")?;
                    let td_class = datetime_module.getattr("timedelta")?;
                    let td = td_class.call1(pyo3::types::PyTuple::new(
                        py,
                        &[
                            0i32.into_pyobject(py)?.into_any(),
                            offset_seconds.into_pyobject(py)?.into_any(),
                        ],
                    )?)?;
                    let tz = tz_class.call1((td,))?;
                    let dt_class = datetime_module.getattr("datetime")?;
                    let result = dt_class.call_method1(
                        "fromtimestamp",
                        (secs as f64 + micros as f64 / 1_000_000.0, tz),
                    )?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::Duration {
                    months,
                    days,
                    nanos,
                } => {
                    let total_days = months * 30 + days;
                    let total_secs = nanos / 1_000_000_000;
                    let remaining_micros = (nanos % 1_000_000_000) / 1_000;
                    let td_class = datetime_module.getattr("timedelta")?;
                    let result = td_class.call1((total_days, total_secs, remaining_micros))?;
                    Ok(result.into_py_any(py)?)
                }
            }
        }
        _ => Ok(py.None()),
    }
}

/// Convert a Python object to a serde_json::Value.
pub fn py_object_to_json(py: Python, obj: &Py<PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none(py) {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(b) = obj.extract::<bool>(py) {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>(py) {
        return Ok(serde_json::json!(i));
    }
    if let Ok(f) = obj.extract::<f64>(py) {
        return Ok(serde_json::json!(f));
    }
    if let Ok(s) = obj.extract::<String>(py) {
        return Ok(serde_json::Value::String(s));
    }

    let bound = obj.bind(py);
    if let Ok(l) = bound.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in l {
            vec.push(py_object_to_json(py, &item.into())?);
        }
        return Ok(serde_json::Value::Array(vec));
    }

    if let Ok(d) = bound.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in d {
            let key = k.extract::<String>()?;
            let val = py_object_to_json(py, &v.into())?;
            map.insert(key, val);
        }
        return Ok(serde_json::Value::Object(map));
    }

    Ok(serde_json::Value::Null)
}

/// Convert a serde_json::Value to a Python object.
pub fn json_value_to_py(py: Python, val: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match val {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py).unwrap().into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py).unwrap().into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py).unwrap().into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
    }
}

/// Convert a Python object to a Uni Value.
pub fn py_object_to_value(py: Python, obj: &Py<PyAny>) -> PyResult<Value> {
    if obj.is_none(py) {
        return Ok(Value::Null);
    }

    let bound = obj.bind(py);

    // Check for datetime types FIRST (before int/float extraction)
    // datetime is a subclass of date, so check datetime first
    let datetime_module = py.import("datetime")?;
    let datetime_class = datetime_module.getattr("datetime")?;
    let date_class = datetime_module.getattr("date")?;
    let time_class = datetime_module.getattr("time")?;

    if bound.is_instance(&datetime_class)? {
        let timestamp_secs: f64 = bound.call_method0("timestamp")?.extract()?;
        let nanos = (timestamp_secs * 1_000_000_000.0) as i64;
        let tzinfo = bound.getattr("tzinfo")?;
        if tzinfo.is_none() {
            return Ok(Value::Temporal(TemporalValue::LocalDateTime {
                nanos_since_epoch: nanos,
            }));
        } else {
            let utcoffset = bound.call_method0("utcoffset")?;
            let offset_seconds: i32 =
                utcoffset.call_method0("total_seconds")?.extract::<f64>()? as i32;
            let tz_name: Option<String> =
                bound.call_method0("tzname")?.extract::<Option<String>>()?;
            return Ok(Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: nanos,
                offset_seconds,
                timezone_name: tz_name,
            }));
        }
    }

    if bound.is_instance(&date_class)? {
        // Convert to days since epoch using Python's own toordinal
        let ordinal: i64 = bound.call_method0("toordinal")?.extract()?;
        let epoch_ordinal: i64 = 719163; // date(1970,1,1).toordinal()
        let days = (ordinal - epoch_ordinal) as i32;
        return Ok(Value::Temporal(TemporalValue::Date {
            days_since_epoch: days,
        }));
    }

    if bound.is_instance(&time_class)? {
        let hour: i64 = bound.getattr("hour")?.extract()?;
        let minute: i64 = bound.getattr("minute")?.extract()?;
        let second: i64 = bound.getattr("second")?.extract()?;
        let microsecond: i64 = bound.getattr("microsecond")?.extract()?;
        let nanos = hour * 3_600_000_000_000
            + minute * 60_000_000_000
            + second * 1_000_000_000
            + microsecond * 1_000;
        let tzinfo = bound.getattr("tzinfo")?;
        if tzinfo.is_none() {
            return Ok(Value::Temporal(TemporalValue::LocalTime {
                nanos_since_midnight: nanos,
            }));
        } else {
            let utcoffset = bound.call_method1("utcoffset", (py.None(),))?;
            let offset_seconds: i32 =
                utcoffset.call_method0("total_seconds")?.extract::<f64>()? as i32;
            return Ok(Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: nanos,
                offset_seconds,
            }));
        }
    }

    // Check primitive types in order of specificity
    if let Ok(b) = obj.extract::<bool>(py) {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>(py) {
        return Ok(Value::Int(i));
    }
    if let Ok(f) = obj.extract::<f64>(py) {
        return Ok(Value::Float(f));
    }
    if let Ok(s) = obj.extract::<String>(py) {
        return Ok(Value::String(s));
    }

    if let Ok(l) = bound.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in l {
            vec.push(py_object_to_value(py, &item.into())?);
        }
        return Ok(Value::List(vec));
    }

    if let Ok(d) = bound.cast::<PyDict>() {
        let mut map = HashMap::new();
        for (k, v) in d {
            let key = k.extract::<String>()?;
            let val = py_object_to_value(py, &v.into())?;
            map.insert(key, val);
        }
        return Ok(Value::Map(map));
    }

    Ok(Value::Null)
}

/// Convert Python params dict to Rust params.
pub fn prepare_params(
    py: Python,
    params: Option<HashMap<String, Py<PyAny>>>,
) -> PyResult<HashMap<String, Value>> {
    let mut rust_params = HashMap::new();
    if let Some(p) = params {
        for (k, v) in p {
            let val = py_object_to_value(py, &v)?;
            rust_params.insert(k, val);
        }
    }
    Ok(rust_params)
}

/// Convert query result rows to Python dicts.
pub fn rows_to_py(py: Python, rows: Vec<::uni_db::Row>) -> PyResult<Vec<Py<PyAny>>> {
    let mut result = Vec::new();
    for row in rows {
        let dict = PyDict::new(py);
        for (col_name, val) in row.as_map() {
            dict.set_item(col_name, value_to_py(py, val)?)?;
        }
        result.push(dict.into());
    }
    Ok(result)
}

/// Convert Locy rows (HashMap<String, Value>) to a Python list of dicts.
fn locy_rows_to_py(py: Python, rows: Vec<HashMap<String, Value>>) -> PyResult<Vec<Py<PyAny>>> {
    let mut result = Vec::new();
    for row in rows {
        let dict = PyDict::new(py);
        for (col_name, val) in row {
            dict.set_item(&col_name, value_to_py(py, &val)?)?;
        }
        result.push(dict.into());
    }
    Ok(result)
}

/// Convert a Locy DerivationNode to a Python dict.
fn derivation_node_to_py(py: Python, node: uni_locy::DerivationNode) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("rule", &node.rule)?;
    dict.set_item("clause_index", node.clause_index)?;
    dict.set_item("priority", node.priority)?;

    let bindings_dict = PyDict::new(py);
    for (k, v) in node.bindings {
        bindings_dict.set_item(&k, value_to_py(py, &v)?)?;
    }
    dict.set_item("bindings", bindings_dict)?;

    let along_dict = PyDict::new(py);
    for (k, v) in node.along_values {
        along_dict.set_item(&k, value_to_py(py, &v)?)?;
    }
    dict.set_item("along_values", along_dict)?;

    let children = PyList::empty(py);
    for child in node.children {
        children.append(derivation_node_to_py(py, child)?)?;
    }
    dict.set_item("children", children)?;
    dict.set_item("graph_fact", node.graph_fact)?;
    dict.set_item("proof_probability", node.proof_probability)?;
    Ok(dict.into())
}

/// Convert a Locy Modification to a Python dict.
fn modification_to_py(py: Python, m: uni_locy::Modification) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    match m {
        uni_locy::Modification::RemoveEdge {
            source_var,
            target_var,
            edge_var,
            edge_type,
            match_properties,
        } => {
            dict.set_item("type", "remove_edge")?;
            dict.set_item("source_var", source_var)?;
            dict.set_item("target_var", target_var)?;
            dict.set_item("edge_var", edge_var)?;
            dict.set_item("edge_type", edge_type)?;
            let props_dict = PyDict::new(py);
            for (k, v) in match_properties {
                props_dict.set_item(&k, value_to_py(py, &v)?)?;
            }
            dict.set_item("match_properties", props_dict)?;
        }
        uni_locy::Modification::ChangeProperty {
            element_var,
            property,
            old_value,
            new_value,
        } => {
            dict.set_item("type", "change_property")?;
            dict.set_item("element_var", element_var)?;
            dict.set_item("property", property)?;
            dict.set_item("old_value", value_to_py(py, &old_value)?)?;
            dict.set_item("new_value", value_to_py(py, &new_value)?)?;
        }
        uni_locy::Modification::AddEdge {
            source_var,
            target_var,
            edge_type,
            properties,
        } => {
            dict.set_item("type", "add_edge")?;
            dict.set_item("source_var", source_var)?;
            dict.set_item("target_var", target_var)?;
            dict.set_item("edge_type", edge_type)?;
            let props_dict = PyDict::new(py);
            for (k, v) in properties {
                props_dict.set_item(&k, value_to_py(py, &v)?)?;
            }
            dict.set_item("properties", props_dict)?;
        }
    }
    Ok(dict.into())
}

/// Convert a Locy CommandResult to a Python dict.
fn command_result_to_py(py: Python, cmd: uni_locy::CommandResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    match cmd {
        uni_locy::CommandResult::Query(rows) => {
            dict.set_item("type", "query")?;
            dict.set_item("rows", locy_rows_to_py(py, rows)?)?;
        }
        uni_locy::CommandResult::Assume(rows) => {
            dict.set_item("type", "assume")?;
            dict.set_item("rows", locy_rows_to_py(py, rows)?)?;
        }
        uni_locy::CommandResult::Explain(node) => {
            dict.set_item("type", "explain")?;
            dict.set_item("tree", derivation_node_to_py(py, node)?)?;
        }
        uni_locy::CommandResult::Abduce(result) => {
            dict.set_item("type", "abduce")?;
            let mods = PyList::empty(py);
            for vm in result.modifications {
                let mod_dict = PyDict::new(py);
                mod_dict.set_item("modification", modification_to_py(py, vm.modification)?)?;
                mod_dict.set_item("validated", vm.validated)?;
                mod_dict.set_item("cost", vm.cost)?;
                mods.append(mod_dict)?;
            }
            dict.set_item("modifications", mods)?;
        }
        uni_locy::CommandResult::Derive { affected } => {
            dict.set_item("type", "derive")?;
            dict.set_item("affected", affected)?;
        }
        uni_locy::CommandResult::Cypher(rows) => {
            dict.set_item("type", "cypher")?;
            dict.set_item("rows", locy_rows_to_py(py, rows)?)?;
        }
    }
    Ok(dict.into())
}

/// Extract a LocyConfig from a Python config dict.
pub fn extract_locy_config(
    py: Python,
    config: HashMap<String, Py<PyAny>>,
) -> PyResult<::uni_db::locy::LocyConfig> {
    let mut locy_config = ::uni_db::locy::LocyConfig::default();
    if let Some(v) = config.get("max_iterations") {
        locy_config.max_iterations = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("timeout") {
        locy_config.timeout = std::time::Duration::from_secs_f64(v.extract::<f64>(py)?);
    }
    if let Some(v) = config.get("max_explain_depth") {
        locy_config.max_explain_depth = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_slg_depth") {
        locy_config.max_slg_depth = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_abduce_candidates") {
        locy_config.max_abduce_candidates = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_abduce_results") {
        locy_config.max_abduce_results = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_derived_bytes") {
        locy_config.max_derived_bytes = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("deterministic_best_by") {
        locy_config.deterministic_best_by = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("strict_probability_domain") {
        locy_config.strict_probability_domain = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("probability_epsilon") {
        locy_config.probability_epsilon = v.extract::<f64>(py)?;
    }
    if let Some(v) = config.get("exact_probability") {
        locy_config.exact_probability = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("max_bdd_variables") {
        locy_config.max_bdd_variables = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("top_k_proofs") {
        locy_config.top_k_proofs = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("top_k_proofs_training") {
        locy_config.top_k_proofs_training = Some(v.extract::<usize>(py)?);
    }
    if let Some(v) = config.get("params") {
        let params_map = v.extract::<HashMap<String, Py<PyAny>>>(py)?;
        locy_config.params = prepare_params(py, Some(params_map))?;
    }
    Ok(locy_config)
}

/// Convert a LocyResult to a Python dict.
pub fn locy_result_to_py(py: Python, result: uni_db::locy::LocyResult) -> PyResult<Py<PyAny>> {
    let result = result.into_inner();
    let dict = PyDict::new(py);

    // derived: HashMap<String, Vec<Row>> -> Python dict of lists of dicts
    let derived_dict = PyDict::new(py);
    for (rule_name, rows) in result.derived {
        derived_dict.set_item(&rule_name, locy_rows_to_py(py, rows)?)?;
    }
    dict.set_item("derived", derived_dict)?;

    // stats
    let stats = crate::types::LocyStats {
        strata_evaluated: result.stats.strata_evaluated,
        total_iterations: result.stats.total_iterations,
        derived_nodes: result.stats.derived_nodes,
        derived_edges: result.stats.derived_edges,
        evaluation_time_secs: result.stats.evaluation_time.as_secs_f64(),
        queries_executed: result.stats.queries_executed,
        mutations_executed: result.stats.mutations_executed,
        peak_memory_bytes: result.stats.peak_memory_bytes,
    };
    dict.set_item("stats", stats.into_py_any(py)?)?;

    // command_results
    let cmd_list = PyList::empty(py);
    for cmd in result.command_results {
        cmd_list.append(command_result_to_py(py, cmd)?)?;
    }
    dict.set_item("command_results", cmd_list)?;

    // warnings: Vec<RuntimeWarning> -> list of dicts
    let warn_list = PyList::empty(py);
    for w in result.warnings {
        let wd = PyDict::new(py);
        let code_str = match w.code {
            uni_locy::RuntimeWarningCode::SharedProbabilisticDependency => {
                "shared_probabilistic_dependency"
            }
            uni_locy::RuntimeWarningCode::BddLimitExceeded => "bdd_limit_exceeded",
            uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact => {
                "cross_group_correlation_not_exact"
            }
        };
        wd.set_item("code", code_str)?;
        wd.set_item("message", &w.message)?;
        wd.set_item("rule_name", &w.rule_name)?;
        match w.variable_count {
            Some(n) => wd.set_item("variable_count", n)?,
            None => wd.set_item("variable_count", py.None())?,
        }
        match w.key_group {
            Some(ref g) => wd.set_item("key_group", g)?,
            None => wd.set_item("key_group", py.None())?,
        }
        warn_list.append(wd)?;
    }
    dict.set_item("warnings", warn_list)?;

    // approximate_groups: HashMap<String, Vec<String>> -> Python dict of lists
    let approx_dict = PyDict::new(py);
    for (rule_name, groups) in result.approximate_groups {
        let group_list = PyList::new(py, groups.iter().map(|s| s.as_str()))?;
        approx_dict.set_item(&rule_name, group_list)?;
    }
    dict.set_item("approximate_groups", approx_dict)?;

    Ok(dict.into())
}

/// Convert QueryMetrics to a Python dict.
pub fn query_metrics_to_py(py: Python, m: &::uni_db::QueryMetrics) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("parse_time_ms", m.parse_time.as_secs_f64() * 1000.0)?;
    dict.set_item("plan_time_ms", m.plan_time.as_secs_f64() * 1000.0)?;
    dict.set_item("exec_time_ms", m.exec_time.as_secs_f64() * 1000.0)?;
    dict.set_item("total_time_ms", m.total_time.as_secs_f64() * 1000.0)?;
    dict.set_item("rows_returned", m.rows_returned)?;
    dict.set_item("rows_scanned", m.rows_scanned)?;
    dict.set_item("bytes_read", m.bytes_read)?;
    dict.set_item("plan_cache_hit", m.plan_cache_hit)?;
    dict.set_item("l0_reads", m.l0_reads)?;
    dict.set_item("storage_reads", m.storage_reads)?;
    dict.set_item("cache_hits", m.cache_hits)?;
    Ok(dict.into())
}

/// Convert an AutoCommitResult to a Python AutoCommitResult.
pub fn auto_commit_result_to_py(
    py: Python,
    r: ::uni_db::AutoCommitResult,
) -> PyResult<crate::types::PyAutoCommitResult> {
    Ok(crate::types::PyAutoCommitResult {
        affected_rows: r.affected_rows(),
        nodes_created: r.nodes_created,
        nodes_deleted: r.nodes_deleted,
        relationships_created: r.relationships_created,
        relationships_deleted: r.relationships_deleted,
        properties_set: r.properties_set,
        properties_removed: r.properties_removed,
        labels_added: r.labels_added,
        labels_removed: r.labels_removed,
        version: r.version,
        metrics: query_metrics_to_py(py, &r.metrics)?,
    })
}

/// Convert an ExecuteResult to a Python ExecuteResult.
pub fn execute_result_to_py(
    py: Python,
    r: ::uni_db::query_crate::ExecuteResult,
) -> PyResult<crate::types::PyExecuteResult> {
    Ok(crate::types::PyExecuteResult {
        affected_rows: r.affected_rows(),
        nodes_created: r.nodes_created(),
        nodes_deleted: r.nodes_deleted(),
        relationships_created: r.relationships_created(),
        relationships_deleted: r.relationships_deleted(),
        properties_set: r.properties_set(),
        labels_added: r.labels_added(),
        labels_removed: r.labels_removed(),
        metrics: query_metrics_to_py(py, r.metrics())?,
    })
}

/// Convert a LocyResult to a Python LocyResult class instance.
pub fn locy_result_to_py_class(
    py: Python,
    result: uni_db::locy::LocyResult,
) -> PyResult<crate::types::PyLocyResult> {
    let result = result.into_inner();
    // Reuse the existing dict-based conversion for the inner fields
    let derived_dict = pyo3::types::PyDict::new(py);
    for (rule_name, rows) in result.derived {
        derived_dict.set_item(&rule_name, locy_rows_to_py(py, rows)?)?;
    }

    let stats = crate::types::LocyStats {
        strata_evaluated: result.stats.strata_evaluated,
        total_iterations: result.stats.total_iterations,
        derived_nodes: result.stats.derived_nodes,
        derived_edges: result.stats.derived_edges,
        evaluation_time_secs: result.stats.evaluation_time.as_secs_f64(),
        queries_executed: result.stats.queries_executed,
        mutations_executed: result.stats.mutations_executed,
        peak_memory_bytes: result.stats.peak_memory_bytes,
    };

    let cmd_list = pyo3::types::PyList::empty(py);
    for cmd in result.command_results {
        cmd_list.append(command_result_to_py(py, cmd)?)?;
    }

    let warn_list = pyo3::types::PyList::empty(py);
    for w in result.warnings {
        let wd = pyo3::types::PyDict::new(py);
        let code_str = match w.code {
            uni_locy::RuntimeWarningCode::SharedProbabilisticDependency => {
                "shared_probabilistic_dependency"
            }
            uni_locy::RuntimeWarningCode::BddLimitExceeded => "bdd_limit_exceeded",
            uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact => {
                "cross_group_correlation_not_exact"
            }
        };
        wd.set_item("code", code_str)?;
        wd.set_item("message", &w.message)?;
        wd.set_item("rule_name", &w.rule_name)?;
        match w.variable_count {
            Some(n) => wd.set_item("variable_count", n)?,
            None => wd.set_item("variable_count", py.None())?,
        }
        match w.key_group {
            Some(ref g) => wd.set_item("key_group", g)?,
            None => wd.set_item("key_group", py.None())?,
        }
        warn_list.append(wd)?;
    }

    let approx_dict = pyo3::types::PyDict::new(py);
    for (rule_name, groups) in result.approximate_groups {
        let group_list = pyo3::types::PyList::new(py, groups.iter().map(|s| s.as_str()))?;
        approx_dict.set_item(&rule_name, group_list)?;
    }

    // Wrap the derived fact set in the opaque PyDerivedFactSet type
    let derived_fact_set: Py<pyo3::PyAny> = match result.derived_fact_set {
        Some(dfs) => {
            let py_dfs = crate::types::PyDerivedFactSet { inner: Some(dfs) };
            py_dfs.into_py_any(py)?
        }
        None => py.None(),
    };

    Ok(crate::types::PyLocyResult {
        derived: derived_dict.into(),
        stats: stats.into_py_any(py)?,
        command_results: cmd_list.into(),
        warnings: warn_list.into(),
        approximate_groups: approx_dict.into(),
        derived_fact_set,
    })
}

/// Convert ExplainOutput to a Python dict.
pub fn explain_output_to_py(
    py: Python,
    output: ::uni_db::ExplainOutput,
) -> PyResult<Py<pyo3::PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("plan_text", &output.plan_text)?;
    dict.set_item("warnings", &output.warnings)?;

    let cost_dict = PyDict::new(py);
    cost_dict.set_item("estimated_rows", output.cost_estimates.estimated_rows)?;
    cost_dict.set_item("estimated_cost", output.cost_estimates.estimated_cost)?;
    dict.set_item("cost_estimates", cost_dict)?;

    let index_usage = PyList::empty(py);
    for usage in &output.index_usage {
        let usage_dict = PyDict::new(py);
        usage_dict.set_item("label_or_type", &usage.label_or_type)?;
        usage_dict.set_item("property", &usage.property)?;
        usage_dict.set_item("index_type", &usage.index_type)?;
        usage_dict.set_item("used", usage.used)?;
        if let Some(reason) = &usage.reason {
            usage_dict.set_item("reason", reason)?;
        }
        index_usage.append(usage_dict)?;
    }
    dict.set_item("index_usage", index_usage)?;

    let suggestions = PyList::empty(py);
    for suggestion in &output.suggestions {
        let sug_dict = PyDict::new(py);
        sug_dict.set_item("label_or_type", &suggestion.label_or_type)?;
        sug_dict.set_item("property", &suggestion.property)?;
        sug_dict.set_item("index_type", &suggestion.index_type)?;
        sug_dict.set_item("reason", &suggestion.reason)?;
        sug_dict.set_item("create_statement", &suggestion.create_statement)?;
        suggestions.append(sug_dict)?;
    }
    dict.set_item("suggestions", suggestions)?;

    Ok(dict.into())
}

/// Convert ProfileOutput to a Python dict.
pub fn profile_output_to_py(
    py: Python,
    profile: ::uni_db::ProfileOutput,
) -> PyResult<Py<pyo3::PyAny>> {
    let profile_dict = PyDict::new(py);
    profile_dict.set_item("total_time_ms", profile.total_time_ms)?;
    profile_dict.set_item("peak_memory_bytes", profile.peak_memory_bytes)?;
    profile_dict.set_item("plan_text", &profile.explain.plan_text)?;

    let ops = PyList::empty(py);
    for op in &profile.runtime_stats {
        let op_dict = PyDict::new(py);
        op_dict.set_item("operator", &op.operator)?;
        op_dict.set_item("actual_rows", op.actual_rows)?;
        op_dict.set_item("time_ms", op.time_ms)?;
        op_dict.set_item("memory_bytes", op.memory_bytes)?;
        if let Some(hits) = op.index_hits {
            op_dict.set_item("index_hits", hits)?;
        }
        if let Some(misses) = op.index_misses {
            op_dict.set_item("index_misses", misses)?;
        }
        ops.append(op_dict)?;
    }
    profile_dict.set_item("operators", ops)?;

    Ok(profile_dict.into())
}

/// Extract a CloudStorageConfig from a Python dict.
///
/// The dict must have a `"provider"` key: `"s3"`, `"gcs"`, or `"azure"`.
pub fn extract_cloud_config(
    py: Python,
    config: &HashMap<String, Py<PyAny>>,
) -> PyResult<uni_common::CloudStorageConfig> {
    let provider = config
        .get("provider")
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "cloud_config must contain a 'provider' key",
            )
        })?
        .extract::<String>(py)?;

    match provider.to_lowercase().as_str() {
        "s3" => {
            let bucket = config
                .get("bucket")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("S3 config requires 'bucket'")
                })?
                .extract::<String>(py)?;
            let region = config
                .get("region")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let endpoint = config
                .get("endpoint")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let access_key_id = config
                .get("access_key_id")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let secret_access_key = config
                .get("secret_access_key")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let session_token = config
                .get("session_token")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let virtual_hosted_style = config
                .get("virtual_hosted_style")
                .map(|v| v.extract::<bool>(py))
                .transpose()?
                .unwrap_or(false);
            Ok(uni_common::CloudStorageConfig::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                session_token,
                virtual_hosted_style,
            })
        }
        "gcs" => {
            let bucket = config
                .get("bucket")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("GCS config requires 'bucket'")
                })?
                .extract::<String>(py)?;
            let service_account_path = config
                .get("service_account_path")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let service_account_key = config
                .get("service_account_key")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            Ok(uni_common::CloudStorageConfig::Gcs {
                bucket,
                service_account_path,
                service_account_key,
            })
        }
        "azure" => {
            let container = config
                .get("container")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Azure config requires 'container'",
                    )
                })?
                .extract::<String>(py)?;
            let account = config
                .get("account")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Azure config requires 'account'",
                    )
                })?
                .extract::<String>(py)?;
            let access_key = config
                .get("access_key")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let sas_token = config
                .get("sas_token")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            Ok(uni_common::CloudStorageConfig::Azure {
                container,
                account,
                access_key,
                sas_token,
            })
        }
        other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unknown cloud provider '{}'. Expected 's3', 'gcs', or 'azure'.",
            other
        ))),
    }
}

/// Extract a UniConfig from a Python dict.
///
/// Supports: `query_timeout` (float, seconds), `max_query_memory` (int, bytes),
/// `parallelism` (int), `cache_size` (int, bytes).
pub fn extract_uni_config(
    py: Python,
    config: &HashMap<String, Py<PyAny>>,
) -> PyResult<uni_common::UniConfig> {
    let mut uni_config = uni_common::UniConfig::default();
    if let Some(v) = config.get("query_timeout") {
        uni_config.query_timeout = std::time::Duration::from_secs_f64(v.extract::<f64>(py)?);
    }
    if let Some(v) = config.get("max_query_memory") {
        uni_config.max_query_memory = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("parallelism") {
        uni_config.parallelism = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("cache_size") {
        uni_config.cache_size = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_transaction_memory") {
        uni_config.max_transaction_memory = v.extract::<usize>(py)?;
    }
    Ok(uni_config)
}

/// Convert a SnapshotManifest to a Python SnapshotInfo object.
pub fn snapshot_manifest_to_py(
    _py: Python,
    manifest: uni_common::core::snapshot::SnapshotManifest,
) -> PyResult<crate::types::SnapshotInfo> {
    Ok(crate::types::SnapshotInfo {
        snapshot_id: manifest.snapshot_id,
        name: manifest.name,
        created_at: manifest.created_at.to_rfc3339(),
        version_hwm: manifest.version_high_water_mark,
    })
}

/// Convert an IndexRebuildTask to a Python IndexRebuildTaskInfo object.
pub fn index_rebuild_task_to_py(
    _py: Python,
    task: uni_store::storage::IndexRebuildTask,
) -> PyResult<crate::types::IndexRebuildTaskInfo> {
    let status = format!("{:?}", task.status).to_lowercase();
    Ok(crate::types::IndexRebuildTaskInfo {
        id: task.id,
        label: task.label,
        status,
        created_at: task.created_at.to_rfc3339(),
        started_at: task.started_at.map(|t| t.to_rfc3339()),
        completed_at: task.completed_at.map(|t| t.to_rfc3339()),
        error: task.error,
        retry_count: task.retry_count,
    })
}

/// Convert an IndexDefinition to a Python IndexDefinitionInfo object.
pub fn index_definition_to_py(
    _py: Python,
    idx: uni_common::core::schema::IndexDefinition,
) -> PyResult<crate::types::IndexDefinitionInfo> {
    match idx {
        uni_common::core::schema::IndexDefinition::Scalar(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: format!("{:?}", cfg.index_type).to_lowercase(),
                label: cfg.label,
                properties: cfg.properties,
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::Vector(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "vector".to_string(),
                label: cfg.label,
                properties: vec![cfg.property],
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::FullText(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "fulltext".to_string(),
                label: cfg.label,
                properties: cfg.properties,
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::Inverted(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "inverted".to_string(),
                label: cfg.label,
                properties: vec![cfg.property],
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::JsonFullText(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "json_fulltext".to_string(),
                label: cfg.label,
                properties: vec![cfg.column],
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Unknown index definition type",
        )),
    }
}

/// Extract a list of (role, content) pairs from Python objects.
///
/// Each element may be a `Message` instance or a dict with `"role"` and `"content"` keys.
pub fn extract_messages(py: Python, messages: Vec<Py<PyAny>>) -> PyResult<Vec<(String, String)>> {
    messages
        .into_iter()
        .enumerate()
        .map(|(i, obj)| {
            let bound = obj.bind(py);
            // Try as PyMessage instance first
            if let Ok(msg) = bound.extract::<crate::types::PyMessage>() {
                return Ok((msg.role, msg.content));
            }
            // Try as dict
            if let Ok(dict) = bound.cast::<pyo3::types::PyDict>() {
                let role: String = dict
                    .get_item("role")?
                    .ok_or_else(|| {
                        pyo3::exceptions::PyTypeError::new_err(format!(
                            "messages[{}]: dict missing 'role' key",
                            i
                        ))
                    })?
                    .extract()?;
                let content: String = dict
                    .get_item("content")?
                    .ok_or_else(|| {
                        pyo3::exceptions::PyTypeError::new_err(format!(
                            "messages[{}]: dict missing 'content' key",
                            i
                        ))
                    })?
                    .extract()?;
                return Ok((role, content));
            }
            Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "messages[{}]: expected Message or dict, got {}",
                i,
                bound.get_type().name()?
            )))
        })
        .collect()
}

/// Convert a GenerationResult to Python types.
pub fn generation_result_to_py(
    py: Python,
    result: ::uni_db::api::xervo::GenerationResult,
) -> PyResult<crate::types::PyGenerationResult> {
    let usage = result
        .usage
        .map(|u| {
            let tu = crate::types::PyTokenUsage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
                total_tokens: u.total_tokens,
            };
            Py::new(py, tu)
        })
        .transpose()?;
    Ok(crate::types::PyGenerationResult {
        text: result.text,
        usage,
    })
}

/// Convert a `DatabaseMetrics` to a Python `PyDatabaseMetrics`.
pub fn database_metrics_to_py(
    _py: Python,
    m: ::uni_db::DatabaseMetrics,
) -> PyResult<crate::types::PyDatabaseMetrics> {
    Ok(crate::types::PyDatabaseMetrics {
        l0_mutation_count: m.l0_mutation_count,
        l0_estimated_size_bytes: m.l0_estimated_size_bytes,
        schema_version: m.schema_version,
        uptime_secs: m.uptime.as_secs_f64(),
        active_sessions: m.active_sessions,
        l1_run_count: m.l1_run_count,
        write_throttle_pressure: m.write_throttle_pressure,
        compaction_status: m.compaction_status,
        wal_size_bytes: m.wal_size_bytes,
        wal_lsn: m.wal_lsn,
        total_queries: m.total_queries,
        total_commits: m.total_commits,
    })
}

/// Convert a `&UniConfig` to a Python dict.
pub fn uni_config_to_py(py: Python, config: &uni_common::UniConfig) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("cache_size", config.cache_size)?;
    dict.set_item("parallelism", config.parallelism)?;
    dict.set_item("query_timeout", config.query_timeout.as_secs_f64())?;
    dict.set_item("max_query_memory", config.max_query_memory)?;
    dict.set_item("max_transaction_memory", config.max_transaction_memory)?;
    dict.set_item("batch_size", config.batch_size)?;
    dict.set_item("auto_flush_threshold", config.auto_flush_threshold)?;
    dict.set_item(
        "auto_flush_interval",
        config.auto_flush_interval.map(|d| d.as_secs_f64()),
    )?;
    dict.set_item("wal_enabled", config.wal_enabled)?;
    dict.set_item(
        "max_recursive_cte_iterations",
        config.max_recursive_cte_iterations,
    )?;
    Ok(dict.into())
}
