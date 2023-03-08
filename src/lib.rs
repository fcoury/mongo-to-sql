use serde_json::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ToSqlError {
    #[error("invalid operand `{0}`")]
    InvalidOperandValue(String),
    #[error("invalid regex `{0}`")]
    InvalidRegexValue(Value),
    #[error("unsupported operator `{0}`")]
    UnsupportedOperator(String),
    #[error("missing operator `{0}`")]
    MissingOperator(String),
    #[error("invalid stage `{0}`")]
    InvalidStage(Value),
}

pub fn match_stage(stage: &serde_json::Value) -> Result<String, ToSqlError> {
    let mut sql = String::new();
    if let Some(stage_obj) = stage.as_object() {
        let op_keys = ["$and", "$or", "$nor"];
        let mut op_values: Vec<&serde_json::Value> = Vec::new();
        for (key, value) in stage_obj.iter() {
            if op_keys.contains(&key.as_str()) {
                if let serde_json::Value::Array(a) = value {
                    op_values = a.iter().collect();
                } else {
                    return Err(ToSqlError::InvalidOperandValue(key.to_string()));
                }
            } else if let serde_json::Value::Object(op) = value {
                if let Some(op_key) = op.keys().next() {
                    let op_value = op.get(op_key).unwrap();
                    match op_key.as_str() {
                        "$gte" => sql.push_str(&format!("{} >= {}", key, op_value)),
                        "$gt" => sql.push_str(&format!("{} > {}", key, op_value)),
                        "$lte" => sql.push_str(&format!("{} <= {}", key, op_value)),
                        "$lt" => sql.push_str(&format!("{} < {}", key, op_value)),
                        "$eq" => sql.push_str(&format!("{} = {}", key, op_value)),
                        "$ne" => sql.push_str(&format!("{} != {}", key, op_value)),
                        "$in" => {
                            let vals = match op_value {
                                serde_json::Value::Array(a) => {
                                    a.iter().map(|v| format!("{}", v)).collect::<Vec<_>>()
                                }
                                _ => vec![format!("{}", op_value)],
                            };
                            sql.push_str(&format!("{} IN ({})", key, vals.join(", ")));
                        }
                        "$nin" => {
                            let vals = match op_value {
                                serde_json::Value::Array(a) => {
                                    a.iter().map(|v| format!("{}", v)).collect::<Vec<_>>()
                                }
                                _ => vec![format!("{}", op_value)],
                            };
                            sql.push_str(&format!("{} NOT IN ({})", key, vals.join(", ")));
                        }
                        "$regex" => sql.push_str(&format!(
                            "{} ~ '{}'",
                            key,
                            op_value
                                .as_str()
                                .ok_or_else(|| ToSqlError::InvalidRegexValue(op_value.clone()))?
                        )),
                        "$options" => {}
                        _ => return Err(ToSqlError::UnsupportedOperator(op_key.to_string())),
                    }
                } else {
                    return Err(ToSqlError::MissingOperator(key.to_string()));
                }
            } else {
                sql.push_str(&format!("{} = {}", key, value));
            }
        }
        if !op_values.is_empty() {
            let sub_sql = op_values
                .iter()
                .map(|sub_stage| match_stage(sub_stage))
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .map(|s| format!("({})", s))
                .collect::<Vec<_>>();
            let sub_sql = sub_sql.join(if stage_obj.contains_key("$and") {
                " AND "
            } else {
                " OR "
            });
            sql.push_str(&format!("({})", sub_sql));
        }
    } else {
        return Err(ToSqlError::InvalidStage(stage.to_owned()));
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_match_stage_with_gte() {
        let stage = json!({ "age": { "$gte": 21 } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "age >= 21");
    }

    #[test]
    fn test_match_stage_with_gt() {
        let stage = json!({ "age": { "$gt": 21 } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "age > 21");
    }

    #[test]
    fn test_match_stage_with_lte() {
        let stage = json!({ "age": { "$lte": 21 } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "age <= 21");
    }

    #[test]
    fn test_match_stage_with_lt() {
        let stage = json!({ "age": { "$lt": 21 } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "age < 21");
    }

    #[test]
    fn test_match_stage_with_eq() {
        let stage = json!({ "name": { "$eq": "John" } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "name = \"John\"");
    }

    #[test]
    fn test_match_stage_with_ne() {
        let stage = json!({ "name": { "$ne": "John" } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "name != \"John\"");
    }

    #[test]
    fn test_match_stage_with_in() {
        let stage = json!({ "status": { "$in": ["active", "pending"] } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "status IN (\"active\", \"pending\")");
    }

    #[test]
    fn test_match_stage_with_nin() {
        let stage = json!({ "status": { "$nin": ["active", "pending"] } });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "status NOT IN (\"active\", \"pending\")");
    }

    #[test]
    fn test_match_stage_with_and() {
        let stage = json!({
            "$and": [
                { "status": "active" },
                { "age": { "$gte": 21 } }
            ]
        });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "((status = \"active\") AND (age >= 21))");
    }

    #[test]
    fn test_match_stage_with_or() {
        let stage = json!({
            "$or": [
                { "status": "active" },
                { "age": { "$gte": 21 } }
            ]
        });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "((status = \"active\") OR (age >= 21))");
    }

    #[test]
    fn test_match_stage_with_regex() {
        let stage = json!({
            "name": {
                "$regex": "^joh?n$",
                "$options": "i"
            }
        });
        let sql = match_stage(&stage).unwrap();
        assert_eq!(sql, "name ~ '^joh?n$'");
    }

    #[test]
    fn test_match_stage_with_unsupported_operator() {
        let stage = json!({ "name": { "$foo": "bar" } });
        let res = match_stage(&stage);
        assert!(res.is_err());
    }
}
