pub(crate) fn all_filters_match(
    filters: &[crate::model::SqlExpression],
    context: &EvalContext<'_>,
) -> EvaluatorResult<bool> {
    for filter in filters {
        let value = evaluate_expression(filter, context)?;
        if !sqlite_bool(&value) {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(crate) fn table_matches(
    pattern: &crate::model::SerializedTablePattern,
    table: &SourceTable,
) -> bool {
    if let Some(connection) = &pattern.connection {
        if connection != &table.connection_tag {
            return false;
        }
    }

    if let Some(schema) = &pattern.schema {
        if schema != &table.schema {
            return false;
        }
    }

    if pattern.table.ends_with('%') {
        table.name.starts_with(pattern.table.trim_end_matches('%'))
    } else {
        table.name == pattern.table
    }
}

fn scoped_lookup_values(scope: &ParameterLookupScope, values: &[Value]) -> Vec<Value> {
    let mut result = Vec::with_capacity(values.len() + 2);
    result.push(Value::String(scope.lookup_name.clone()));
    result.push(Value::String(scope.query_id.clone()));
    result.extend(values.to_vec());
    result
}

fn indexed_row_to_vec(row: &JsonMap) -> Vec<Value> {
    let mut parsed: BTreeMap<usize, Value> = BTreeMap::new();
    for (key, value) in row {
        if let Ok(index) = key.parse::<usize>() {
            parsed.insert(index, value.clone());
        }
    }

    parsed.into_values().collect()
}

fn intersection(groups: Vec<Vec<Value>>) -> Vec<Value> {
    if groups.is_empty() {
        return Vec::new();
    }

    let mut current: BTreeSet<String> = groups[0]
        .iter()
        .map(serialize_value_single)
        .collect::<BTreeSet<_>>();

    for values in groups.iter().skip(1) {
        let next = values
            .iter()
            .map(serialize_value_single)
            .collect::<BTreeSet<_>>();
        current = current.intersection(&next).cloned().collect();
        if current.is_empty() {
            break;
        }
    }

    current
        .into_iter()
        .filter_map(|value| serde_json::from_str::<Value>(&value).ok())
        .collect()
}

fn serialize_value_single(value: &Value) -> String {
    serialize_value_array(std::slice::from_ref(value))
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string()
}

fn cartesian_product(values: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    if values.is_empty() {
        return vec![Vec::new()];
    }

    let mut result: Vec<Vec<Value>> = vec![Vec::new()];
    for group in values {
        if group.is_empty() {
            return Vec::new();
        }

        let mut next = Vec::new();
        for prefix in &result {
            for value in &group {
                let mut item = prefix.clone();
                item.push(value.clone());
                next.push(item);
            }
        }
        result = next;
    }

    result
}

pub(crate) struct EvalContext<'a> {
    pub row: Option<&'a JsonMap>,
    pub request: &'a RequestParameters,
}

impl<'a> EvalContext<'a> {
    pub fn request_source(&self, request: &str) -> Value {
        match request {
            "auth" => self.request.auth.clone(),
            "connection" => self.request.connection.clone(),
            "subscription" => self.request.subscription.clone(),
            _ => Value::Null,
        }
    }
}
