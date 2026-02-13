impl SyncPlanEvaluator {
    pub fn evaluate_row(&self, options: EvaluateRowOptions) -> EvaluatorResult<Vec<EvaluatedRow>> {
        let mut results = Vec::new();

        for bucket in &self.plan.buckets {
            for source_index in &bucket.sources {
                let source = self.plan.data_sources.get(*source_index).ok_or_else(|| {
                    EvaluatorError::InvalidPlan(format!(
                        "missing data source index: {source_index}"
                    ))
                })?;

                if !table_matches(&source.table, &options.source_table) {
                    continue;
                }

                let context = EvalContext {
                    row: Some(&options.record),
                    request: &RequestParameters::default(),
                };

                if !all_filters_match(&source.filters, &context)? {
                    continue;
                }

                let mut data = Map::new();
                for column in &source.columns {
                    match column {
                        crate::model::ColumnSource::Star(v) if v == "star" => {
                            for (key, value) in filter_json_row(&options.record) {
                                data.entry(key).or_insert(value);
                            }
                        }
                        crate::model::ColumnSource::Expression { expr, alias } => {
                            let value = evaluate_expression(expr, &context)?;
                            if matches!(value, Value::Null | Value::String(_) | Value::Number(_)) {
                                data.insert(alias.clone(), value);
                            }
                        }
                        _ => {}
                    }
                }

                let mut partition_values = Vec::new();
                let mut partition_valid = true;
                for partition in &source.partition_by {
                    let value = evaluate_expression(&partition.expr, &context)?;
                    if let Some(normalized) = normalize_parameter_value(&value) {
                        partition_values.push(normalized);
                    } else {
                        partition_valid = false;
                        break;
                    }
                }

                if !partition_valid {
                    continue;
                }

                let bucket = format!(
                    "{}{}",
                    bucket.unique_name,
                    serialize_value_array(&partition_values)
                );
                let id = id_from_data(&data);
                let output_table = source
                    .output_table_name
                    .clone()
                    .unwrap_or_else(|| options.source_table.name.clone());

                results.push(EvaluatedRow {
                    bucket,
                    table: output_table,
                    id,
                    data,
                });
            }
        }

        Ok(results)
    }
}
