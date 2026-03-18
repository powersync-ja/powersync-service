impl SyncPlanEvaluator {
    pub fn evaluate_row(&self, options: EvaluateRowOptions) -> EvaluatorResult<Vec<EvaluatedRow>> {
        self.evaluate_row_ref(&options)
    }

    pub fn evaluate_row_ref(&self, options: &EvaluateRowOptions) -> EvaluatorResult<Vec<EvaluatedRow>> {
        self.evaluate_row_parts(&options.source_table, &options.record)
    }

    pub fn evaluate_row_parts(
        &self,
        source_table: &SourceTable,
        record: &JsonMap,
    ) -> EvaluatorResult<Vec<EvaluatedRow>> {
        let mut results = Vec::new();
        let request = RequestParameters::default();
        let context = EvalContext {
            row: Some(record),
            request: &request,
            table_rows: None,
        };

        for bucket in &self.plan.buckets {
            for source_index in &bucket.sources {
                let source = self.plan.data_sources.get(*source_index).ok_or_else(|| {
                    EvaluatorError::InvalidPlan(format!(
                        "missing data source index: {source_index}"
                    ))
                })?;

                if !table_matches(&source.table, source_table) {
                    continue;
                }

                let table_rows = evaluate_table_valued_inputs(&source.table_valued_functions, &context)?;
                for binding in table_rows {
                    let scoped = EvalContext {
                        row: Some(record),
                        request: &request,
                        table_rows: Some(&binding),
                    };

                    if !all_filters_match(&source.filters, &scoped)? {
                        continue;
                    }

                    let mut data = Map::new();
                    for column in &source.columns {
                        match column {
                            crate::model::ColumnSource::Star(v) if v == "star" => {
                                extend_filtered_json_row(&mut data, record);
                            }
                            crate::model::ColumnSource::Expression { expr, alias } => {
                                let value = evaluate_expression(expr, &scoped)?;
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
                        let value = evaluate_expression(&partition.expr, &scoped)?;
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
                        .unwrap_or_else(|| source_table.name.clone());

                    results.push(EvaluatedRow {
                        bucket,
                        table: output_table,
                        id,
                        data,
                    });
                }
            }
        }

        Ok(results)
    }

    pub fn row_parse_requirements(&self, source_table: &SourceTable) -> RowParseRequirements {
        let mut requirements = RowParseRequirements::default();

        for bucket in &self.plan.buckets {
            for source_index in &bucket.sources {
                let Some(source) = self.plan.data_sources.get(*source_index) else {
                    continue;
                };

                if !table_matches(&source.table, source_table) {
                    continue;
                }

                for filter in &source.filters {
                    collect_column_references(filter, &mut requirements.full_columns);
                }
                for function in &source.table_valued_functions {
                    for input in &function.function_inputs {
                        collect_column_references(input, &mut requirements.full_columns);
                    }
                }
                for partition in &source.partition_by {
                    collect_column_references(&partition.expr, &mut requirements.full_columns);
                }
                for column in &source.columns {
                    match column {
                        crate::model::ColumnSource::Star(v) if v == "star" => {
                            requirements.include_star_scalars = true;
                        }
                        crate::model::ColumnSource::Expression { expr, .. } => {
                            collect_column_references(expr, &mut requirements.full_columns);
                        }
                        _ => {}
                    }
                }
            }
        }

        requirements
    }
}
