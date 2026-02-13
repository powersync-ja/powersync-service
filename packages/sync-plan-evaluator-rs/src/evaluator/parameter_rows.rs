impl SyncPlanEvaluator {
    pub fn evaluate_parameter_row(
        &self,
        source_table: &SourceTable,
        row: &JsonMap,
    ) -> EvaluatorResult<Vec<EvaluatedParameters>> {
        let mut results = Vec::new();
        let request = RequestParameters::default();
        let context = EvalContext {
            row: Some(row),
            request: &request,
        };

        for parameter_index in &self.plan.parameter_indexes {
            if !table_matches(&parameter_index.table, source_table) {
                continue;
            }

            if !all_filters_match(&parameter_index.filters, &context)? {
                continue;
            }

            let mut output_values = Vec::new();
            for output in &parameter_index.output {
                let value = evaluate_expression(output, &context)?;
                if let Some(value) = normalize_parameter_value(&value) {
                    output_values.push(value);
                } else {
                    output_values.clear();
                    break;
                }
            }
            if output_values.is_empty() && !parameter_index.output.is_empty() {
                continue;
            }

            let mut partition_values = Vec::new();
            for partition in &parameter_index.partition_by {
                let value = evaluate_expression(&partition.expr, &context)?;
                if let Some(value) = normalize_parameter_value(&value) {
                    partition_values.push(value);
                } else {
                    partition_values.clear();
                    break;
                }
            }
            if partition_values.len() != parameter_index.partition_by.len() {
                continue;
            }

            let scope = &parameter_index.lookup_scope;
            let lookup_values = scoped_lookup_values(scope, &partition_values);
            let lookup = ScopedParameterLookup {
                serialized_representation: serialize_value_array(&lookup_values),
                values: lookup_values,
            };

            let mut bucket_param_row = JsonMap::new();
            for (index, value) in output_values.into_iter().enumerate() {
                bucket_param_row.insert(index.to_string(), value);
            }

            results.push(EvaluatedParameters {
                lookup,
                bucket_parameters: vec![bucket_param_row],
            });
        }

        Ok(results)
    }
}
