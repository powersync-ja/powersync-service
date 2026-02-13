impl SyncPlanEvaluator {
    pub fn resolve_bucket_queries(
        &self,
        prepared: &PreparedBucketQueries,
        results: &[LookupResults],
    ) -> EvaluatorResult<Vec<ResolvedBucket>> {
        let mut all_buckets = prepared.static_buckets.clone();

        let mut lookup_rows_by_key: HashMap<String, Vec<JsonMap>> = HashMap::new();
        for result in results {
            lookup_rows_by_key.insert(
                result.lookup.serialized_representation.clone(),
                result.rows.clone(),
            );
        }

        for query in &prepared.dynamic_queries {
            let mut staged_rows: HashMap<(usize, usize), Vec<Vec<Value>>> = HashMap::new();
            let request_values: HashMap<(usize, usize), Vec<ScopedParameterLookup>> = query
                .lookup_requests
                .iter()
                .map(|request| ((request.stage_id, request.id_in_stage), request.values.clone()))
                .collect();

            for (stage_id, stage) in query.lookup_stages.iter().enumerate() {
                for (id_in_stage, lookup) in stage.iter().enumerate() {
                    let scoped_values = if let Some(values) = request_values.get(&(stage_id, id_in_stage))
                    {
                        values.clone()
                    } else {
                        match lookup {
                            SerializedExpandingLookup::Parameter {
                                lookup,
                                instantiation,
                            } => {
                                let parameter_index =
                                    self.plan.parameter_indexes.get(*lookup).ok_or_else(|| {
                                        EvaluatorError::InvalidPlan(format!(
                                            "missing parameter lookup index: {lookup}"
                                        ))
                                    })?;
                                let scope = &parameter_index.lookup_scope;
                                let inputs =
                                    self.resolve_parameter_values(instantiation, &staged_rows)?;
                                cartesian_product(inputs)
                                    .into_iter()
                                    .map(|values| {
                                        let scoped_values = scoped_lookup_values(scope, &values);
                                        ScopedParameterLookup {
                                            serialized_representation: serialize_value_array(
                                                &scoped_values,
                                            ),
                                            values: scoped_values,
                                        }
                                    })
                                    .collect()
                            }
                            SerializedExpandingLookup::TableValued { .. } => Vec::new(),
                        }
                    };

                    let mut rows = Vec::new();
                    for lookup in &scoped_values {
                        if let Some(found) = lookup_rows_by_key.get(&lookup.serialized_representation)
                        {
                            for row in found {
                                rows.push(indexed_row_to_vec(row));
                            }
                        }
                    }
                    staged_rows.insert((stage_id, id_in_stage), rows);
                }
            }

            let instantiated =
                self.resolve_parameter_values(&query.source_instantiation, &staged_rows)?;
            for instance in cartesian_product(instantiated) {
                all_buckets.push(ResolvedBucket {
                    bucket: format!(
                        "{}{}",
                        query.bucket_prefix,
                        serialize_value_array(&instance)
                    ),
                    priority: query.priority,
                    definition: query.descriptor.clone(),
                    inclusion_reasons: vec![query.inclusion_reason.clone()],
                });
            }
        }

        Ok(all_buckets)
    }

    fn bucket_from_querier(
        &self,
        bucket_index: usize,
        values: &[Value],
    ) -> EvaluatorResult<String> {
        let bucket = self.plan.buckets.get(bucket_index).ok_or_else(|| {
            EvaluatorError::InvalidPlan(format!("missing bucket index for querier: {bucket_index}"))
        })?;

        Ok(format!(
            "{}{}",
            bucket.unique_name,
            serialize_value_array(values)
        ))
    }

    fn instantiate_without_lookups(
        &self,
        values: &[SerializedParameterValue],
        context: &EvalContext<'_>,
    ) -> EvaluatorResult<Vec<Vec<Value>>> {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let result = match value {
                SerializedParameterValue::Request { expr } => {
                    let value = evaluate_expression(expr, context)?;
                    normalize_parameter_value(&value)
                        .map(|v| vec![v])
                        .unwrap_or_default()
                }
                SerializedParameterValue::Intersection { values } => {
                    let items = self.instantiate_without_lookups(values, context)?;
                    intersection(items)
                }
                SerializedParameterValue::Lookup { .. } => {
                    return Err(EvaluatorError::UnsupportedExpression(
                        "lookup references require resolve_bucket_queries()".to_string(),
                    ))
                }
            };

            out.push(result);
        }

        Ok(out)
    }

    fn prepare_lookup_requests(
        &self,
        querier: &crate::model::SerializedStreamQuerier,
        context: &EvalContext<'_>,
    ) -> EvaluatorResult<Vec<LookupRequest>> {
        let mut requests = Vec::new();

        for (stage_id, stage) in querier.lookup_stages.iter().enumerate() {
            for (id_in_stage, lookup) in stage.iter().enumerate() {
                if let SerializedExpandingLookup::Parameter {
                    lookup,
                    instantiation,
                } = lookup
                {
                    let parameter_index =
                        self.plan.parameter_indexes.get(*lookup).ok_or_else(|| {
                            EvaluatorError::InvalidPlan(format!(
                                "missing parameter lookup index: {lookup}"
                            ))
                        })?;
                    let scope = &parameter_index.lookup_scope;

                    let input_values = match self.instantiate_without_lookups(instantiation, context)
                    {
                        Ok(values) => values,
                        Err(EvaluatorError::UnsupportedExpression(_)) => {
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let mut lookup_values = Vec::new();
                    for values in cartesian_product(input_values) {
                        let scoped_values = scoped_lookup_values(scope, &values);
                        lookup_values.push(ScopedParameterLookup {
                            serialized_representation: serialize_value_array(&scoped_values),
                            values: scoped_values,
                        });
                    }

                    requests.push(LookupRequest {
                        stage_id,
                        id_in_stage,
                        values: lookup_values,
                    });
                }
            }
        }

        Ok(requests)
    }

    fn resolve_parameter_values(
        &self,
        source_instantiation: &[SerializedParameterValue],
        staged_rows: &HashMap<(usize, usize), Vec<Vec<Value>>>,
    ) -> EvaluatorResult<Vec<Vec<Value>>> {
        let mut values = Vec::new();

        for value in source_instantiation {
            let candidates = match value {
                SerializedParameterValue::Request { .. } => {
                    return Err(EvaluatorError::UnsupportedExpression(
                        "request references are expected to be resolved in prepare_bucket_queries()".to_string(),
                    ))
                }
                SerializedParameterValue::Lookup {
                    lookup,
                    result_index,
                } => {
                    let rows = staged_rows
                        .get(&(lookup.stage_id, lookup.id_in_stage))
                        .cloned()
                        .unwrap_or_default();

                    let mut out = Vec::new();
                    for row in rows {
                        if let Some(value) = row.get(*result_index) {
                            out.push(value.clone());
                        }
                    }
                    out
                }
                SerializedParameterValue::Intersection { values } => {
                    let grouped = self.resolve_parameter_values(values, staged_rows)?;
                    intersection(grouped)
                }
            };

            values.push(candidates);
        }

        Ok(values)
    }

    pub fn plan(&self) -> &SerializedSyncPlan {
        &self.plan
    }

    pub fn default_schema(&self) -> &str {
        &self.options.default_schema
    }
}
