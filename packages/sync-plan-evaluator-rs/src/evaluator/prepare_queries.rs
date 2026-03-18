impl SyncPlanEvaluator {
    pub fn prepare_bucket_queries(
        &self,
        options: PrepareBucketQueryOptions,
    ) -> EvaluatorResult<PreparedBucketQueries> {
        let mut static_buckets = Vec::new();
        let mut dynamic_queries = Vec::new();

        for stream in &self.plan.streams {
            let requested_streams = options
                .streams
                .get(&stream.stream.name)
                .cloned()
                .unwrap_or_default();

            let mut stream_subscriptions: Vec<(RequestParameters, BucketInclusionReason)> =
                Vec::new();

            for requested in &requested_streams {
                let mut parameters = options.global_parameters.clone();
                parameters.subscription = requested
                    .parameters
                    .clone()
                    .unwrap_or(Value::Object(Map::new()));
                stream_subscriptions.push((
                    parameters,
                    BucketInclusionReason::Subscription {
                        subscription: requested.opaque_id,
                    },
                ));
            }

            let has_explicit_default = requested_streams
                .iter()
                .any(|stream| stream.parameters.is_none());
            if stream.stream.is_subscribed_by_default
                && options.has_default_streams
                && !has_explicit_default
            {
                stream_subscriptions.push((
                    options.global_parameters.clone(),
                    BucketInclusionReason::Default("default".to_string()),
                ));
            }

            for (subscription_params, inclusion_reason) in stream_subscriptions {
                for querier in &stream.queriers {
                    let context = EvalContext {
                        row: None,
                        request: &subscription_params,
                        table_rows: None,
                    };

                    if !all_filters_match(&querier.request_filters, &context)? {
                        continue;
                    }

                    let mut staged_rows: HashMap<(usize, usize), Vec<Vec<Value>>> = HashMap::new();
                    let mut resolved_rows = Vec::new();
                    let mut lookup_requests = Vec::new();
                    let mut query_uninstantiable = false;
                    let mut has_dynamic_lookups = false;

                    for (stage_id, stage) in querier.lookup_stages.iter().enumerate() {
                        for (id_in_stage, lookup) in stage.iter().enumerate() {
                            match lookup {
                                SerializedExpandingLookup::TableValued {
                                    function_name,
                                    function_inputs,
                                    outputs,
                                    filters,
                                } => {
                                    let args = function_inputs
                                        .iter()
                                        .map(|expr| evaluate_expression(expr, &context))
                                        .collect::<EvaluatorResult<Vec<_>>>()?;
                                    let rows = evaluate_table_valued_function(function_name, &args)?
                                        .into_iter()
                                        .filter_map(|row| {
                                            let row_context = EvalContext {
                                                row: Some(&row),
                                                request: &subscription_params,
                                                table_rows: None,
                                            };

                                            match all_filters_match(filters, &row_context) {
                                                Ok(false) => None,
                                                Ok(true) => Some(
                                                    outputs
                                                        .iter()
                                                        .map(|expr| evaluate_expression(expr, &row_context))
                                                        .collect::<EvaluatorResult<Vec<_>>>(),
                                                ),
                                                Err(error) => Some(Err(error)),
                                            }
                                        })
                                        .collect::<EvaluatorResult<Vec<_>>>()?;

                                    if rows.is_empty() {
                                        query_uninstantiable = true;
                                        break;
                                    }
                                    resolved_rows.push(ResolvedLookupRows {
                                        stage_id,
                                        id_in_stage,
                                        rows: rows.iter().map(|row| vec_to_indexed_row(row)).collect(),
                                    });
                                    staged_rows.insert((stage_id, id_in_stage), rows);
                                }
                                SerializedExpandingLookup::Parameter {
                                    lookup,
                                    instantiation,
                                } => {
                                    has_dynamic_lookups = true;

                                    if let Some(input_values) = self.try_resolve_parameter_values(
                                        instantiation,
                                        &context,
                                        &staged_rows,
                                    )? {
                                        let parameter_index = self
                                            .plan
                                            .parameter_indexes
                                            .get(*lookup)
                                            .ok_or_else(|| {
                                                EvaluatorError::InvalidPlan(format!(
                                                    "missing parameter lookup index: {lookup}"
                                                ))
                                            })?;
                                        let scope = &parameter_index.lookup_scope;
                                        let values = cartesian_product(input_values)
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
                                            .collect::<Vec<_>>();

                                        if values.is_empty() {
                                            query_uninstantiable = true;
                                            break;
                                        }

                                        lookup_requests.push(LookupRequest {
                                            stage_id,
                                            id_in_stage,
                                            values,
                                        });
                                    }
                                }
                            }
                        }

                        if query_uninstantiable {
                            break;
                        }
                    }

                    if query_uninstantiable {
                        continue;
                    }

                    if !has_dynamic_lookups {
                        let values = self
                            .try_resolve_parameter_values(
                                &querier.source_instantiation,
                                &context,
                                &staged_rows,
                            )?
                            .unwrap_or_default();
                        for instance in cartesian_product(values) {
                            let bucket = self.bucket_from_querier(querier.bucket, &instance)?;
                            static_buckets.push(ResolvedBucket {
                                bucket,
                                priority: stream.stream.priority,
                                definition: stream.stream.name.clone(),
                                inclusion_reasons: vec![inclusion_reason.clone()],
                            });
                        }
                        continue;
                    }

                    let bucket_prefix = self
                        .plan
                        .buckets
                        .get(querier.bucket)
                        .ok_or_else(|| {
                            EvaluatorError::InvalidPlan(format!(
                                "missing bucket index: {}",
                                querier.bucket
                            ))
                        })?
                        .unique_name
                        .clone();

                    dynamic_queries.push(DynamicBucketQuery {
                        descriptor: stream.stream.name.clone(),
                        priority: stream.stream.priority,
                        bucket_prefix,
                        inclusion_reason: inclusion_reason.clone(),
                        resolved_rows,
                        lookup_stages: querier.lookup_stages.clone(),
                        lookup_requests,
                        source_instantiation: querier.source_instantiation.clone(),
                    });
                }
            }
        }

        Ok(PreparedBucketQueries {
            static_buckets,
            dynamic_queries,
        })
    }
}
