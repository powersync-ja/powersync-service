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
                    };

                    if !all_filters_match(&querier.request_filters, &context)? {
                        continue;
                    }

                    if querier.lookup_stages.is_empty() {
                        let values = self
                            .instantiate_without_lookups(&querier.source_instantiation, &context)?;
                        for instance in cartesian_product(values) {
                            let bucket = self.bucket_from_querier(querier.bucket, &instance)?;
                            static_buckets.push(ResolvedBucket {
                                bucket,
                                priority: stream.stream.priority,
                                definition: stream.stream.name.clone(),
                                inclusion_reasons: vec![inclusion_reason.clone()],
                            });
                        }
                    } else {
                        let lookup_requests = self.prepare_lookup_requests(querier, &context)?;

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
                            lookup_stages: querier.lookup_stages.clone(),
                            lookup_requests,
                            source_instantiation: querier.source_instantiation.clone(),
                        });
                    }
                }
            }
        }

        Ok(PreparedBucketQueries {
            static_buckets,
            dynamic_queries,
        })
    }
}
