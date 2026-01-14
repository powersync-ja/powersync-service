import { RequestParameters } from '../types.js';
import { SqlBuilder, SqlEngine } from './sql_engine.js';
import * as plan from './plan.js';

export interface PreparedRequestFilters {
  requestMatches(parameters: RequestParameters): boolean;
  subscriptionMatches(parameters: RequestParameters): boolean;
}

export function prepareFilters(
  engine: SqlEngine,
  filters: plan.SqlExpression<plan.RequestSqlParameterValue>[]
): PreparedRequestFilters {
  let connectionRequestFilter: SqlBuilder | null = null;
  let subscriptionRequestFilter: SqlBuilder | null = null;

  for (const filter of filters) {
    function prepareFilterBuilder() {
      const builder = new SqlBuilder('SELECT 1 WHERE (');
      builder.addExpression(filter);
      builder.sql += ')';
      return builder;
    }

    function addToBuilder(builder: SqlBuilder) {
      builder.sql += ' AND (';
      builder.addExpression(filter);
      builder.sql += ')';
    }

    for (const instantiation of filter.values) {
      if (instantiation.request === 'subscription') {
        // We need to add this to the subscription-specific filter.
        if (subscriptionRequestFilter) {
          addToBuilder(subscriptionRequestFilter);
        } else {
          subscriptionRequestFilter = prepareFilterBuilder();
        }
        break;
      }
    }

    // This doesn't depend on subscription parameters, so we can evaluate it earlier.
    if (connectionRequestFilter) {
      addToBuilder(connectionRequestFilter);
    } else {
      connectionRequestFilter = prepareFilterBuilder();
    }
  }

  let connection = connectionRequestFilter ? engine.prepare(connectionRequestFilter.sql) : null;
  let subscription = subscriptionRequestFilter ? engine.prepare(subscriptionRequestFilter.sql) : null;

  return {
    requestMatches(parameters: RequestParameters) {
      if (connection == null) {
        return true;
      }

      return connection.evaluateScalar(parametersForRequest(parameters, connectionRequestFilter!.values)) != null;
    },
    subscriptionMatches(parameters: RequestParameters) {
      if (subscription == null) {
        return true;
      }

      return subscription.evaluateScalar(parametersForRequest(parameters, subscriptionRequestFilter!.values)) != null;
    }
  };
}

export function parametersForRequest(parameters: RequestParameters, values: plan.SqlParameterValue[]): string[] {
  return values.map((v) => {
    if (plan.isRequestSqlParameterValue(v)) {
      switch (v.request) {
        case 'auth':
          return parameters.rawTokenPayload;
        case 'subscription':
          return parameters.rawStreamParameters!;
        case 'connection':
          return parameters.rawUserParameters;
      }
    } else {
      throw new Error('Illegal column reference in request filter');
    }
  });
}
