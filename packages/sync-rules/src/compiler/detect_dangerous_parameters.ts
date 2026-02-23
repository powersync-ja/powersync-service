import {
  LookupResultParameterValue,
  ParameterLookup,
  ParameterValue,
  RequestParameterValue,
  StreamResolver
} from './bucket_resolver.js';
import { ConnectionParameter, SourceLocation, SyncExpression } from './expression.js';

/**
 * Detects streams exclusively using unauthenticated parameters (derived from the subscription or global request data).
 *
 * We emit a warning for those streams since the developer might not be aware that these parameters aren't validated.
 */
export class DangerousParameterDetector {
  processResolver(resolver: StreamResolver) {
    let classification = this.#classifyParameters(resolver.resolvedBucket.instantiation);
    if (classification == 'neutral' && resolver.requestFilters) {
      classification = this.#mergeClassification(
        resolver.requestFilters.map((f) => this.#classifyExpression(f.expression))
      );
    }

    if (classification == 'authenticated' || classification == 'neutral') {
      // We have at least one authenticated parameter, all good.
      return;
    } else {
      // Unauthenticated parameter, warn.
      const { location, errors } = classification.unauthenticated;

      errors.report(
        'Clients can send any value for this unauthenticated parameter, so this is unsuitable for authorization. Consider further constraining this with an authenticated parameter.',
        location,
        { isWarning: true }
      );
    }
  }
  #classifyParameter(parameter: ParameterValue): ParameterClassification {
    if (parameter instanceof RequestParameterValue) {
      return this.#classifyExpression(parameter.expression.expression);
    } else if (parameter instanceof LookupResultParameterValue) {
      const lookup = parameter.lookup!;
      if (lookup instanceof ParameterLookup) {
        return this.#classifyParameters(lookup.instantiation);
      } else {
        return this.#mergeClassification(
          lookup.tableValuedFunction.parameters.map((e) => this.#classifyExpression(e.expression))
        );
      }
    } else {
      // Intersection parameter, this is authenticated if any inner value is.
      return this.#classifyParameters(parameter.inner);
    }
  }

  #classifyParameters(parameters: ParameterValue[]): ParameterClassification {
    return this.#mergeClassification(parameters.map((p) => this.#classifyParameter(p)));
  }

  #classifyExpression(expr: SyncExpression): ParameterClassification {
    let isAuthenticated = false;
    let anyUnauthenticatedExpr: SourceLocation | null = null;

    for (const instantiation of expr.instantiation) {
      if (instantiation instanceof ConnectionParameter) {
        if (instantiation.source == 'auth') {
          isAuthenticated = true;
        } else if (anyUnauthenticatedExpr == null) {
          anyUnauthenticatedExpr = {
            location: instantiation.syntacticOrigin,
            errors: expr.location.errors
          };
        }
      }
    }

    if (anyUnauthenticatedExpr != null && !isAuthenticated) {
      return { unauthenticated: anyUnauthenticatedExpr };
    } else {
      return 'authenticated';
    }
  }

  /**
   * Merges an array of inner classifications, considering the array authenticatated if any element is.
   */
  #mergeClassification(inner: ParameterClassification[]): ParameterClassification {
    let unauthenticated: SourceLocation | null = null;
    for (const classification of inner) {
      if (classification == 'authenticated') {
        return 'authenticated';
      } else if (unauthenticated == null && classification != 'neutral') {
        unauthenticated = classification.unauthenticated;
      }
    }

    return unauthenticated == null ? 'neutral' : { unauthenticated };
  }
}

type ParameterClassification = { unauthenticated: SourceLocation } | 'authenticated' | 'neutral';
