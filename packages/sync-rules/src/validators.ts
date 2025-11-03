import { isRequestFunctionCall } from './request_functions.js';
import { isParameterMatchClause } from './sql_support.js';
import { CompiledClause, isLegacyParameterFromTableClause, ParameterMatchClause } from './types.js';

/**
 * Detects the use of request parameters in a compiled clause.
 */
export class DetectRequestParameters {
  /** request.user_id(), request.jwt(), token_parameters.* */
  usesAuthenticatedRequestParameters: boolean = false;
  /** request.parameters(), user_parameters.* */
  usesUnauthenticatedRequestParameters: boolean = false;
  /** subscription.parameters(), subscription.parameters('a')  */
  usesStreamParameters: boolean = false;

  accept(clause?: CompiledClause) {
    if (clause == null) {
      return null;
    }

    if (isRequestFunctionCall(clause)) {
      const f = clause.function;

      this.usesAuthenticatedRequestParameters ||= f.parameterUsage == 'authenticated';
      this.usesUnauthenticatedRequestParameters ||=
        f.parameterUsage == 'unauthenticated' || f.parameterUsage == 'subscription';
      this.usesStreamParameters ||= f.parameterUsage == 'subscription';
    } else if (isLegacyParameterFromTableClause(clause)) {
      const table = clause.table;
      if (table == 'token_parameters') {
        this.usesAuthenticatedRequestParameters = true;
      } else if (table == 'user_parameters') {
        this.usesUnauthenticatedRequestParameters = true;
      }
    } else if (isParameterMatchClause(clause) && clause.specialType == 'or') {
      // We only treat this clause as using authenticated request parameters if all subclauses use authenticated request
      // parameters.
      const leftVisitor = new DetectRequestParameters();
      const rightVisitor = new DetectRequestParameters();

      let isFirstChild = true;
      clause.visitChildren!((v) => {
        if (isFirstChild) {
          leftVisitor.accept(v);
        } else {
          rightVisitor.accept(v);
        }
      });
      this.usesAuthenticatedRequestParameters =
        leftVisitor.usesAuthenticatedRequestParameters && rightVisitor.usesAuthenticatedRequestParameters;

      // For unauthenticated parameters, it's enough if either side reads unauthenticated data.
      this.usesUnauthenticatedRequestParameters ||= leftVisitor.usesUnauthenticatedRequestParameters;
      this.usesUnauthenticatedRequestParameters ||= rightVisitor.usesUnauthenticatedRequestParameters;
      return;
    }

    clause.visitChildren?.((c) => this.accept(c));
  }

  acceptAll(clauses: Iterable<CompiledClause>) {
    for (const clause of clauses) {
      this.accept(clause);
    }
  }
}
