import {
  EvaluateTableValuedFunction,
  ExpandingLookup,
  IntersectionParameterValue,
  LookupResultParameterValue,
  ParameterLookup,
  ParameterValue,
  RequestParameterValue,
  ResolveBucket,
  StreamResolver
} from './bucket_resolver.js';
import { equalsIgnoringResultSet } from './compatibility.js';
import { And, BaseTerm, EqualsClause, RequestExpression, RowExpression, SingleDependencyExpression } from './filter.js';
import { PartitionKey, PointLookup, RowEvaluator } from './rows.js';
import { PhysicalSourceResultSet, RequestTableValuedResultSet, SourceResultSet } from './table.js';
import { ParsingErrorListener, SyncStreamCompiler } from './compiler.js';
import { HashMap, HashSet } from './equality.js';
import { ParsedStreamQuery } from './parser.js';
import { StreamOptions } from '../sync_plan/plan.js';

/**
 * Builds stream resolvers for a single stream, potentially consisting of multiple queries.
 */
export class QuerierGraphBuilder {
  private readonly resolvers: StreamResolver[] = [];
  readonly counter: UniqueCounter;

  constructor(
    readonly compiler: SyncStreamCompiler,
    readonly options: StreamOptions
  ) {
    this.counter = new UniqueCounter();
  }

  /**
   * Adds a given query to the stream compiled by this builder.
   */
  process(query: ParsedStreamQuery, errors: ParsingErrorListener) {
    for (const variant of query.where.terms) {
      const resolved = new PendingQuerierPath(this, query, errors, variant).resolvePrimaryInput();
      this.resolvers.push(resolved);
    }
  }

  /**
   * Merges created stream resolvers and adds them to the compiler.
   */
  finish() {
    this.compiler.output.resolvers.push(...this.mergeBuckets());
  }

  /**
   * Merges bucket definitions from multiple queries if they're compatible.
   *
   * As an example, consider a stream defined with two queries:
   *
   *  - `SELECT * FROM settings WHERE user_id = auth.user_id()`
   *  - `SELECT * FROM notes WHERE user_id = auth.user_id()`
   *
   * While the two queries select from different source rows (meaning that their {@link RowEvaluator}s are distinct),
   * they also don't really need independent buckets.
   *
   * This method merges two stream resolvers if they have {@link ResolveBucket} steps with the same instantiation (as
   * in, the same stream subscription + connection is guaranteed to evaluate to the same parameters).
   */
  private mergeBuckets(): StreamResolver[] {
    const resolvers = new HashSet<StreamResolver>({
      equals: (a, b) => a.hasIdenticalInstantiation(b),
      hash: (hasher, value) => value.buildInstantiationHash(hasher)
    });

    for (const sourceResolver of this.resolvers) {
      const [existing, didInsert] = resolvers.getOrInsert(sourceResolver);
      if (!didInsert) {
        for (const newEvaluator of sourceResolver.resolvedBucket.evaluators) {
          existing.resolvedBucket.evaluators.add(newEvaluator);
        }
      }
    }

    return [...resolvers];
  }
}

/**
 * Splits a logical conjunction ({@link And}) to create source row processors and parameter lookups.
 *
 * This works by first processing subterms related to the source set selected from. If the subterm is a row expression,
 * we add it as a filter to the row processor or parameter lookup. If it is a match expression, we create a partition
 * key and obtain the value by recursively applying this algorithm to resolve the other side.
 */
class PendingQuerierPath {
  // Terms in the And that have not yet been handled.
  private readonly pendingFactors: BaseTerm[] = [];
  private readonly pendingLookups = new Map<SourceResultSet, PendingExpandingLookup>();

  /**
   * A stack of result sets currently being analyzed.
   *
   * This is used to guard against circular references, although those should never happen.
   */
  private readonly resolveStack: SourceResultSet[] = [];
  private pendingStage: PendingStage = { lookups: [] };

  constructor(
    private readonly builder: QuerierGraphBuilder,
    private readonly query: ParsedStreamQuery,
    private readonly errors: ParsingErrorListener,
    condition: And
  ) {
    this.pendingFactors.push(...condition.terms);
  }

  resolvePrimaryInput(): StreamResolver {
    const state = this.resolveResultSet(this.query.sourceTable);
    const [partitions, partitionValues] = state.resolvePartitions();
    const evaluator = this.builder.compiler.output.canonicalizeEvaluator(
      new RowEvaluator({
        columns: this.query.resultColumns,
        syntacticSource: this.query.sourceTable,
        filters: state.filters,
        partitionBy: partitions
      })
    );
    this.processExistsOperators();

    // Resolving a result set removes its conditions from pendingFactors, so remaining conditions must be related to the
    // request (e.g. where `auth.parameter('is_admin')`).
    const requestConditions: RequestExpression[] = [];
    for (const remaining of this.pendingFactors) {
      if (remaining instanceof SingleDependencyExpression) {
        if (remaining.resultSet != null) {
          this.errors.report(
            'This filter is unrelated to the request or the table being synced, and not supported.',
            remaining.expression.node!
          );
        } else {
          requestConditions.push(new RequestExpression(remaining));
        }
      } else {
        this.errors.report('Unable to associate this filter with added tables', remaining.location!);
      }
    }

    return new StreamResolver(
      this.builder.options,
      requestConditions,
      this.materializeLookupStages(),
      new ResolveBucket(evaluator, partitionValues),
      this.builder.counter.use(this.builder.options.name)
    );
  }

  private pushStage(): PendingStage {
    const childStage = this.pendingStage;
    this.pendingStage = childStage.parent ??= { lookups: [] };
    return childStage;
  }

  private popStage(stage: PendingStage) {
    this.pendingStage = stage;
  }

  private resolvePointLookup(resultSet: PhysicalSourceResultSet): PendingExpandingLookup {
    const resolved = this.resolveResultSet(resultSet);
    const [partitionKeys, partitionInputs] = resolved.resolvePartitions();

    return new PendingExpandingLookup({
      type: 'point',
      pattern: resultSet,
      filters: resolved.filters,
      partitionKeys: partitionKeys,
      inputs: partitionInputs
    });
  }

  private resolveTableValuedLookup(resultSet: RequestTableValuedResultSet): PendingExpandingLookup {
    const resolved = this.resolveResultSet(resultSet);
    if (!resolved.partition.isEmpty) {
      // At the moment, inputs to a table-valued functions must be static or only depend on the request. We may lift
      // this restriction in the future.
      this.errors.report('Table-valued result sets cannot be partitioned', resultSet.source.origin);
    }

    return new PendingExpandingLookup({ type: 'table_valued', resultSet, filters: resolved.filters });
  }

  private resolveExpandingLookup(resultSet: SourceResultSet): PendingExpandingLookup {
    const existing = this.pendingLookups.get(resultSet);
    if (existing != null) {
      return existing;
    }

    // Something depends on this lookup when resolveExpandingLookup is called, so we have to push this into a new stage
    // to ensure we have results before the current stage.
    const childStage = this.pushStage();

    const resolved =
      resultSet instanceof PhysicalSourceResultSet
        ? this.resolvePointLookup(resultSet)
        : this.resolveTableValuedLookup(resultSet);

    this.pendingLookups.set(resultSet, resolved);
    this.pendingStage.lookups.push(resolved);
    this.popStage(childStage);
    return resolved;
  }

  /**
   * Extracts filters, partition keys and partition instantiations for a given source result set.
   */
  private resolveResultSet(source: SourceResultSet): ResolvedResultSet {
    if (this.resolveStack.indexOf(source) != -1) {
      throw new Error('internal error: circular reference when resolving result set');
    }
    this.resolveStack.push(source);
    const state = new ResolvedResultSet();

    for (const expression of [...this.pendingFactors]) {
      if (expression instanceof SingleDependencyExpression) {
        if (expression.resultSet === source) {
          // This expression only depends on the table, so we add it as a filter for the row or parameter evaluator.
          state.filters.push(new RowExpression(expression));
          this.removePendingExpression(expression);
        }
      } else {
        // Must be a match term.
        const partitionBy = (thisRow: SingleDependencyExpression, otherRow: SingleDependencyExpression) => {
          this.removePendingExpression(expression);
          const key = new PartitionKey(new RowExpression(thisRow));
          const values = state.partition.putIfAbsent(key, () => []);

          if (otherRow.resultSet != null) {
            const lookup = this.resolveExpandingLookup(otherRow.resultSet);
            const index = lookup.addOutput(new RowExpression(otherRow));
            const value = new LookupResultParameterValue(index);
            lookup.dependents.push(value);
            values.push(value);
          } else {
            // Other row doesn't depend on a source row, so we can read the value out of the connection.
            values.push(new RequestParameterValue(otherRow));
          }
        };

        if (expression.left.resultSet === source) {
          partitionBy(expression.left, expression.right);
        } else if (expression.right.resultSet === source) {
          partitionBy(expression.right, expression.left);
        } else {
          // Unrelated match clause.
          continue;
        }
      }
    }

    const popped = this.resolveStack.pop();
    if (popped !== source) {
      throw new Error('internal error: resolve stack broken');
    }
    return state;
  }

  /**
   * Handles `EXIST`-like subquery operators.
   *
   * Consider a filter like `WHERE auth.user_id() IN (SELECT id FROM users WHERE is_admin)`. This gets represented as an
   * {@link EqualsClause}, but it doesn't introduce any bucket parameters.
   *
   * We handle these in a special way: The `auth.user_id()` equality is pushed into the subquery, effectively resulting
   * in `WHERE EXISTS (SELECT id FROM users WHERE is_admin AND id = auth.user_id())`. This is implemented by pushing a
   * parameter lookup that is never used, but still evaluated to skip connections where it doesn't match.
   */
  private processExistsOperators() {
    for (const expression of [...this.pendingFactors]) {
      if (expression instanceof EqualsClause) {
        const process = (connection: SingleDependencyExpression, other: SingleDependencyExpression) => {
          if (other.resultSet != null) {
            // We just need to add the lookup to implement EXISTS semantics, it will never be used anywhere.
            this.resolveExpandingLookup(other.resultSet);
          }
        };

        if (expression.left.dependsOnConnection) {
          process(expression.left, expression.right);
        } else if (expression.right.dependsOnConnection) {
          process(expression.right, expression.left);
        }
      }
    }
  }

  private removePendingExpression(removed: BaseTerm) {
    const index = this.pendingFactors.indexOf(removed);
    this.pendingFactors.splice(index, 1);
  }

  private materializeLookupStages(): ExpandingLookup[][] {
    const targets: ExpandingLookup[][] = [];
    this.materializeLookupStage(this.pendingStage, targets);
    return targets;
  }

  private materializeLookupStage(stage: PendingStage, target: ExpandingLookup[][]) {
    if (stage.parent != null) {
      this.materializeLookupStage(stage.parent, target);
    }

    if (stage.lookups.length != 0) {
      const lookups: ExpandingLookup[] = [];
      target.push(lookups);

      for (const lookup of stage.lookups) {
        const data = lookup.data;
        let lookupWithInputs: ExpandingLookup;

        if (data.type == 'point') {
          const canonicalized = this.builder.compiler.output.canonicalizePointLookup(
            new PointLookup({
              syntacticSource: data.pattern,
              filters: data.filters,
              partitionBy: data.partitionKeys,
              result: lookup.usedOutputs
            })
          );
          lookupWithInputs = new ParameterLookup(canonicalized, data.inputs);
        } else {
          lookupWithInputs = new EvaluateTableValuedFunction(data.resultSet, lookup.usedOutputs, data.filters);
        }

        lookups.push(lookupWithInputs);
        for (const usage of lookup.dependents) {
          usage.lookup = lookupWithInputs;
        }
      }
    }
  }
}

class ResolvedResultSet {
  readonly filters: RowExpression[] = [];
  readonly partition = new HashMap<PartitionKey, ParameterValue[]>(equalsIgnoringResultSet);

  resolvePartitions(): [PartitionKey[], ParameterValue[]] {
    const keys: PartitionKey[] = [];
    const instantiation: ParameterValue[] = [];

    for (const [key, values] of this.partition.entries) {
      keys.push(key);
      if (values.length == 1) {
        instantiation.push(values[0]);
      } else {
        instantiation.push(new IntersectionParameterValue(values));
      }
    }

    return [keys, instantiation];
  }
}

class PendingExpandingLookup {
  readonly usedOutputs: RowExpression[] = [];
  readonly dependents: LookupResultParameterValue[] = [];

  constructor(readonly data: PendingPointLookup | PendingTableValuedFunctionLookup) {}

  addOutput(param: RowExpression) {
    for (let i = 0; i < this.usedOutputs.length; i++) {
      const existing = this.usedOutputs[i];
      if (existing.equalsAssumingSameResultSet(param)) {
        return i;
      }
    }

    const index = this.usedOutputs.length;
    this.usedOutputs.push(param);
    return index;
  }
}

interface PendingPointLookup {
  type: 'point';
  pattern: PhysicalSourceResultSet;
  filters: RowExpression[];
  partitionKeys: PartitionKey[];
  inputs: ParameterValue[];
}

interface PendingTableValuedFunctionLookup {
  type: 'table_valued';
  resultSet: RequestTableValuedResultSet;
  filters: RowExpression[];
}

interface PendingStage {
  parent?: PendingStage;
  lookups: PendingExpandingLookup[];
}
class UniqueCounter {
  current: number;

  constructor() {
    this.current = 0;
  }

  use(prefix: string) {
    const value = this.current++;
    return `${prefix}|${value}`;
  }
}
