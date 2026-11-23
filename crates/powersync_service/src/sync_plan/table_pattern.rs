use std::{collections::HashMap, str::FromStr, sync::Arc};

use serde::Deserialize;

/// Matches a table or schema name.
#[derive(Clone, Debug)]
pub struct NameMatcher {
    pub name: String,
    pub wildcard: bool,
}

impl NameMatcher {
    pub fn from_string(mut src: String) -> Self {
        let wildcard = if src.ends_with('%') {
            src.pop();
            true
        } else {
            false
        };

        Self {
            name: src,
            wildcard,
        }
    }
}

impl<'a> Deserialize<'a> for NameMatcher {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let name: String = String::deserialize(deserializer)?;
        Ok(Self::from_string(name))
    }
}

impl FromStr for NameMatcher {
    type Err = !;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_string(s.to_owned()))
    }
}

#[derive(Clone, Debug)]
pub struct TablePattern {
    pub connection_tag: String,
    pub schema: NameMatcher,
    pub table: NameMatcher,
}

impl TablePattern {
    pub const DEFAULT_TAG: &'static str = "default";
}

/// Reference to a source table associated with a replicated row.
#[derive(Clone, Copy, Debug)]
pub struct SourceTableRef<'a> {
    /// A tag associated with the connection.
    ///
    /// This is not currently supported, the value must be `default`.
    pub connection_tag: &'a str,
    /// The name of the schema.
    pub schema: &'a str,
    /// The name of the table.
    pub name: &'a str,
}

/// Resolves table names in a schema to the value for patterns matching them.
type TableIndex<T> = NameIndex<Arc<T>>;

/// A static index resolving source tables to values associated with the set of [TablePattern]s
/// matching them.
///
/// The index is built upfront for a fixed list of patterns. For every unique combination of
/// patterns that can match a source table, a single value is created and shared between all
/// tables resolving to that combination.
///
/// For example, source patterns `foo`, `bar` and `%` create three groups: One with an empty key for
/// `%`, one with key `foo` containing `foo` and `%`, and one with key `bar` containing `bar` and
/// `%`.
///
/// The index is built in stages, with exact matches over connection tags and either prefix or
/// exact matches for schema and table names.
#[derive(Debug)]
pub struct TablePatternIndex<T> {
    // If we were to support connection tags, we'd have to wrap this in a map
    by_schema: NameIndex<Arc<TableIndex<T>>>,
}

impl<T: Default> TablePatternIndex<T> {
    /// Builds an index for the given sources, each of which is associated with the
    /// [TablePattern]s returned by `patterns`.
    ///
    /// For each unique, non-empty combination of sources with a pattern matching a table, a value
    /// is created by starting from [Default::default] and invoking `add` with each source of that
    /// combination, in the order they appear in `sources`. Sources are added once, even if
    /// multiple of their patterns match.
    pub fn build<'a, S, P>(
        sources: &'a [S],
        patterns: impl Fn(&'a S) -> P,
        mut add: impl FnMut(&mut T, &'a S),
    ) -> Self
    where
        P: IntoIterator<Item = &'a TablePattern>,
    {
        // All patterns, along with the index of the source they belong to. Since sources are
        // visited in order, the source indices are sorted.
        let entries: Vec<(usize, &TablePattern)> = sources
            .iter()
            .enumerate()
            .flat_map(|(i, source)| patterns(source).into_iter().map(move |p| (i, p)))
            .collect();

        // Created groups. The Arc<T> is the group, Vec<usize> is the unique set of sources with
        // overlapping table patterns forming the group.
        let mut created_groups: HashMap<Vec<usize>, Arc<T>> = HashMap::new();
        let mut group_for = |mut matches: Vec<usize>| -> Arc<T> {
            // Resolve matching patterns to their sources. Patterns of the same source are
            // adjacent, so this only needs to remove consecutive duplicates.
            for i in &mut matches {
                *i = entries[*i].0;
            }
            matches.dedup();

            let group = created_groups.entry(matches).or_insert_with_key(|matches| {
                let mut group = T::default();
                for &i in matches {
                    add(&mut group, &sources[i]);
                }
                Arc::new(group)
            });
            Arc::clone(group)
        };

        // Do the same thing one level higher up, forming groups of overlapping schema patterns.
        let mut table_indexes: HashMap<Vec<usize>, Arc<TableIndex<T>>> = HashMap::new();
        let mut table_index_for = |schema_matches: Vec<usize>| -> Arc<TableIndex<T>> {
            let index = table_indexes
                .entry(schema_matches)
                .or_insert_with_key(|schema_matches| {
                    let tables = schema_matches.iter().map(|&i| (i, &entries[i].1.table));
                    Arc::new(NameIndex::build(tables, &mut group_for))
                });
            Arc::clone(index)
        };

        let schemas = entries.iter().enumerate().map(|(i, (_, source))| {
            assert!(source.connection_tag == TablePattern::DEFAULT_TAG);
            (i, &source.schema)
        });
        let by_schema = NameIndex::build(schemas, &mut table_index_for);

        Self { by_schema }
    }
}

impl<T> TablePatternIndex<T> {
    /// Returns the value for the patterns matching the given table, or [None] if no pattern
    /// matches it.
    pub fn lookup(&self, table: SourceTableRef) -> Option<&Arc<T>> {
        debug_assert!(table.connection_tag == TablePattern::DEFAULT_TAG);
        self.by_schema.lookup(table.schema)?.lookup(table.name)
    }
}

/// Resolves a schema or table name to a value associated with all [NameMatcher]s matching it.
///
/// Nodes exist for every name used in a matcher (and the empty name). For each node, we precompute
/// the value for the name itself and for names that extend it, so resolving a name only requires
/// finding the longest node that is a prefix of the name.
///
/// For example, a name index built on `foo`, `bar` and `%` has lengths `3, 0` and the following
/// entries in nodes:
///
/// | Key |  Exact | Prefix |
/// |-----|--------|--------|
/// | foo | foo, % |      % |
/// | bar | bar, % |      % |
/// |   % |      % |      % |
#[derive(Debug)]
struct NameIndex<T> {
    /// Distinct lengths of node names, in descending order.
    lengths: Box<[usize]>,
    nodes: HashMap<Box<str>, NameNode<T>>,
}

#[derive(Debug)]
struct NameNode<T> {
    /// The value for a name equal to the name of this node, or [None] if no matcher matches it.
    exact: Option<T>,
    /// The value for a name that this node is a strict prefix of, or [None] if no matcher
    /// matches such names.
    prefix: Option<T>,
}

impl<T> NameIndex<T> {
    /// Builds an index for `(id, matcher)` pairs.
    ///
    /// `resolve` is invoked with the sorted, non-empty ids of matchers matching a node.
    fn build<'a>(
        matchers: impl IntoIterator<Item = (usize, &'a NameMatcher)>,
        mut resolve: impl FnMut(Vec<usize>) -> T,
    ) -> Self {
        let mut matchers: Vec<_> = matchers.into_iter().collect();
        matchers.sort_by_key(|(id, _)| *id);

        let mut names: Vec<&str> = matchers.iter().map(|(_, m)| m.name.as_str()).collect();
        names.push("");
        names.sort_unstable();
        names.dedup();

        let nodes: HashMap<Box<str>, NameNode<T>> = names
            .into_iter()
            .filter_map(|name| {
                let mut exact = vec![];
                let mut prefix = vec![];
                for &(id, matcher) in &matchers {
                    if matcher.wildcard && name.starts_with(&matcher.name) {
                        exact.push(id);
                        prefix.push(id);
                    } else if !matcher.wildcard && name == matcher.name {
                        exact.push(id);
                    }
                }

                if exact.is_empty() {
                    // Prefix matches are a subset of exact matches, so neither has a value.
                    return None;
                }

                let node = NameNode {
                    exact: Some(resolve(exact)),
                    prefix: (!prefix.is_empty()).then(|| resolve(prefix)),
                };
                Some((name.into(), node))
            })
            .collect();

        let mut lengths: Vec<usize> = nodes.keys().map(|name| name.len()).collect();
        lengths.sort_unstable_by(|a, b| b.cmp(a));
        lengths.dedup();

        Self {
            lengths: lengths.into(),
            nodes,
        }
    }

    fn lookup(&self, name: &str) -> Option<&T> {
        for &len in &self.lengths {
            // A prefix that doesn't end on a char boundary can't be equal to a node name.
            let Some(prefix) = name.get(..len) else {
                continue;
            };

            if let Some(node) = self.nodes.get(prefix) {
                let value = if len == name.len() {
                    &node.exact
                } else {
                    &node.prefix
                };
                return value.as_ref();
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{NameMatcher, SourceTableRef, TablePattern, TablePatternIndex};

    fn name_matcher(name: &str) -> NameMatcher {
        let Ok(matcher) = name.parse();
        matcher
    }

    #[test]
    fn parse() {
        let Ok(matcher) = "users".parse::<NameMatcher>();
        assert_eq!(matcher.name, "users");
        assert!(!matcher.wildcard);

        let Ok(matcher) = "users%".parse::<NameMatcher>();
        assert_eq!(matcher.name, "users");
        assert!(matcher.wildcard);
    }

    fn pattern(schema: &str, table: &str) -> TablePattern {
        TablePattern {
            connection_tag: TablePattern::DEFAULT_TAG.to_string(),
            schema: name_matcher(schema),
            table: name_matcher(table),
        }
    }

    fn index(patterns: &[TablePattern]) -> TablePatternIndex<Vec<usize>> {
        let sources: Vec<_> = patterns.iter().enumerate().collect();
        TablePatternIndex::build(
            &sources,
            |(_, pattern)| [*pattern],
            |ids: &mut Vec<usize>, (id, _)| ids.push(*id),
        )
    }

    fn lookup<'a>(
        index: &'a TablePatternIndex<Vec<usize>>,
        schema: &str,
        name: &str,
    ) -> Option<&'a [usize]> {
        index
            .lookup(SourceTableRef {
                connection_tag: TablePattern::DEFAULT_TAG,
                schema,
                name,
            })
            .map(|v| v.as_slice())
    }

    #[test]
    fn exact() {
        let index = index(&[pattern("public", "users")]);

        assert_eq!(lookup(&index, "public", "users"), Some(&[0][..]));
        assert_eq!(lookup(&index, "public", "users_table"), None);
        assert_eq!(lookup(&index, "public", "user"), None);
        assert_eq!(lookup(&index, "public", "accounts"), None);
        assert_eq!(lookup(&index, "other", "users"), None);
    }

    #[test]
    fn overlapping_table_patterns() {
        let index = index(&[
            pattern("public", "%"),
            pattern("public", "user%"),
            pattern("public", "users"),
        ]);

        assert_eq!(lookup(&index, "public", "accounts"), Some(&[0][..]));
        assert_eq!(lookup(&index, "public", ""), Some(&[0][..]));
        assert_eq!(lookup(&index, "public", "use"), Some(&[0][..]));
        assert_eq!(lookup(&index, "public", "user"), Some(&[0, 1][..]));
        assert_eq!(lookup(&index, "public", "user_x"), Some(&[0, 1][..]));
        assert_eq!(lookup(&index, "public", "users"), Some(&[0, 1, 2][..]));
        assert_eq!(lookup(&index, "public", "users_2"), Some(&[0, 1][..]));
    }

    #[test]
    fn schema_wildcards() {
        let index = index(&[
            pattern("tenant_%", "users"),
            pattern("tenant_a", "user%"),
            pattern("%", "accounts"),
        ]);

        assert_eq!(lookup(&index, "tenant_a", "users"), Some(&[0, 1][..]));
        assert_eq!(lookup(&index, "tenant_b", "users"), Some(&[0][..]));
        assert_eq!(lookup(&index, "tenant_a", "user_x"), Some(&[1][..]));
        assert_eq!(lookup(&index, "tenant_b", "user_x"), None);
        assert_eq!(lookup(&index, "tenant_b", "accounts"), Some(&[2][..]));
        assert_eq!(lookup(&index, "public", "accounts"), Some(&[2][..]));
        assert_eq!(lookup(&index, "public", "users"), None);
    }

    #[test]
    fn duplicate_patterns() {
        let index = index(&[pattern("public", "users"), pattern("public", "users")]);

        assert_eq!(lookup(&index, "public", "users"), Some(&[0, 1][..]));
    }

    #[test]
    fn shares_values_for_same_combination() {
        let index = index(&[pattern("public", "user%")]);
        let a = index.lookup(SourceTableRef {
            connection_tag: TablePattern::DEFAULT_TAG,
            schema: "public",
            name: "users_a",
        });
        let b = index.lookup(SourceTableRef {
            connection_tag: TablePattern::DEFAULT_TAG,
            schema: "public",
            name: "user",
        });

        assert!(Arc::ptr_eq(a.unwrap(), b.unwrap()));
    }

    #[test]
    fn non_char_boundary() {
        let index = index(&[pattern("public", "a%")]);

        // The first byte of "ä" is not a char boundary.
        assert_eq!(lookup(&index, "public", "äb"), None);
        assert_eq!(lookup(&index, "public", "ab"), Some(&[0][..]));
    }
}
