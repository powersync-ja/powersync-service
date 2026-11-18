use std::{collections::HashMap, str::FromStr, sync::Arc};

/// Matches a table or schema name.
#[derive(Clone, Debug)]
pub struct NameMatcher {
    pub name: String,
    pub wildcard: bool,
}

impl FromStr for NameMatcher {
    type Err = !;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, wildcard) = if s.ends_with('%') {
            (&s[0..s.len() - 1], true)
        } else {
            (s, false)
        };

        Ok(Self {
            name: name.to_string(),
            wildcard,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TablePattern {
    pub connection_tag: String,
    pub schema: NameMatcher,
    pub table: NameMatcher,
}

/// Reference to a source table associated with a replicated row.
#[derive(Clone, Copy, Debug)]
pub struct SourceTableRef<'a> {
    /// A tag associated with the connection.
    pub connection_tag: &'a str,
    /// The name of the schema.
    pub schema: &'a str,
    /// The name of the table.
    pub name: &'a str,
}

/// A static index resolving source tables to values associated with the set of [TablePattern]s
/// matching them.
///
/// The index is built upfront for a fixed list of patterns. For every unique combination of
/// patterns that can match a source table, a single value is created and shared between all
/// tables resolving to that combination.
#[derive(Debug)]
pub struct TablePatternIndex<T> {
    by_connection_tag: HashMap<Box<str>, NameIndex<Arc<NameIndex<Option<Arc<T>>>>>>,
}

impl<T> TablePatternIndex<T> {
    /// Builds an index for the given patterns.
    ///
    /// `create` is invoked once for each unique, non-empty combination of matching patterns,
    /// receiving the sorted indices of those patterns in `patterns`.
    pub fn build(patterns: &[TablePattern], mut create: impl FnMut(&[usize]) -> T) -> Self {
        let mut patterns_by_tag: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, pattern) in patterns.iter().enumerate() {
            patterns_by_tag
                .entry(&pattern.connection_tag)
                .or_default()
                .push(i);
        }

        let mut values: HashMap<Vec<usize>, Arc<T>> = HashMap::new();
        let mut table_indexes: HashMap<Vec<usize>, Arc<NameIndex<Option<Arc<T>>>>> = HashMap::new();

        let by_connection_tag = patterns_by_tag
            .into_iter()
            .map(|(tag, ids)| {
                let schema_index = NameIndex::build(
                    ids.iter().map(|&i| (i, &patterns[i].schema)),
                    |schema_matches| {
                        table_indexes
                            .entry(schema_matches)
                            .or_insert_with_key(|schema_matches| {
                                Arc::new(NameIndex::build(
                                    schema_matches.iter().map(|&i| (i, &patterns[i].table)),
                                    |matches| {
                                        if matches.is_empty() {
                                            return None;
                                        }

                                        Some(
                                            values
                                                .entry(matches)
                                                .or_insert_with_key(|k| Arc::new(create(k)))
                                                .clone(),
                                        )
                                    },
                                ))
                            })
                            .clone()
                    },
                );

                (tag.into(), schema_index)
            })
            .collect();

        Self { by_connection_tag }
    }

    /// Returns the value for the patterns matching the given table, or [None] if no pattern
    /// matches it.
    pub fn lookup(&self, table: SourceTableRef) -> Option<&Arc<T>> {
        self.by_connection_tag
            .get(table.connection_tag)?
            .lookup(table.schema)
            .lookup(table.name)
            .as_ref()
    }
}

/// Resolves a schema or table name to a value associated with all [NameMatcher]s matching it.
///
/// Nodes exist for the empty name and every name used in a matcher. For each node, we precompute
/// the value for the name itself and for names that extend it, so resolving a name only requires
/// finding the longest node that is a prefix of the name.
#[derive(Debug)]
struct NameIndex<T> {
    /// Distinct lengths of node names, in descending order. Always ends with `0`.
    lengths: Box<[usize]>,
    nodes: HashMap<Box<str>, NameNode<T>>,
}

#[derive(Debug)]
struct NameNode<T> {
    /// The value for a name equal to the name of this node.
    exact: T,
    /// The value for a name that this node is a strict prefix of.
    prefix: T,
}

impl<T> NameIndex<T> {
    /// Builds an index for `(id, matcher)` pairs.
    ///
    /// `resolve` is invoked with the sorted ids of matchers matching a node.
    fn build<'a>(
        matchers: impl Iterator<Item = (usize, &'a NameMatcher)>,
        mut resolve: impl FnMut(Vec<usize>) -> T,
    ) -> Self {
        let mut matchers: Vec<_> = matchers.collect();
        matchers.sort_by_key(|(id, _)| *id);

        let mut names: Vec<&str> = matchers.iter().map(|(_, m)| m.name.as_str()).collect();
        names.push("");
        names.sort_unstable();
        names.dedup();

        let mut lengths: Vec<usize> = names.iter().map(|name| name.len()).collect();
        lengths.sort_unstable_by(|a, b| b.cmp(a));
        lengths.dedup();

        let nodes = names
            .into_iter()
            .map(|name| {
                let mut exact = vec![];
                let mut prefix = vec![];
                for (id, matcher) in &matchers {
                    if matcher.wildcard && name.starts_with(&matcher.name) {
                        exact.push(*id);
                        prefix.push(*id);
                    } else if !matcher.wildcard && name == matcher.name {
                        exact.push(*id);
                    }
                }

                let node = NameNode {
                    exact: resolve(exact),
                    prefix: resolve(prefix),
                };
                (name.into(), node)
            })
            .collect();

        Self {
            lengths: lengths.into(),
            nodes,
        }
    }

    fn lookup(&self, name: &str) -> &T {
        for &len in &self.lengths {
            // A prefix that doesn't end on a char boundary can't be equal to a node name.
            let Some(prefix) = name.get(..len) else {
                continue;
            };

            if let Some(node) = self.nodes.get(prefix) {
                return if len == name.len() {
                    &node.exact
                } else {
                    &node.prefix
                };
            }
        }

        unreachable!("Node for empty name should exist")
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{NameMatcher, SourceTableRef, TablePattern, TablePatternIndex};

    #[test]
    fn parse() {
        let Ok(matcher) = "users".parse::<NameMatcher>();
        assert_eq!(matcher.name, "users");
        assert!(!matcher.wildcard);

        let Ok(matcher) = "users%".parse::<NameMatcher>();
        assert_eq!(matcher.name, "users");
        assert!(matcher.wildcard);
    }

    fn pattern(connection_tag: &str, schema: &str, table: &str) -> TablePattern {
        let Ok(schema) = schema.parse();
        let Ok(table) = table.parse();

        TablePattern {
            connection_tag: connection_tag.to_string(),
            schema,
            table,
        }
    }

    fn index(patterns: &[TablePattern]) -> TablePatternIndex<Vec<usize>> {
        TablePatternIndex::build(patterns, |ids| ids.to_vec())
    }

    fn lookup<'a>(
        index: &'a TablePatternIndex<Vec<usize>>,
        schema: &str,
        name: &str,
    ) -> Option<&'a [usize]> {
        index
            .lookup(SourceTableRef {
                connection_tag: "tag",
                schema,
                name,
            })
            .map(|v| v.as_slice())
    }

    #[test]
    fn exact() {
        let index = index(&[pattern("tag", "public", "users")]);

        assert_eq!(lookup(&index, "public", "users"), Some(&[0][..]));
        assert_eq!(lookup(&index, "public", "users_table"), None);
        assert_eq!(lookup(&index, "public", "user"), None);
        assert_eq!(lookup(&index, "public", "accounts"), None);
        assert_eq!(lookup(&index, "other", "users"), None);
    }

    #[test]
    fn connection_tag() {
        let index = index(&[pattern("tag", "public", "users")]);

        assert!(
            index
                .lookup(SourceTableRef {
                    connection_tag: "another",
                    schema: "public",
                    name: "users",
                })
                .is_none()
        );
    }

    #[test]
    fn overlapping_table_patterns() {
        let index = index(&[
            pattern("tag", "public", "%"),
            pattern("tag", "public", "user%"),
            pattern("tag", "public", "users"),
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
            pattern("tag", "tenant_%", "users"),
            pattern("tag", "tenant_a", "user%"),
            pattern("tag", "%", "accounts"),
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
        let index = index(&[
            pattern("tag", "public", "users"),
            pattern("tag", "public", "users"),
        ]);

        assert_eq!(lookup(&index, "public", "users"), Some(&[0, 1][..]));
    }

    #[test]
    fn shares_values_for_same_combination() {
        let index = index(&[pattern("tag", "public", "user%")]);
        let a = index.lookup(SourceTableRef {
            connection_tag: "tag",
            schema: "public",
            name: "users_a",
        });
        let b = index.lookup(SourceTableRef {
            connection_tag: "tag",
            schema: "public",
            name: "user",
        });

        assert!(Arc::ptr_eq(a.unwrap(), b.unwrap()));
    }

    #[test]
    fn non_char_boundary() {
        let index = index(&[pattern("tag", "public", "a%")]);

        // The first byte of "ä" is not a char boundary.
        assert_eq!(lookup(&index, "public", "äb"), None);
        assert_eq!(lookup(&index, "public", "ab"), Some(&[0][..]));
    }
}
