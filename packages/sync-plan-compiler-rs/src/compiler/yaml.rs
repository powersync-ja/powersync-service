#[derive(Debug, Clone)]
struct ParsedSyncYaml {
    edition: i64,
    sync_config_compiler: bool,
    streams: Vec<ParsedStream>,
}

#[derive(Debug, Clone)]
struct ParsedStream {
    name: String,
    auto_subscribe: bool,
    priority: u8,
    queries: Vec<String>,
    with_queries: HashMap<String, String>,
}

fn parse_sync_yaml(yaml: &str) -> Result<ParsedSyncYaml, CompilerError> {
    let mut edition = 1;
    let mut sync_config_compiler = false;
    let mut streams = Vec::<ParsedStream>::new();

    enum Section {
        None,
        Config,
        Streams,
    }

    let mut section = Section::None;
    let mut current_stream: Option<usize> = None;
    let mut in_queries = false;

    #[derive(Debug, Clone)]
    enum BlockTarget {
        Query { stream_index: usize },
        With { stream_index: usize, name: String },
    }

    let mut in_with = false;
    let mut block_target: Option<BlockTarget> = None;
    let mut block_indent: usize = 0;
    let mut block_content_indent: usize = 0;
    let mut block_text = String::new();

    for (line_index, raw_line) in yaml.lines().enumerate() {
        let indent = raw_line.chars().take_while(|c| *c == ' ').count();

        if let Some(target) = block_target.clone() {
            if indent > block_indent {
                let content = if raw_line.len() >= block_content_indent {
                    &raw_line[block_content_indent..]
                } else {
                    ""
                };
                block_text.push_str(content.trim_end());
                block_text.push('\n');
                continue;
            }

            let value = block_text.trim_end_matches('\n').to_string();
            match target {
                BlockTarget::Query { stream_index } => {
                    streams[stream_index].queries.push(value);
                }
                BlockTarget::With { stream_index, name } => {
                    streams[stream_index].with_queries.insert(name, value);
                }
            }
            block_target = None;
            block_text.clear();
        }

        let line = raw_line.trim_end();
        if line.trim().is_empty() {
            continue;
        }

        let stripped = strip_comment(line);
        let trimmed = stripped.trim();
        if trimmed.is_empty() {
            continue;
        }

        if indent == 0 {
            current_stream = None;
            in_queries = false;
            in_with = false;

            match trimmed {
                "config:" => {
                    section = Section::Config;
                    continue;
                }
                "streams:" => {
                    section = Section::Streams;
                    continue;
                }
                _ => {
                    section = Section::None;
                    continue;
                }
            }
        }

        match section {
            Section::Config => {
                if indent != 2 {
                    continue;
                }
                let (key, value) = parse_key_value(trimmed)
                    .map_err(|e| CompilerError::Parse(format!("line {}: {e}", line_index + 1)))?;

                match key {
                    "edition" => {
                        edition = value.parse::<i64>().map_err(|_| {
                            CompilerError::Parse(format!(
                                "line {}: config.edition must be an integer",
                                line_index + 1
                            ))
                        })?;
                    }
                    "sync_config_compiler" => {
                        sync_config_compiler = parse_bool(value).ok_or_else(|| {
                            CompilerError::Parse(format!(
                                "line {}: config.sync_config_compiler must be true/false",
                                line_index + 1
                            ))
                        })?;
                    }
                    _ => {}
                }
            }
            Section::Streams => {
                if indent == 2 && trimmed.ends_with(':') {
                    let name = trimmed.trim_end_matches(':').trim().to_string();
                    streams.push(ParsedStream {
                        name,
                        auto_subscribe: false,
                        priority: 3,
                        queries: Vec::new(),
                        with_queries: HashMap::new(),
                    });
                    current_stream = Some(streams.len() - 1);
                    in_queries = false;
                    in_with = false;
                    continue;
                }

                let Some(stream_index) = current_stream else {
                    continue;
                };

                if indent == 4 {
                    let (key, value) = parse_key_value(trimmed).map_err(|e| {
                        CompilerError::Parse(format!("line {}: {e}", line_index + 1))
                    })?;

                    match key {
                        "auto_subscribe" => {
                            streams[stream_index].auto_subscribe =
                                parse_bool(value).ok_or_else(|| {
                                    CompilerError::Parse(format!(
                                        "line {}: auto_subscribe must be true/false",
                                        line_index + 1
                                    ))
                                })?;
                            in_queries = false;
                            in_with = false;
                        }
                        "priority" => {
                            let parsed = value.parse::<u8>().map_err(|_| {
                                CompilerError::Parse(format!(
                                    "line {}: priority must be an integer",
                                    line_index + 1
                                ))
                            })?;
                            if parsed > 3 {
                                return Err(CompilerError::Parse(format!(
                                    "line {}: priority must be between 0 and 3",
                                    line_index + 1
                                )));
                            }
                            streams[stream_index].priority = parsed;
                            in_queries = false;
                            in_with = false;
                        }
                        "query" => {
                            if value == "|" {
                                block_target = Some(BlockTarget::Query { stream_index });
                                block_indent = indent;
                                block_content_indent = indent + 2;
                                block_text.clear();
                            } else {
                                streams[stream_index]
                                    .queries
                                    .push(unquote(value).to_string());
                            }
                            in_queries = false;
                            in_with = false;
                        }
                        "queries" => {
                            in_queries = true;
                            in_with = false;
                        }
                        "with" => {
                            in_with = true;
                            in_queries = false;
                        }
                        _ => {
                            in_queries = false;
                            in_with = false;
                        }
                    }
                } else if indent == 6 && in_queries {
                    let item = trimmed.strip_prefix("-").ok_or_else(|| {
                        CompilerError::Parse(format!(
                            "line {}: expected list item in queries",
                            line_index + 1
                        ))
                    })?;
                    streams[stream_index]
                        .queries
                        .push(unquote(item.trim()).to_string());
                } else if indent == 6 && in_with {
                    let (name, value) = parse_key_value(trimmed).map_err(|e| {
                        CompilerError::Parse(format!("line {}: {e}", line_index + 1))
                    })?;

                    if value == "|" {
                        block_target = Some(BlockTarget::With {
                            stream_index,
                            name: name.to_string(),
                        });
                        block_indent = indent;
                        block_content_indent = indent + 2;
                        block_text.clear();
                    } else {
                        streams[stream_index]
                            .with_queries
                            .insert(name.to_string(), unquote(value).to_string());
                    }
                }
            }
            Section::None => {}
        }
    }

    if let Some(target) = block_target {
        let value = block_text.trim_end_matches('\n').to_string();
        match target {
            BlockTarget::Query { stream_index } => {
                streams[stream_index].queries.push(value);
            }
            BlockTarget::With { stream_index, name } => {
                streams[stream_index].with_queries.insert(name, value);
            }
        }
    }

    for stream in &streams {
        if stream.queries.is_empty() {
            return Err(CompilerError::Parse(format!(
                "stream '{}' must define query or queries",
                stream.name
            )));
        }
    }

    Ok(ParsedSyncYaml {
        edition,
        sync_config_compiler,
        streams,
    })
}

fn strip_comment(line: &str) -> String {
    let mut in_single = false;
    let mut result = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            in_single = !in_single;
            result.push(ch);
            continue;
        }

        if ch == '#' && !in_single {
            break;
        }

        result.push(ch);
    }

    result
}

fn parse_key_value(input: &str) -> Result<(&str, &str), &'static str> {
    let Some((key, value)) = input.split_once(':') else {
        return Err("expected key: value");
    };

    Ok((key.trim(), value.trim()))
}

fn parse_bool(input: &str) -> Option<bool> {
    match input {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn unquote(input: &str) -> &str {
    if input.len() >= 2
        && ((input.starts_with('"') && input.ends_with('"'))
            || (input.starts_with('\'') && input.ends_with('\'')))
    {
        &input[1..input.len() - 1]
    } else {
        input
    }
}

fn parse_table_pattern(
    table: &str,
    schema: Option<&str>,
    default_schema: &str,
) -> SerializedTablePattern {
    let table = unquote(table).to_string();
    let _ = default_schema;
    SerializedTablePattern {
        connection: schema.map(|_| "default".to_string()),
        schema: schema.map(|value| value.to_string()),
        table,
    }
}

fn expand_with_queries(query_sql: &str, with_queries: &HashMap<String, String>) -> String {
    let mut expanded = query_sql.to_string();
    for (name, cte_sql) in with_queries {
        let inferred_column = infer_single_output_column(cte_sql);
        let from_pattern = format!("FROM {name}");
        let from_replacement = format!("FROM ({cte_sql}) AS {name}");
        expanded = expanded.replace(&from_pattern, &from_replacement);

        let from_pattern_lower = format!("from {name}");
        let from_replacement_lower = format!("from ({cte_sql}) as {name}");
        expanded = expanded.replace(&from_pattern_lower, &from_replacement_lower);

        let in_pattern = format!("IN {name}");
        let in_replacement = if let Some(column) = &inferred_column {
            format!("IN (SELECT {column} FROM ({cte_sql}) AS {name})")
        } else {
            format!("IN (SELECT * FROM ({cte_sql}) AS {name})")
        };
        expanded = expanded.replace(&in_pattern, &in_replacement);

        let in_pattern_lower = format!("in {name}");
        let in_replacement_lower = if let Some(column) = &inferred_column {
            format!("in (select {column} from ({cte_sql}) as {name})")
        } else {
            format!("in (select * from ({cte_sql}) as {name})")
        };
        expanded = expanded.replace(&in_pattern_lower, &in_replacement_lower);
    }

    expanded
}

fn infer_single_output_column(sql: &str) -> Option<String> {
    let normalized = normalize_sql_for_pg_query(sql);
    let parsed = parse_select_query(&normalized).ok()?;
    let column = parsed.columns.first()?;
    let SelectItemAst::Expr { expr, alias } = column else {
        return None;
    };
    alias
        .clone()
        .or_else(|| column_alias_from_expr(expr))
        .or_else(|| resolve_identifier(expr, &parsed).map(|column| column.column))
}

fn normalize_sql_for_pg_query(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let mut output = String::new();
    let mut cursor = 0usize;

    while let Some(found) = lower[cursor..].find(" in array[") {
        let start = cursor + found;
        output.push_str(&sql[cursor..start]);
        output.push_str(" IN (");

        let array_start = start + " in array[".len();
        let mut index = array_start;
        let mut bracket_depth = 1i32;
        let chars = sql.as_bytes();

        while index < chars.len() && bracket_depth > 0 {
            match chars[index] {
                b'[' => bracket_depth += 1,
                b']' => bracket_depth -= 1,
                _ => {}
            }
            index += 1;
        }

        if bracket_depth != 0 {
            output.push_str(&sql[array_start..]);
            cursor = sql.len();
            break;
        }

        output.push_str(&sql[array_start..index - 1]);
        output.push(')');
        cursor = index;
    }

    output.push_str(&sql[cursor..]);
    output
}
