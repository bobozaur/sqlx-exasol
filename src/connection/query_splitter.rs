pub fn split_exasol_queries(input: &str) -> Vec<&str> {
    let mut chars = input.char_indices().peekable();
    let mut state = State::InQuery;
    let mut statements = Vec::new();
    let mut start = 0;

    while let Some((i, c)) = chars.next() {
        let peek = chars.peek().map(|(_, c)| c);

        match (state, c, peek) {
            // Line comment start
            (State::InQuery, '-', Some('-')) => {
                chars.next();
                state = State::InLineComment;
            }
            // Block comment start
            (State::InQuery, '/', Some('*')) => {
                chars.next();
                state = State::InBlockComment;
            }
            // Double quote start
            (State::InQuery, '"', _) => state = State::InDoubleQuote,
            // Single quote start
            (State::InQuery, '\'', _) => state = State::InSingleQuote,
            // Statement end
            (State::InQuery, ';', _) => {
                let stmt = input[start..=i].trim();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                start = i + 1;
            }
            // Skip escaped double quote
            (State::InDoubleQuote, '"', Some('"')) => {
                chars.next();
            }
            // Skip escaped single quote
            (State::InSingleQuote, '\'', Some('\'')) => {
                chars.next();
            }
            // Double quote end
            (State::InDoubleQuote, '"', _) => state = State::InQuery,
            // Single quote end
            (State::InSingleQuote, '\'', _) => state = State::InQuery,
            // Line comment end
            (State::InLineComment, '\n', _) => state = State::InQuery,
            // Block comment end
            (State::InBlockComment, '*', Some('/')) => {
                chars.next();
                state = State::InQuery;
            }
            _ => (),
        }
    }

    // Add final part if anything remains after the last `;`
    let rest = input[start..].trim();
    if !rest.is_empty() {
        statements.push(rest);
    }

    statements
}

#[derive(Clone, Copy)]
enum State {
    InQuery,
    InLineComment,
    InBlockComment,
    InDoubleQuote,
    InSingleQuote,
}
