pub fn split_queries(input: &str) -> Vec<&str> {
    let mut chars = input.char_indices().peekable();
    let mut state = State::Query;
    let mut statements = Vec::new();
    let mut start = 0;

    while let Some((i, c)) = chars.next() {
        let peek = chars.peek().map(|(_, c)| c);

        #[allow(clippy::match_same_arms, reason = "better readability if split")]
        match (state, c, peek) {
            // Line comment start
            (State::Query, '-', Some('-')) => {
                chars.next();
                state = State::LineComment;
            }
            // Block comment start
            (State::Query, '/', Some('*')) => {
                chars.next();
                state = State::BlockComment;
            }
            // Double quote start
            (State::Query, '"', _) => state = State::DoubleQuote,
            // Single quote start
            (State::Query, '\'', _) => state = State::SingleQuote,
            // Statement end
            (State::Query, ';', _) => {
                let stmt = input[start..=i].trim();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                start = i + 1;
            }
            // Skip escaped double quote
            (State::DoubleQuote, '"', Some('"')) => {
                chars.next();
            }
            // Skip escaped single quote
            (State::SingleQuote, '\'', Some('\'')) => {
                chars.next();
            }
            // Double quote end
            (State::DoubleQuote, '"', _) => state = State::Query,
            // Single quote end
            (State::SingleQuote, '\'', _) => state = State::Query,
            // Line comment end
            (State::LineComment, '\n', _) => state = State::Query,
            // Block comment end
            (State::BlockComment, '*', Some('/')) => {
                chars.next();
                state = State::Query;
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
    Query,
    LineComment,
    BlockComment,
    DoubleQuote,
    SingleQuote,
}
