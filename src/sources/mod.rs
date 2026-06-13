pub mod syslog;

#[cfg(feature = "journald")]
pub mod journald;

/// Strip ANSI / VT100 escape sequences from `s`.
///
/// Handles CSI sequences (`ESC [ <params> <final-byte>`) and plain two-byte
/// sequences (`ESC <char>`).  Applied to all log bodies so that processes
/// whose output includes terminal colour codes (e.g. tracing-subscriber
/// without `with_ansi(false)`) produce clean text in the Spotflow dashboard.
pub(super) fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        match chars.peek() {
            Some(&'[') => {
                // CSI sequence: ESC [ <params/intermediates> <final-byte>
                chars.next(); // consume '['
                for ch in chars.by_ref() {
                    if ch.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            Some(_) => {
                // Simple two-byte sequence (e.g. ESC M): skip next char.
                chars.next();
            }
            None => {}
        }
    }
    out
}
