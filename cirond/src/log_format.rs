use std::fmt::{self, Write as _};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Event formatter used for cirond's own log output.
///
/// It differs from tracing-subscriber's default formatter in two ways, both
/// aimed at making forwarded child-process logs (`log_forward = true`)
/// readable:
/// - the level is followed by a tab instead of being space-padded, so it
///   still lines up in a column when viewed with a proportional-width font
///   (space padding only aligns in a monospace font);
/// - when the event carries a `process` field (set by the child log
///   forwarder in `process.rs`), that program's name is shown in place of
///   the Rust module target, e.g. `myprogram: ...` instead of
///   `cirond::child: ...`.
pub struct CironFormatter;

#[derive(Default)]
struct EventVisitor {
    process: Option<String>,
    message: String,
    fields: String,
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "message" => {
                let _ = write!(self.message, "{:?}", value);
            }
            "process" => {
                let mut s = String::new();
                let _ = write!(s, "{:?}", value);
                self.process = Some(s);
            }
            name => {
                let _ = write!(self.fields, " {}={:?}", name, value);
            }
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "process" => self.process = Some(value.to_string()),
            name => {
                let _ = write!(self.fields, " {}={:?}", name, value);
            }
        }
    }
}

impl<S, N> FormatEvent<S, N> for CironFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        SystemTime.format_time(&mut writer)?;
        writer.write_char(' ')?;

        let meta = event.metadata();
        if writer.has_ansi_escapes() {
            let color = match *meta.level() {
                Level::TRACE => "35",
                Level::DEBUG => "34",
                Level::INFO => "32",
                Level::WARN => "33",
                Level::ERROR => "31",
            };
            write!(writer, "\x1b[{}m{}\x1b[0m", color, meta.level())?;
        } else {
            write!(writer, "{}", meta.level())?;
        }
        // A tab (rather than space-padding to a fixed width) keeps the
        // following column aligned even in fonts where "INFO" and "ERROR"
        // don't occupy the same visual width.
        writer.write_char('\t')?;

        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);

        let target = visitor.process.as_deref().unwrap_or_else(|| meta.target());
        write!(writer, "{}: {}{}", target, visitor.message, visitor.fields)?;

        writeln!(writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::SubscriberExt as _;

    #[derive(Clone, Default)]
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for TestWriter {
        type Writer = TestWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Runs `emit` against a subscriber using [`CironFormatter`] and returns
    /// everything it wrote, with ANSI colors disabled so assertions can
    /// match on plain text.
    fn capture(emit: impl FnOnce()) -> String {
        let writer = TestWriter::default();
        let layer = tracing_subscriber::fmt::layer()
            .event_format(CironFormatter)
            .with_writer(writer.clone())
            .with_ansi(false);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, emit);

        String::from_utf8(writer.0.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn shows_process_name_instead_of_target() {
        let name = "myprogram".to_string();
        let output = capture(|| {
            tracing::info!(target: "cirond::child", process = %name, stream = "stdout", "hello world");
        });

        assert!(
            output.contains("myprogram: hello world"),
            "output was: {output:?}"
        );
        assert!(
            !output.contains("cirond::child"),
            "target should be replaced by the process name, output was: {output:?}"
        );
    }

    #[test]
    fn falls_back_to_target_without_process_field() {
        let output = capture(|| {
            tracing::info!(target: "cirond::process", "plain message");
        });

        assert!(
            output.contains("cirond::process: plain message"),
            "output was: {output:?}"
        );
    }

    #[test]
    fn level_is_followed_by_a_tab_not_padding() {
        let output = capture(|| {
            tracing::info!("tabbed");
        });
        let error_output = capture(|| {
            tracing::error!("tabbed");
        });

        assert!(output.contains("INFO\t"), "output was: {output:?}");
        assert!(
            error_output.contains("ERROR\t"),
            "output was: {error_output:?}"
        );
    }

    #[test]
    fn extra_fields_are_appended_after_the_message() {
        let output = capture(|| {
            tracing::info!(target: "cirond::child", process = %"proc", stream = "stdout", "msg");
        });

        assert!(
            output.contains(r#"msg stream="stdout""#),
            "output was: {output:?}"
        );
    }

    #[test]
    fn process_field_passed_as_str_is_also_recognized() {
        // `process = "literal"` (no `%` sigil) takes a different code path
        // (`Visit::record_str`) than `process = %some_string`
        // (`Visit::record_debug`); both must resolve to the program name.
        let output = capture(|| {
            tracing::info!(target: "cirond::child", process = "literal", "hi");
        });

        assert!(output.contains("literal: hi"), "output was: {output:?}");
    }
}
