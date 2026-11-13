//! ffprobe / ffmpeg for the audio/video branch of Convert: [`probe_duration`] and
//! [`segment_audio`] (mono 16 kHz 64 kbps mp3 segments of `segment_s` seconds, video dropped).
//!
//! Both run the binaries from `PATH` with `tokio::process`, `kill_on_drop` (a step abandoned at
//! its deadline does not leave ffmpeg running), stdout/stderr read through capped buffers. The
//! input may only be opened through the `file` protocol and only by a media demuxer
//! ([`INPUT_FORMATS`]): a crafted playlist (concat, hls, …) cannot make ffmpeg open URLs or other
//! local files.
//!
//! Failures: a binary that is not there is a Retryable `ffmpeg_unavailable` (a deployment fault,
//! not the input's). A non-zero exit whose stderr names a local I/O fault (see
//! [`IO_FAULT_MARKERS`]: a full or failing scratch disk) is a Retryable `ffmpeg_io_error`; any
//! other non-zero exit is an InvalidRequest `audio_decode_failed` (bad media). Both carry the
//! tool's stderr (head and tail).
//!
//! The ffprobe duration is the container's claim and an uploader controls it (a VBR mp3 without
//! a Xing header, a doctored header). [`segment_audio`] therefore bounds the decode itself
//! (`-t max+1`) and fails `too_long` on the decoded length.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::error::StepError;
use crate::providers::ErrorClass;

/// stderr kept in an error message (its tail: ffmpeg prints the reason last).
const MAX_STDERR_CHARS: usize = 800;
/// Output bytes kept per stream of a tool run: the first and the last `CAPTURE_EDGE` bytes; the
/// middle is read and discarded (a crafted input can make ffmpeg log without end).
const CAPTURE_EDGE: usize = 32 * 1024;

/// Demuxers the input may be opened with (`-format_whitelist`): the media containers Detect
/// accepts as audio/video. No playlist/concat/image2/lavfi demuxer.
pub const INPUT_FORMATS: &str = "mov,mp4,m4a,3gp,3g2,mj2,matroska,webm,wav,w64,mp3,flac,ogg,\
                                 aac,asf,avi,mpegts,caf,aiff,amr";

/// stderr fragments (strerror texts) that mean the run failed on a local I/O fault, not on the
/// media: retried. Heuristic: ffmpeg reports write failures only through these texts.
pub const IO_FAULT_MARKERS: &[&str] = &[
    "No space left on device",
    "Input/output error",
    "Disk quota exceeded",
    "Read-only file system",
];

/// Prefix of a Convert step's scratch dir in the temp root (see [`sweep_scratch_dirs`]).
pub const SCRATCH_PREFIX: &str = "strata-ingest-";

/// Decoded length allowed past `max_s` before `too_long`: the mp3 encoder pads ~45 ms and the
/// cuts land on 36 ms frames, so an input of exactly `max_s` ends a little later.
const LENGTH_TOLERANCE_S: f64 = 0.5;

/// A last segment shorter than this is dropped. The mp3 encoder flushes ~45 ms of padding past
/// the input's end, so an input that ends just past a cut point (e.g. exactly 120 s cut every
/// 60 s: cuts land on 36 ms frame boundaries at 60.012 s, 120.024 s) leaves a sliver segment of
/// padding; sent to a transcriber, near-silence invites a hallucinated sentence. At most this
/// much real audio is lost, too little to hold a word.
pub const MIN_TAIL_SEGMENT_S: f64 = 0.25;

/// One segment file ffmpeg wrote, with its span in the input (from ffmpeg's segment list).
#[derive(Debug, Clone, PartialEq)]
pub struct AudioSegment {
    pub path: PathBuf,
    pub start_s: f64,
    pub end_s: f64,
}

/// The input's duration in seconds (container `format=duration`).
pub async fn probe_duration(input: &Path) -> Result<f64, StepError> {
    let out = run(
        "ffprobe",
        Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-protocol_whitelist",
                "file",
                "-format_whitelist",
                INPUT_FORMATS,
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
            ])
            .arg(input),
    )
    .await?;
    let text = String::from_utf8_lossy(&out);
    let text = text.trim();
    text.parse::<f64>()
        .ok()
        .filter(|d| d.is_finite() && *d >= 0.0)
        .ok_or_else(|| {
            err(
                ErrorClass::InvalidRequest,
                "audio_decode_failed",
                format!("ffprobe reports no duration ({text:?})"),
            )
        })
}

/// `ffmpeg -i in -t <max_s+1> -vn -ac 1 -ar 16000 -b:a 64k -f segment -segment_time <s>
/// out_%04d.mp3` into `out_dir`, plus a csv segment list for each segment's exact start/end.
///
/// The decode stops one second past `max_s`; a decoded length over `max_s` (whatever the
/// container claimed) fails InvalidRequest `too_long`. Segment times are relative to the first
/// segment's start (an input whose start_time is not 0 still starts at 0). Segments in order; a
/// trailing sliver under [`MIN_TAIL_SEGMENT_S`] is left out.
pub async fn segment_audio(
    input: &Path,
    out_dir: &Path,
    segment_s: u64,
    max_s: u64,
) -> Result<Vec<AudioSegment>, StepError> {
    let list = out_dir.join("segments.csv");
    run(
        "ffmpeg",
        Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-protocol_whitelist",
                "file",
                "-format_whitelist",
                INPUT_FORMATS,
                "-i",
            ])
            .arg(input)
            .args([
                "-t",
                &max_s.saturating_add(1).to_string(),
                "-vn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-b:a",
                "64k",
                "-f",
                "segment",
                "-segment_time",
                &segment_s.max(1).to_string(),
                "-segment_list_type",
                "csv",
                "-segment_list",
            ])
            .arg(&list)
            .arg(out_dir.join("out_%04d.mp3")),
    )
    .await?;
    let csv = tokio::fs::read_to_string(&list).await.map_err(|e| {
        err(
            ErrorClass::Retryable,
            "internal",
            format!("reading ffmpeg's segment list: {e}"),
        )
    })?;
    let mut segments = parse_segment_list(&csv, out_dir)?;
    if let Some(first) = segments.first().map(|s| s.start_s) {
        for s in &mut segments {
            s.start_s -= first;
            s.end_s -= first;
        }
    }
    // Checked on the raw list, before a trailing sliver is dropped.
    if let Some(end) = segments.last().map(|s| s.end_s)
        && end > max_s as f64 + LENGTH_TOLERANCE_S
    {
        return Err(err(
            ErrorClass::InvalidRequest,
            "too_long",
            format!("the media decodes to more than {max_s} s (stopped at {end:.1} s)"),
        ));
    }
    if segments.len() > 1
        && segments
            .last()
            .is_some_and(|s| s.end_s - s.start_s < MIN_TAIL_SEGMENT_S)
    {
        segments.pop();
    }
    if segments.is_empty() {
        return Err(err(
            ErrorClass::InvalidRequest,
            "no_audio",
            "ffmpeg produced no audio segments",
        ));
    }
    Ok(segments)
}

/// ffmpeg's csv segment list: `out_0000.mp3,0.000000,60.000000` per line.
fn parse_segment_list(csv: &str, dir: &Path) -> Result<Vec<AudioSegment>, StepError> {
    let mut out = Vec::new();
    for line in csv.lines().filter(|l| !l.trim().is_empty()) {
        let mut cols = line.rsplitn(3, ',');
        let (end, start, name) = (cols.next(), cols.next(), cols.next());
        let parsed = match (
            name,
            start.map(str::parse::<f64>),
            end.map(str::parse::<f64>),
        ) {
            (Some(name), Some(Ok(start_s)), Some(Ok(end_s))) => Some(AudioSegment {
                path: dir.join(name.trim_matches('"')),
                start_s,
                end_s,
            }),
            _ => None,
        };
        out.push(parsed.ok_or_else(|| {
            err(
                ErrorClass::Retryable,
                "internal",
                format!("unreadable ffmpeg segment list line {line:?}"),
            )
        })?);
    }
    Ok(out)
}

/// Runs `cmd` to completion; stdout on success. Both streams are read through [`capture`].
async fn run(tool: &str, cmd: &mut Command) -> Result<Vec<u8>, StepError> {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| {
            let what = if e.kind() == std::io::ErrorKind::NotFound {
                format!("{tool} is not installed (not on PATH)")
            } else {
                format!("starting {tool}: {e}")
            };
            err(ErrorClass::Retryable, "ffmpeg_unavailable", what)
        })?;
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let (out, errs, status) = tokio::join!(capture(stdout), capture(stderr), child.wait());
    let status = status.map_err(|e| {
        err(
            ErrorClass::Retryable,
            "ffmpeg_unavailable",
            format!("waiting for {tool}: {e}"),
        )
    })?;
    let errs = errs.unwrap_or_default();
    if !status.success() {
        let (class, code) = failure_class(&String::from_utf8_lossy(&errs));
        return Err(err(
            class,
            code,
            format!("{tool} exited with {status}: {}", tail(&errs)),
        ));
    }
    out.map_err(|e| {
        err(
            ErrorClass::Retryable,
            "ffmpeg_unavailable",
            format!("reading {tool}'s output: {e}"),
        )
    })
}

/// A failed run's class and code from its stderr: a local I/O fault is retried, bad media is not.
fn failure_class(stderr: &str) -> (ErrorClass, &'static str) {
    if IO_FAULT_MARKERS.iter().any(|m| stderr.contains(m)) {
        (ErrorClass::Retryable, "ffmpeg_io_error")
    } else {
        (ErrorClass::InvalidRequest, "audio_decode_failed")
    }
}

/// Reads `r` to its end, keeping its first and last [`CAPTURE_EDGE`] bytes (joined by `…`).
async fn capture(mut r: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut head = Vec::new();
    let mut tail: std::collections::VecDeque<u8> = std::collections::VecDeque::new();
    let mut cut = false;
    let mut buf = [0u8; 8192];
    loop {
        let n = r.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let mut chunk = &buf[..n];
        if head.len() < CAPTURE_EDGE {
            let take = (CAPTURE_EDGE - head.len()).min(chunk.len());
            head.extend_from_slice(&chunk[..take]);
            chunk = &chunk[take..];
        }
        tail.extend(chunk);
        while tail.len() > CAPTURE_EDGE {
            tail.pop_front();
            cut = true;
        }
    }
    if cut {
        head.extend_from_slice("\n…\n".as_bytes());
    }
    head.extend(tail);
    Ok(head)
}

/// Removes `strata-ingest-*` scratch dirs in `root` last modified more than `older_than` ago:
/// left behind by a worker that was killed mid-Convert (a clean exit removes its own). Runs once
/// at worker start; a live Convert's dir is far younger (its step deadline is 15 min). Returns
/// how many were removed; errors on single entries are logged and skipped.
pub fn sweep_scratch_dirs(root: &Path, older_than: Duration) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let now = SystemTime::now();
    let mut removed = 0;
    for entry in entries.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(SCRATCH_PREFIX)
        {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        // A symlink is not ours to follow, whatever its name.
        if !meta.is_dir() || meta.file_type().is_symlink() {
            continue;
        }
        let old = meta
            .modified()
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .is_some_and(|age| age > older_than);
        if !old {
            continue;
        }
        match std::fs::remove_dir_all(entry.path()) {
            Ok(()) => removed += 1,
            Err(e) => tracing::warn!(path = %entry.path().display(), error = %e,
                "removing a stale scratch dir failed"),
        }
    }
    removed
}

fn tail(stderr: &[u8]) -> String {
    let s = String::from_utf8_lossy(stderr);
    let s = s.trim();
    let n = s.chars().count();
    if n <= MAX_STDERR_CHARS {
        return s.to_string();
    }
    let cut: String = s.chars().skip(n - MAX_STDERR_CHARS).collect();
    format!("…{cut}")
}

fn err(class: ErrorClass, code: &str, message: impl Into<String>) -> StepError {
    StepError {
        class,
        code: code.to_string(),
        message: message.into(),
        provider: Some("ffmpeg".into()),
        retry_after_s: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_list_parses_and_rejects_garbage() {
        let dir = Path::new("/tmp/x");
        let segs = parse_segment_list(
            "out_0000.mp3,0.000000,60.000000\nout_0001.mp3,60.000000,90.5\n",
            dir,
        )
        .unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[1].path, dir.join("out_0001.mp3"));
        assert_eq!((segs[1].start_s, segs[1].end_s), (60.0, 90.5));
        assert!(parse_segment_list("out_0000.mp3,zero,1\n", dir).is_err());
    }

    #[test]
    fn io_faults_retry_and_bad_media_does_not() {
        let enospc = "[segment @ 0x1] Failed to open segment 'out_0003.mp3'\n\
                      Error writing trailer: No space left on device";
        assert_eq!(
            failure_class(enospc),
            (ErrorClass::Retryable, "ffmpeg_io_error")
        );
        assert_eq!(
            failure_class("av_interleaved_write_frame(): Input/output error").0,
            ErrorClass::Retryable
        );
        assert_eq!(
            failure_class("original: Invalid data found when processing input"),
            (ErrorClass::InvalidRequest, "audio_decode_failed")
        );
    }

    #[tokio::test]
    async fn capture_keeps_head_and_tail_of_endless_output() {
        let mut big = b"HEAD".to_vec();
        big.extend(std::iter::repeat_n(b'x', 5 * 1024 * 1024));
        big.extend_from_slice(b"TAIL");
        let kept = capture(&big[..]).await.unwrap();
        assert!(kept.len() <= 2 * CAPTURE_EDGE + 8, "{}", kept.len());
        assert!(kept.starts_with(b"HEAD") && kept.ends_with(b"TAIL"));
        assert_eq!(capture(&b"short"[..]).await.unwrap(), b"short");
    }

    #[test]
    fn sweep_removes_only_old_scratch_dirs() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join(format!("{SCRATCH_PREFIX}old"));
        let young = root.path().join(format!("{SCRATCH_PREFIX}young"));
        let other = root.path().join("someone-else");
        for d in [&old, &young, &other] {
            std::fs::create_dir(d).unwrap();
            std::fs::write(d.join("f"), b"x").unwrap();
        }
        let file = root.path().join(format!("{SCRATCH_PREFIX}file"));
        std::fs::write(&file, b"x").unwrap();
        let two_hours_ago = SystemTime::now() - Duration::from_secs(7200);
        for p in [&old, &other, &file] {
            std::fs::File::options()
                .read(true)
                .open(p)
                .and_then(|f| f.set_modified(two_hours_ago))
                .unwrap();
        }
        assert_eq!(
            sweep_scratch_dirs(root.path(), Duration::from_secs(3600)),
            1
        );
        assert!(!old.exists());
        assert!(young.exists() && other.exists() && file.exists());
        assert_eq!(
            sweep_scratch_dirs(&root.path().join("missing"), Duration::ZERO),
            0
        );
    }

    #[test]
    fn stderr_tail_keeps_the_end() {
        let long = format!("{}THE REASON", "x".repeat(5_000));
        let t = tail(long.as_bytes());
        assert!(t.ends_with("THE REASON"));
        assert!(t.chars().count() <= MAX_STDERR_CHARS + 1);
    }
}
