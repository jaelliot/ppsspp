// Copyright (c) 2012- PPSSPP Project.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, version 2.0 or later versions.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License 2.0 for more details.

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tungstenite::handshake::server::{Request, Response};
use tungstenite::{Message, WebSocket};

struct ClientProcess(Option<Child>);

impl Drop for ClientProcess {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            child.kill().ok();
            child.wait().ok();
        }
    }
}

// Exercise the real command-line client against a debugger peer, without booting a game.
fn run_client(
    args: &[&str],
    script: Option<&str>,
    replies: impl FnOnce(&Value) -> Vec<Value>,
) -> (Output, Duration) {
    run_exchange(args, script, |socket, request| {
        for reply in replies(request) {
            socket.send(Message::Text(reply.to_string())).unwrap();
        }
    })
}

fn run_exchange(
    args: &[&str],
    script: Option<&str>,
    exchange: impl FnOnce(&mut WebSocket<TcpStream>, &Value),
) -> (Output, Duration) {
    run_session(args, script, |socket| {
        let request = loop {
            let value = read_json(socket);
            match value["event"].as_str().unwrap() {
                "client.config.set" | "version" => {
                    socket.send(Message::Text(value.to_string())).unwrap();
                }
                _ => break value,
            }
        };
        exchange(socket, &request);
    })
}

fn run_session(
    args: &[&str],
    script: Option<&str>,
    exchange: impl FnOnce(&mut WebSocket<TcpStream>),
) -> (Output, Duration) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_wsdbg"))
        .arg(listener.local_addr().unwrap().port().to_string())
        .arg("--compact")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // A failed peer assertion must not leave the real client running.
    let mut process = ClientProcess(Some(child));
    let child = process.0.as_mut().unwrap();
    if let Some(script) = script {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(script.as_bytes())
            .unwrap();
    } else {
        drop(child.stdin.take());
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(e) => {
                child.kill().ok();
                let output = process.0.take().unwrap().wait_with_output().unwrap();
                panic!("client did not connect: {e}; {output:?}");
            }
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    // The callback's error type is fixed by tungstenite.
    #[allow(clippy::result_large_err)]
    let mut socket = tungstenite::accept_hdr(stream, |_: &Request, mut response: Response| {
        response.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "debugger.ppsspp.org".parse().unwrap(),
        );
        Ok(response)
    })
    .unwrap();

    let response_time = Instant::now();
    exchange(&mut socket);

    // Keep the peer connected: an error must finish the request itself, without relying on EOF.
    // The existing REPL shutdown drain is bounded at ten seconds.
    let deadline = Instant::now() + Duration::from_secs(12);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().ok();
            let output = process.0.take().unwrap().wait_with_output().unwrap();
            panic!("client did not exit: {output:?}");
        }
        thread::sleep(Duration::from_millis(5));
    }
    let elapsed = response_time.elapsed();
    (
        process.0.take().unwrap().wait_with_output().unwrap(),
        elapsed,
    )
}

fn read_json(socket: &mut WebSocket<TcpStream>) -> Value {
    loop {
        if let Message::Text(text) = socket.read().unwrap() {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

fn error_reply(request: &Value) -> Value {
    json!({"event": "error", "ticket": request["ticket"], "message": "CPU not started"})
}

fn assert_success(output: &Output, expected: bool) {
    assert_eq!(
        output.status.success(),
        expected,
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn one_shot_reports_server_errors() {
    let (output, _) = run_client(&["cpu.status"], None, |request| vec![error_reply(request)]);
    assert_success(&output, false);
}

#[test]
fn one_shot_ignores_unrelated_errors() {
    let (output, _) = run_client(&["cpu.status"], None, |request| {
        vec![
            json!({"event": "error", "ticket": 9999, "message": "another request failed"}),
            json!({"event": "cpu.status", "ticket": request["ticket"], "stepping": true}),
        ]
    });
    assert_success(&output, true);
}

#[test]
fn wait_all_reports_server_errors() {
    let (output, _) = run_client(
        &["--wait-all", "--wait", "0.2", "cpu.status"],
        None,
        |request| vec![error_reply(request)],
    );
    assert_success(&output, false);
}

#[test]
fn wait_all_ignores_unrelated_errors() {
    let (output, _) = run_client(
        &["--wait-all", "--wait", "0.2", "cpu.status"],
        None,
        |request| {
            vec![
                json!({"event": "error", "ticket": 9999, "message": "another request failed"}),
                json!({"event": "cpu.status", "ticket": request["ticket"], "stepping": true}),
            ]
        },
    );
    assert_success(&output, true);
}

#[test]
fn wait_all_keeps_failure_after_later_reply() {
    let (output, _) = run_client(
        &["--wait-all", "--wait", "0.2", "cpu.status"],
        None,
        |request| {
            vec![
                error_reply(request),
                json!({"event": "cpu.status", "ticket": request["ticket"], "stepping": true}),
            ]
        },
    );
    assert_success(&output, false);
}

#[test]
fn raw_without_ticket_remains_observational() {
    let (output, _) = run_client(
        &["--wait", "0.2", "--raw", r#"{"event":"cpu.status"}"#],
        None,
        |request| {
            assert!(request.get("ticket").is_none());
            vec![json!({"event": "error", "message": "CPU not started"})]
        },
    );
    assert_success(&output, true);
}

#[test]
fn sync_reports_server_errors() {
    let (output, _) = run_client(&["--sync"], Some("cpu.status\n:quit\n"), |request| {
        vec![error_reply(request)]
    });
    assert_success(&output, false);
}

#[test]
fn sync_resume_error_does_not_wait_for_stepping() {
    let (output, elapsed) = run_client(
        &["--sync", "--sync-timeout", "3"],
        Some("cpu.resume\n:quit\n"),
        |request| vec![error_reply(request)],
    );
    assert_success(&output, false);
    assert!(elapsed < Duration::from_secs(2), "error took {elapsed:?}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("timed out"));
}

#[test]
fn sync_ignores_unrelated_errors() {
    let (output, _) = run_client(&["--sync"], Some("cpu.status\n:quit\n"), |request| {
        vec![
            json!({"event": "error", "ticket": 9999, "message": "another request failed"}),
            json!({"event": "cpu.status", "ticket": request["ticket"], "stepping": true}),
        ]
    });
    assert_success(&output, true);
}

#[test]
fn sync_resume_accepts_deferred_ack_and_stepping() {
    let (output, _) = run_client(&["--sync"], Some("cpu.resume\n:quit\n"), |request| {
        vec![
            json!({"event": "deferred", "ticket": request["ticket"], "for": "cpu.resume"}),
            json!({"event": "cpu.stepping", "pc": 0x08800000}),
        ]
    });
    assert_success(&output, true);
}

fn deferred_reply(request: &Value) -> Value {
    json!({"event": "deferred", "ticket": request["ticket"], "for": request["event"]})
}

fn saved_reply(request: &Value, status: &str) -> Value {
    json!({"event": request["event"], "ticket": request["ticket"], "status": status})
}

fn state_error_reply(request: &Value, status: &str) -> Value {
    json!({
        "event": "error", "ticket": request["ticket"], "for": request["event"],
        "status": status, "level": 2, "message": "No completion result",
    })
}

#[test]
fn savestate_one_shot_waits_for_terminal_success() {
    let (output, _) = run_client(
        &["game.state.save", "path=checkpoint.ppst", "wait=true"],
        None,
        |request| vec![deferred_reply(request), saved_reply(request, "success")],
    );
    assert_success(&output, true);
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"status\":\"success\""));
}

#[test]
fn savestate_sync_waits_for_terminal_success() {
    let (output, _) = run_exchange(
        &["--sync"],
        Some("game.state.save path=checkpoint.ppst wait=true\ncpu.status\n:quit\n"),
        |socket, request| {
            socket
                .send(Message::Text(deferred_reply(request).to_string()))
                .unwrap();
            socket
                .get_mut()
                .set_read_timeout(Some(Duration::from_millis(300)))
                .unwrap();
            assert!(
                matches!(socket.read(), Err(tungstenite::Error::Io(ref e))
                    if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
                "--sync sent the next command before save-state completion",
            );
            socket
                .get_mut()
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            socket
                .send(Message::Text(saved_reply(request, "success").to_string()))
                .unwrap();
            let status = read_json(socket);
            assert_eq!(status["event"], "cpu.status");
            socket.send(Message::Text(status.to_string())).unwrap();
        },
    );
    assert_success(&output, true);
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"status\":\"success\""));
}

#[test]
fn savestate_terminal_errors_fail() {
    for status in ["failed", "abandoned"] {
        for sync in [false, true] {
            let args: &[&str] = if sync {
                &["--sync"]
            } else {
                &["game.state.load", "path=checkpoint.ppst", "wait=true"]
            };
            let (output, _) = run_client(
                args,
                sync.then_some("game.state.load path=checkpoint.ppst wait=true\n:quit\n"),
                |request| vec![deferred_reply(request), state_error_reply(request, status)],
            );
            assert_success(&output, false);
            assert!(String::from_utf8_lossy(&output.stdout).contains(status));
        }
    }
}

#[test]
fn savestate_direct_warning_and_immediate_error_are_final() {
    let (output, _) = run_client(
        &["game.state.load", "path=checkpoint.ppst", "wait=true"],
        None,
        |request| vec![saved_reply(request, "warning")],
    );
    assert_success(&output, true);
    let (output, elapsed) = run_client(
        &["--sync", "--sync-timeout", "3"],
        Some("game.state.load path=checkpoint.ppst wait=true\n:quit\n"),
        |request| vec![error_reply(request)],
    );
    assert_success(&output, false);
    assert!(elapsed < Duration::from_secs(2));
}

#[test]
fn savestate_missing_or_unknown_terminal_status_fails() {
    for status in [None, Some("queued")] {
        let (output, _) = run_client(
            &["game.state.save", "path=checkpoint.ppst", "wait=true"],
            None,
            |request| {
                let mut reply = json!({"event": request["event"], "ticket": request["ticket"]});
                if let Some(status) = status {
                    reply["status"] = json!(status);
                }
                vec![reply]
            },
        );
        assert_success(&output, false);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("completion"), "{stderr}");
        assert!(stderr.contains("unsupported"), "{stderr}");
    }
}

#[test]
fn savestate_wait_all_requires_terminal_and_keeps_failure() {
    for terminal_error in [false, true] {
        let (output, _) = run_client(
            &[
                "--wait-all",
                "--wait",
                "0.2",
                "game.state.save",
                "path=checkpoint.ppst",
                "wait=true",
            ],
            None,
            |request| {
                let mut replies = vec![deferred_reply(request)];
                if terminal_error {
                    replies.push(state_error_reply(request, "failed"));
                    replies.push(saved_reply(request, "success"));
                }
                replies
            },
        );
        assert_success(&output, false);
    }
}

#[test]
fn savestate_sync_timeout_does_not_start_another_drain() {
    let (output, elapsed) = run_client(
        &["--sync", "--sync-timeout", "0.2"],
        Some("game.state.save path=checkpoint.ppst wait=true\n:quit\n"),
        |request| vec![deferred_reply(request)],
    );
    assert_success(&output, false);
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}

#[test]
fn savestate_default_and_feed_acknowledgements_stay_compatible() {
    for args in [
        vec!["game.state.save", "path=checkpoint.ppst"],
        vec!["game.state.save", "path=checkpoint.ppst", "wait=false"],
        vec!["cpu.resume"],
        vec!["gpu.stats.feed"],
    ] {
        let (output, _) = run_client(&args, None, |request| {
            if request["event"] == "game.state.save" {
                vec![json!({"event": request["event"], "ticket": request["ticket"]})]
            } else {
                vec![deferred_reply(request)]
            }
        });
        assert_success(&output, true);
    }
}

#[test]
fn savestate_uses_actual_outgoing_event_and_ticket() {
    for sync in [false, true] {
        let args: &[&str] = if sync {
            &["--sync", "--sync-timeout", "0.2"]
        } else {
            &[
                "--wait",
                "0.2",
                "cpu.status",
                "event=game.state.save",
                "path=checkpoint.ppst",
                "ticket=73",
                "wait=true",
            ]
        };
        let (output, _) = run_client(
            args,
            sync.then_some("cpu.status event=game.state.save path=checkpoint.ppst ticket=73 wait=true\n:quit\n"),
            |request| {
                assert_eq!(request["event"], "game.state.save");
                assert_eq!(request["ticket"], 73);
                vec![deferred_reply(request), saved_reply(request, "success")]
            },
        );
        assert_success(&output, true);
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"status\":\"success\""));
    }
}

#[test]
fn savestate_raw_ticket_is_tracked_and_unticketed_stays_observational() {
    let (output, _) = run_client(
        &[
            "--raw",
            r#"{"event":"game.state.save","path":"checkpoint.ppst","wait":true,"ticket":73}"#,
        ],
        None,
        |request| {
            vec![
                deferred_reply(request),
                state_error_reply(request, "failed"),
            ]
        },
    );
    assert_success(&output, false);
    let (output, _) = run_client(
        &[
            "--wait",
            "0.2",
            "--raw",
            r#"{"event":"game.state.save","path":"checkpoint.ppst","wait":true}"#,
        ],
        None,
        |request| {
            assert!(request.get("ticket").is_none());
            vec![json!({"event": "error", "status": "failed"})]
        },
    );
    assert_success(&output, true);
}

#[test]
fn savestate_ignores_unrelated_and_wrong_event_messages() {
    let (output, _) = run_client(
        &["game.state.save", "path=checkpoint.ppst", "wait=true"],
        None,
        |request| {
            vec![
                json!({"event": "error", "ticket": 9999, "status": "failed"}),
                json!({"event": "cpu.status", "ticket": request["ticket"]}),
                deferred_reply(request),
                deferred_reply(request),
                state_error_reply(request, "failed"),
            ]
        },
    );
    assert_success(&output, false);
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"status\":\"failed\""));
}

#[test]
fn savestate_nowait_result_is_observed_during_cpu_execution() {
    for fails in [false, true] {
        let (output, _) = run_exchange(
            &["--sync", "--sync-timeout", "0.5"],
            Some(
                ":nowait game.state.save path=checkpoint.ppst wait=true\ncpu.runUntilTime relativeUs=1000\n:quit\n",
            ),
            |socket, request| {
                socket
                    .send(Message::Text(deferred_reply(request).to_string()))
                    .unwrap();
                let advance = read_json(socket);
                assert_eq!(advance["event"], "cpu.runUntilTime");
                assert_ne!(advance["ticket"], request["ticket"]);
                for reply in [
                    json!({"event": "error", "ticket": 9999, "status": "failed"}),
                    deferred_reply(request),
                    json!({"event": "cpu.runUntilTime", "ticket": advance["ticket"]}),
                    if fails {
                        state_error_reply(request, "abandoned")
                    } else {
                        saved_reply(request, "success")
                    },
                    saved_reply(request, "success"),
                    json!({"event": "cpu.stepping", "pc": 0x08800000}),
                ] {
                    socket.send(Message::Text(reply.to_string())).unwrap();
                }
            },
        );
        assert_success(&output, !fails);
        assert!(String::from_utf8_lossy(&output.stdout).contains("cpu.stepping"));
    }
}

#[test]
fn savestate_nowait_eof_and_quit_wait_for_terminal() {
    for ending in ["", ":quit\n"] {
        let script = format!(":nowait game.state.save path=checkpoint.ppst wait=true\n{ending}");
        let (output, _) = run_exchange(&["--sync"], Some(&script), |socket, request| {
            socket
                .send(Message::Text(deferred_reply(request).to_string()))
                .unwrap();
            // Keep acceptance and completion separate; an ACK-only client will exit in between.
            thread::sleep(Duration::from_millis(200));
            socket
                .send(Message::Text(saved_reply(request, "success").to_string()))
                .ok();
        });
        assert_success(&output, true);
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"status\":\"success\""));
    }
}

#[test]
fn savestate_nowait_disconnect_is_unknown_not_success() {
    for ending in ["", ":quit\n"] {
        let script = format!(":nowait game.state.save path=checkpoint.ppst wait=true\n{ending}");
        let (output, _) = run_exchange(&["--sync"], Some(&script), |socket, request| {
            socket
                .send(Message::Text(deferred_reply(request).to_string()))
                .unwrap();
            socket.close(None).unwrap();
        });
        assert_success(&output, false);
        assert!(String::from_utf8_lossy(&output.stderr).contains("unknown"));
    }
}

#[test]
fn savestate_nowait_drain_timeout_is_unknown_not_success() {
    let (output, elapsed) = run_client(
        &["--sync"],
        Some(":nowait game.state.save path=checkpoint.ppst wait=true\n"),
        |request| vec![deferred_reply(request)],
    );
    assert_success(&output, false);
    assert!(elapsed < Duration::from_secs(12));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown"));
}

#[test]
fn savestate_pending_ticket_cannot_be_reused() {
    let (output, _) = run_exchange(
        &["--sync", "--sync-timeout", "0.5"],
        Some(concat!(
            ":nowait {\"event\":\"game.state.save\",\"path\":\"checkpoint.ppst\",\"wait\":true,\"ticket\":73}\n",
            "{\"event\":\"cpu.status\",\"ticket\":73}\n",
            "cpu.status\n:quit\n",
        )),
        |socket, request| {
            socket
                .send(Message::Text(deferred_reply(request).to_string()))
                .unwrap();
            let status = read_json(socket);
            assert_eq!(status["event"], "cpu.status");
            assert_ne!(status["ticket"], request["ticket"]);
            for reply in [saved_reply(request, "success"), status] {
                socket.send(Message::Text(reply.to_string())).unwrap();
            }
        },
    );
    assert_success(&output, false);
    assert!(String::from_utf8_lossy(&output.stderr).contains("already pending"));
}

#[test]
fn savestate_raw_ticket_is_not_reused_by_snapshot() {
    let (output, _) = run_exchange(
        &["--sync", "--sync-timeout", "0.5"],
        Some(concat!(
            ":nowait {\"event\":\"game.state.save\",\"path\":\"checkpoint.ppst\",\"wait\":true,\"ticket\":3}\n",
            ":snapshot checkpoint 0x08800000 1\n:quit\n",
        )),
        |socket, request| {
            socket
                .send(Message::Text(deferred_reply(request).to_string()))
                .unwrap();
            let read = read_json(socket);
            assert_eq!(read["event"], "memory.read");
            // Config/version used 1/2; the raw request did not advance the automatic allocator.
            assert_ne!(read["ticket"], request["ticket"]);
            for reply in [
                state_error_reply(request, "failed"),
                json!({"event": "memory.read", "ticket": read["ticket"], "base64": "AA=="}),
            ] {
                socket.send(Message::Text(reply.to_string())).unwrap();
            }
        },
    );
    assert_success(&output, false);
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("snapshot 'checkpoint' saved: 1 bytes")
    );
}

#[test]
fn savestate_shorthand_cannot_disable_ticket_correlation() {
    for ticket in ["ticket=null", "ticket=uncorrelated"] {
        let (output, _) = run_session(
            &[
                "--wait",
                "0.2",
                "game.state.save",
                "path=checkpoint.ppst",
                "wait=true",
                ticket,
            ],
            None,
            |socket| loop {
                match socket.read() {
                    Ok(Message::Text(text)) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        assert_eq!(
                            value["event"], "client.config.set",
                            "invalid shorthand request was sent"
                        );
                        socket.send(Message::Text(value.to_string())).unwrap();
                    }
                    Ok(Message::Close(_)) | Err(tungstenite::Error::ConnectionClosed) => break,
                    Err(tungstenite::Error::Protocol(
                        tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                    )) => break,
                    result => panic!("unexpected message before client exit: {result:?}"),
                }
            },
        );
        assert_success(&output, false);
        assert!(String::from_utf8_lossy(&output.stderr).contains("non-negative integer"));
    }
}
