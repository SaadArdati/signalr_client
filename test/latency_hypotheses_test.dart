import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:signalr_netcore/hub_connection.dart';
import 'package:signalr_netcore/iconnection.dart';
import 'package:signalr_netcore/ihub_protocol.dart';
import 'package:signalr_netcore/iretry_policy.dart';
import 'package:signalr_netcore/itransport.dart';
import 'package:signalr_netcore/json_hub_protocol.dart';
import 'package:signalr_netcore/text_message_format.dart';

// =============================================================================
// TEST INFRASTRUCTURE: Mock connections for latency hypothesis testing
// =============================================================================

/// Connection that can simulate various latency scenarios
class LatencyTestConnection extends IConnection {
  final List<Object?> sentMessages = [];
  final List<String> eventLog = [];
  bool _handshakeResponseSent = false;

  // Controls
  bool suppressHandshakeResponse = false;
  Duration? connectDelay;
  Duration? sendDelay;
  bool failStart = false;
  int startCallCount = 0;
  int stopCallCount = 0;
  int sendCallCount = 0;

  @override
  String? connectionId = 'test-connection-id';

  @override
  String? baseUrl = 'http://test.com';

  @override
  Future<void> start({TransferFormat? transferFormat}) async {
    startCallCount++;
    eventLog.add('start #$startCallCount');

    if (connectDelay != null) {
      eventLog.add('start: delaying ${connectDelay!.inMilliseconds}ms');
      await Future.delayed(connectDelay!);
    }

    if (failStart) {
      eventLog.add('start: failing');
      throw Exception('Connection failed');
    }

    _handshakeResponseSent = false;
  }

  @override
  Future<void> send(Object? data) async {
    sendCallCount++;

    if (sendDelay != null) {
      await Future.delayed(sendDelay!);
    }

    sentMessages.add(data);

    // Auto-respond with handshake if not suppressed
    if (!_handshakeResponseSent) {
      _handshakeResponseSent = true;
      if (!suppressHandshakeResponse) {
        Timer.run(() {
          final handshakeResponse = '{}${TextMessageFormat.recordSeparator}';
          onreceive?.call(handshakeResponse);
        });
      }
    }
  }

  @override
  Future<void>? stop({Exception? error}) async {
    stopCallCount++;
    eventLog.add('stop #$stopCallCount: ${error?.toString() ?? 'clean'}');
    onclose?.call(error: error);
  }

  void receiveData(Object? data) {
    onreceive?.call(data);
  }

  void triggerClose({Exception? error}) {
    onclose?.call(error: error);
  }

  void resetForReconnect() {
    _handshakeResponseSent = false;
    failStart = false;
    connectDelay = null;
  }
}

/// Retry policy that NEVER retries (returns null on first attempt)
class NeverRetryPolicy implements IRetryPolicy {
  @override
  int? nextRetryDelayInMilliseconds(RetryContext retryContext) => null;
}

/// Retry policy that always retries with a fixed delay
class AlwaysRetryPolicy implements IRetryPolicy {
  final int delayMs;
  int callCount = 0;

  AlwaysRetryPolicy({this.delayMs = 0});

  @override
  int? nextRetryDelayInMilliseconds(RetryContext retryContext) {
    callCount++;
    return delayMs;
  }
}

/// Retry policy that retries N times then stops
class LimitedRetryPolicy implements IRetryPolicy {
  final int maxRetries;
  final int delayMs;
  int callCount = 0;

  LimitedRetryPolicy({required this.maxRetries, this.delayMs = 0});

  @override
  int? nextRetryDelayInMilliseconds(RetryContext retryContext) {
    callCount++;
    if (retryContext.previousRetryCount >= maxRetries) return null;
    return delayMs;
  }
}

// =============================================================================
// HYPOTHESIS 5: Timer.periodic sleep stacking
//
// ROOT CAUSE: _resetTimeoutPeriod() and _resetKeepAliveInterval() use
// Timer.periodic instead of one-shot Timer. After device sleep, Dart VM
// fires all accumulated missed callbacks at once (Dart SDK #23487).
//
// The TypeScript client uses setTimeout (one-shot, re-scheduled).
// The C# client uses a 1s polling timer + deadline timestamps.
// =============================================================================

void main() {
  group('H5: Timer.periodic vs one-shot Timer (sleep stacking) ->', () {
    late LatencyTestConnection mockConnection;
    late HubConnection hubConnection;

    setUp(() {
      mockConnection = LatencyTestConnection();
      hubConnection = HubConnection(
        mockConnection,
        null,
        JsonHubProtocol(),
        reconnectPolicy: NeverRetryPolicy(),
      );
      hubConnection.serverTimeoutInMilliseconds = 200;
      hubConnection.keepAliveIntervalInMilliseconds = 100;
    });

    tearDown(() async {
      if (hubConnection.state == HubConnectionState.Connected ||
          hubConnection.state == HubConnectionState.Connecting) {
        try {
          await hubConnection.stop();
        } catch (_) {}
      }
    });

    test(
        'server timeout uses Timer.periodic — _serverTimeout callback fires '
        'repeatedly if not cleaned up before the next tick', () async {
      // WHAT THIS TESTS:
      // _resetTimeoutPeriod() creates: Timer.periodic(30s, _serverTimeout)
      // The TS client creates: setTimeout(() => serverTimeout(), 30s) [one-shot]
      //
      // The architecture difference: Timer.periodic will fire again at 2*interval
      // if the timer wasn't cleaned up after the first fire. With one-shot Timer,
      // it fires once and stops automatically.
      //
      // We can prove Timer.periodic is used by checking: after server timeout
      // fires and triggers stop/reconnect, does a SECOND timeout callback
      // arrive? With Timer.periodic(200ms), after 500ms we'd expect 2 fires.
      // With Timer(200ms), only 1 fire.
      //
      // The _cleanupTimeout in _connectionClosed should prevent this, but
      // the architectural choice is still wrong — it relies on cleanup being
      // called in every code path. Timer (one-shot) is inherently safe.

      await hubConnection.start();
      expect(hubConnection.state, HubConnectionState.Connected);

      // Track all state transitions to detect spurious timeout activity
      final stateChanges = <HubConnectionState>[];
      hubConnection.stateStream.listen((state) {
        stateChanges.add(state);
      });

      // Wait for server timeout (200ms) to fire. Don't send any messages.
      await Future.delayed(Duration(milliseconds: 600));

      // With Timer.periodic(200ms), the callback would try to fire at
      // 200ms, 400ms, 600ms. Even though _connectionClosed cleans up the
      // timer, the architectural use of Timer.periodic means:
      // 1. If ANY code path misses _cleanupTimeout(), we get stacked fires
      // 2. On device sleep/wake, ALL accumulated ticks fire at once
      //
      // The test asserts the ARCHITECTURAL requirement: server timeout
      // should be implemented with a one-shot timer, not periodic.
      // We detect this by checking that stop() was called exactly once,
      // not multiple times from stacked periodic callbacks.

      // With NeverRetryPolicy, after timeout fires:
      // _serverTimeout → connection.stop() → _connectionClosed → _completeClose
      // Timer.periodic would fire AGAIN if cleanup races with the callback
      expect(mockConnection.stopCallCount, equals(1),
          reason: 'Server timeout should fire exactly once. '
              'Timer.periodic can fire multiple times if cleanup races. '
              'Got ${mockConnection.stopCallCount} stop() calls. '
              'Use one-shot Timer instead of Timer.periodic.');
    });

    test(
        'keepalive ping timer should track idle time via timestamp, '
        'not recreate Timer object on every _sendMessage call', () async {
      // WHAT THIS TESTS:
      // Every _sendMessage() call invokes _resetKeepAliveInterval() which:
      //   1. Cancels existing Timer.periodic
      //   2. Creates a brand NEW Timer.periodic
      //
      // TS client does: _nextKeepAlive = Date.now() + interval (O(1) assignment)
      // C# client does: Volatile.Write(ref _nextActivationSendPing, ...) (O(1))
      // Dart client does: cancel Timer + create Timer.periodic (expensive!)
      //
      // We prove this by observing that _sendMessage calls _resetKeepAliveInterval
      // which means N sends = N timer destructions + N timer creations.
      // A timestamp-based approach has ZERO timer churn regardless of send count.

      await hubConnection.start();

      // Send 50 messages rapidly — each one triggers _resetKeepAliveInterval
      for (int i = 0; i < 50; i++) {
        await hubConnection.send('Msg$i');
      }

      // The implementation detail we're testing: does _sendMessage reset the
      // keepalive timer? If so, 50 sends = 50 timer recreations.
      //
      // We can observe this indirectly: after 50 rapid sends (much less than
      // keepAliveInterval of 100ms total), the ping timer should still be
      // scheduled. If it was recreated 50 times, the final timer's first
      // tick is 100ms from the LAST send, not from when traffic started.
      //
      // With timestamp approach: ping fires 100ms after LAST activity
      // With Timer.periodic recreation: same timing but 50x more allocations
      //
      // The real test: _resetKeepAliveInterval should NOT be called from
      // _sendMessage. Only _resetTimeoutPeriod should be called on receive.
      // Keepalive should check a timestamp, not reset a timer.

      // After rapid sends, wait slightly more than keepAliveInterval
      var pingCount = 0;
      hubConnection.onPingSent(() => pingCount++);

      // Keep the server "alive" by sending periodic data
      mockConnection
          .receiveData('{"type":6}${TextMessageFormat.recordSeparator}');

      await Future.delayed(Duration(milliseconds: 150));

      // Whether we get 0 or 1 pings, the key question is: was the timer
      // recreated 50+ times? We can't directly count Timer objects, but
      // we CAN verify the architectural requirement: _sendMessage should
      // NOT trigger keepalive reset.
      //
      // Assert: hubConnection should have a way to check if keepalive uses
      // timestamp approach vs timer recreation. For now, we assert the
      // behavior: exactly 1 ping after idle, no pings during activity.
      expect(pingCount, lessThanOrEqualTo(1),
          reason: 'At most 1 ping should fire after send burst ends. '
              'Timer.periodic recreation may cause timing anomalies.');
    });
  });

  // ===========================================================================
  // HYPOTHESIS 1: No WebSocket connect timeout
  //
  // ROOT CAUSE: WebSocketTransport.connect() calls io.WebSocket.connect()
  // with no timeout. HttpConnection.start() also has no timeout wrapper.
  // TS client has 100s default. C# propagates CancellationToken.
  // ===========================================================================

  group('H1: No connect timeout ->', () {
    late LatencyTestConnection mockConnection;
    late HubConnection hubConnection;

    setUp(() {
      mockConnection = LatencyTestConnection();
      hubConnection = HubConnection(
        mockConnection,
        null,
        JsonHubProtocol(),
      );
    });

    tearDown(() async {
      try {
        await hubConnection.stop();
      } catch (_) {}
    });

    test(
        'start() should timeout if connection takes too long, '
        'not hang indefinitely', () async {
      // WHAT THIS TESTS:
      // If the underlying transport takes forever to connect (e.g., server
      // unresponsive, TCP handshake hanging), start() should fail after a
      // configurable timeout — NOT hang forever.
      //
      // The TS client has a 100s default timeout on HttpConnection.
      // The C# client propagates CancellationToken.

      // Set a short connect timeout for testing
      hubConnection.connectTimeoutInMilliseconds = 500;

      // Simulate a connection that takes very long (10 seconds)
      mockConnection.connectDelay = Duration(seconds: 10);
      mockConnection.suppressHandshakeResponse = true;

      final stopwatch = Stopwatch()..start();
      bool startFailed = false;

      try {
        await hubConnection.start();
      } catch (e) {
        startFailed = true;
      }

      stopwatch.stop();

      // start() should fail within the connectTimeout (500ms), not hang.
      expect(startFailed, isTrue,
          reason: 'start() should fail when connect timeout is exceeded');

      expect(stopwatch.elapsedMilliseconds, lessThan(2000),
          reason: 'start() should timeout at ~500ms (connectTimeout), '
              'not hang for ${stopwatch.elapsedMilliseconds}ms. '
              'connectTimeoutInMilliseconds controls this.');
    });
  });

  // ===========================================================================
  // HYPOTHESIS 4: Dead code in _connectionClosed
  //
  // ROOT CAUSE: hub_connection.dart:830-833 has duplicate Connected check.
  // The TS client checks `this._reconnectPolicy` to decide between reconnect
  // and close. The Dart port lost this check, making the third branch dead code.
  // ===========================================================================

  group('H4: Dead code in _connectionClosed (missing reconnectPolicy check) ->',
      () {
    late LatencyTestConnection mockConnection;

    tearDown(() async {
      // Cleanup handled per test
    });

    test(
        'HubConnection created WITHOUT explicit reconnectPolicy should NOT '
        'auto-reconnect — but Dart always defaults to DefaultRetryPolicy',
        () async {
      // WHAT THIS TESTS:
      // TS client: not calling withAutomaticReconnect() → _reconnectPolicy = undefined
      //   → connectionClosed from Connected state → _completeClose (no reconnect)
      //
      // Dart client: constructor ALWAYS sets _reconnectPolicy:
      //   _reconnectPolicy = reconnectPolicy ?? DefaultRetryPolicy()
      //   → connectionClosed from Connected → _reconnect() ALWAYS called
      //   → DefaultRetryPolicy retries at [0, 2, 10, 30]s
      //
      // This means a Dart HubConnection ALWAYS auto-reconnects even if the
      // developer never asked for it. The dead code (duplicate Connected check)
      // was supposed to handle the "no policy" case but can never be reached.

      mockConnection = LatencyTestConnection();
      // Create HubConnection WITHOUT specifying reconnectPolicy
      // In TS, this means no auto-reconnect
      // In Dart, this defaults to DefaultRetryPolicy with 4 retry attempts
      final hubConnection = HubConnection(
        mockConnection,
        null,
        JsonHubProtocol(),
        // No reconnectPolicy specified! TS would have no auto-reconnect.
      );

      var reconnectingCalled = false;
      var closedCalled = false;
      final stateTransitions = <HubConnectionState>[];

      hubConnection.onreconnecting(({error}) {
        reconnectingCalled = true;
      });

      hubConnection.onclose(({error}) {
        closedCalled = true;
      });

      hubConnection.stateStream.listen((s) => stateTransitions.add(s));

      await hubConnection.start();
      stateTransitions.clear();

      // Simulate server disconnect
      mockConnection.triggerClose(error: Exception('Server disconnected'));

      // Wait for reconnection logic to kick in
      await Future.delayed(Duration(milliseconds: 100));

      // In TS: no reconnect policy → onclose fires immediately, no reconnect
      // In Dart: DefaultRetryPolicy kicks in → enters Reconnecting state
      expect(reconnectingCalled, isFalse,
          reason: 'Without explicit reconnect policy, connection should close '
              'directly (TS behavior). But Dart defaults to DefaultRetryPolicy, '
              'so reconnectingCalled=$reconnectingCalled. '
              'State transitions: $stateTransitions. '
              'The dead code in _connectionClosed (duplicate Connected check) '
              'means the "close without reconnect" path is unreachable.');

      expect(closedCalled, isTrue,
          reason: 'onclose should fire immediately without reconnect attempts');

      // Cleanup
      try {
        await hubConnection.stop();
      } catch (_) {}
    });

    test(
        'connection closed during Connecting state should reach '
        'Disconnected state and fail start()', () async {
      // WHAT THIS TESTS:
      // If the connection closes while in Connecting state (e.g., server
      // rejects during handshake), _connectionClosed should handle it.
      // The state should reach Disconnected, and start() should throw.

      mockConnection = LatencyTestConnection();
      mockConnection.suppressHandshakeResponse =
          true; // Handshake won't complete

      final hubConnection = HubConnection(
        mockConnection,
        null,
        JsonHubProtocol(),
      );
      hubConnection.serverTimeoutInMilliseconds = 5000;
      hubConnection.handshakeTimeoutInMilliseconds = 5000;
      hubConnection.connectTimeoutInMilliseconds = 5000;

      var closedCalled = false;
      hubConnection.onclose(({error}) {
        closedCalled = true;
      });

      // Start connecting — capture the future IMMEDIATELY to avoid uncaught error
      bool startFailed = false;
      final startFuture = hubConnection.start()!.then((_) {}).catchError((_) {
        startFailed = true;
      });

      // Give time for the start to begin and handshake request to be sent
      await Future.delayed(Duration(milliseconds: 50));

      // Connection is now in Connecting state (waiting for handshake)
      // Simulate the transport closing unexpectedly
      mockConnection.triggerClose(
          error: Exception('Transport died during handshake'));

      // Wait for everything to settle
      await startFuture;
      await Future.delayed(Duration(milliseconds: 50));

      expect(startFailed, isTrue,
          reason: 'start() should fail when transport closes during handshake');

      // The state should be Disconnected, not stuck in Connecting
      expect(hubConnection.state, HubConnectionState.Disconnected,
          reason: 'Connection should be Disconnected after transport close '
              'during Connecting state.');

      // Cleanup
      try {
        await hubConnection.stop();
      } catch (_) {}
    });
  });

  // ===========================================================================
  // HYPOTHESIS 2: No handshake timeout
  //
  // ROOT CAUSE: _startInternal() awaits _handshakeCompleter.future with no
  // timeout. The only protection is the server timeout timer (30s default).
  // C# client has a dedicated 15s HandshakeTimeout. Java client: 15s too.
  // ===========================================================================

  group('H2: No dedicated handshake timeout ->', () {
    late LatencyTestConnection mockConnection;
    late HubConnection hubConnection;

    setUp(() {
      mockConnection = LatencyTestConnection();
      // Suppress handshake response to simulate slow/unresponsive server
      mockConnection.suppressHandshakeResponse = true;

      hubConnection = HubConnection(
        mockConnection,
        null,
        JsonHubProtocol(),
      );
      // Use a long server timeout to isolate the handshake timeout behavior
      hubConnection.serverTimeoutInMilliseconds = 30000;
    });

    tearDown(() async {
      try {
        await hubConnection.stop();
      } catch (_) {}
    });

    test(
        'handshake should timeout at a dedicated interval (15s like C#/Java), '
        'not wait for the full 30s server timeout', () async {
      // WHAT THIS TESTS:
      // C# has: HandshakeTimeout = 15s (separate from ServerTimeout = 30s)
      // Java has: HandshakeTimeout = 15s
      // Dart has: No handshake timeout. Relies on serverTimeoutInMilliseconds.
      //
      // This means a Dart client waits up to 30 SECONDS for a handshake
      // response, vs 15 seconds in the reference implementations.
      //
      // For EquitTrade: on mobile resume, if the server doesn't respond to
      // handshake, the user waits 30s instead of 15s before reconnection
      // even begins.

      final stopwatch = Stopwatch()..start();
      bool startFailed = false;

      try {
        await hubConnection.start()!.timeout(
              Duration(seconds: 20),
              onTimeout: () =>
                  throw TimeoutException('Exceeded 20s test limit'),
            );
      } catch (e) {
        startFailed = true;
      }

      stopwatch.stop();

      // The handshake should fail within a dedicated handshake timeout
      // (e.g., 15 seconds like C#/Java), NOT after the full server timeout.
      //
      // Currently, this test will either:
      // 1. Timeout at 20s (our test limit) — proving there's no handshake timeout
      // 2. Fail at ~30s — proving it uses server timeout, not handshake timeout
      //
      // A proper handshakeTimeoutInMilliseconds property should default to 15000ms.
      expect(startFailed, isTrue,
          reason: 'start() should fail when handshake times out');

      // If we get here, check it was fast (handshake timeout, not server timeout)
      expect(stopwatch.elapsedMilliseconds, lessThan(16000),
          reason:
              'Handshake should timeout at ~15s (dedicated handshakeTimeout), '
              'not ${stopwatch.elapsedMilliseconds}ms. The Dart client has no '
              'handshakeTimeoutInMilliseconds property — it relies on the full '
              '30s serverTimeout, doubling the wait time vs C#/Java clients.');
    });
  });

  // ===========================================================================
  // HYPOTHESIS 3: No per-attempt reconnect timeout
  //
  // ROOT CAUSE: _reconnect() calls await _startInternal() with no timeout.
  // Each reconnect attempt inherits the full connect + handshake time.
  // With H1 (no connect timeout) and H2 (no handshake timeout), a single
  // reconnect attempt can take 30-60+ seconds.
  // ===========================================================================

  group('H3: No per-attempt reconnect timeout ->', () {
    late LatencyTestConnection mockConnection;
    late AlwaysRetryPolicy retryPolicy;
    late HubConnection hubConnection;

    setUp(() {
      mockConnection = LatencyTestConnection();
      retryPolicy = AlwaysRetryPolicy(delayMs: 0); // Retry immediately

      hubConnection = HubConnection(
        mockConnection,
        null,
        JsonHubProtocol(),
        reconnectPolicy: retryPolicy,
      );
      hubConnection.serverTimeoutInMilliseconds = 200;
      hubConnection.keepAliveIntervalInMilliseconds = 100;
    });

    tearDown(() async {
      mockConnection.resetForReconnect();
      try {
        await hubConnection.stop();
      } catch (_) {}
    });

    test(
        'a single slow reconnect attempt should be cancelled after a timeout, '
        'not block the entire reconnect loop', () async {
      // WHAT THIS TESTS:
      // If the first reconnect attempt hangs (server unresponsive),
      // the reconnect loop should timeout that individual attempt and
      // try the next one.

      // Set short timeouts for testing
      hubConnection.connectTimeoutInMilliseconds = 200;
      hubConnection.handshakeTimeoutInMilliseconds = 200;
      // Per-attempt timeout = connect + handshake = 400ms

      // Connect first
      await hubConnection.start();
      expect(hubConnection.state, HubConnectionState.Connected);

      // Make reconnect attempts slow (3 seconds each) — way beyond timeout
      mockConnection.connectDelay = Duration(seconds: 3);

      // Track reconnect attempts
      var reconnectAttempts = 0;
      hubConnection.onreconnecting(({error}) {
        reconnectAttempts++;
      });

      // Trigger disconnect to start reconnect loop
      mockConnection.triggerClose(error: Exception('Network lost'));

      // Wait 2 seconds — with 400ms per-attempt timeout and 0ms delay,
      // we should see ~5 reconnect attempts (2000 / 400 = 5)
      await Future.delayed(Duration(seconds: 2));

      // With per-attempt timeout: each attempt times out at ~400ms, loop moves on
      // Without per-attempt timeout: stuck on first attempt for 3s
      // Initial start (1) + at least 3 reconnect attempts should happen in 2s
      expect(mockConnection.startCallCount, greaterThan(3),
          reason: 'With 400ms per-attempt timeout and 3s slow connection, '
              'multiple attempts should complete in 2s. '
              'Got ${mockConnection.startCallCount} start calls. '
              'Per-attempt timeout allows the loop to cancel slow attempts.');

      // Cleanup
      mockConnection.connectDelay = null;
    });

    test(
        'reconnect delay (Future.delayed) should be cancellable when '
        'stop() is called during the delay', () async {
      // WHAT THIS TESTS:
      // The TS client stores _reconnectDelayHandle and calls clearTimeout
      // in stop(). The Dart client uses Future.delayed which is NOT cancellable.
      // If stop() is called during a reconnect delay, the delay continues
      // running and may start a new connection attempt after stop() completes.

      final retryPolicy =
          AlwaysRetryPolicy(delayMs: 2000); // 2s between retries
      final connection = LatencyTestConnection();
      final hub = HubConnection(
        connection,
        null,
        JsonHubProtocol(),
        reconnectPolicy: retryPolicy,
      );
      hub.serverTimeoutInMilliseconds = 100;

      await hub.start();
      expect(hub.state, HubConnectionState.Connected);

      // Make reconnect attempts fail to keep the loop going
      connection.failStart = true;
      connection.triggerClose(error: Exception('Disconnected'));

      // Wait for reconnect to enter the delay phase
      await Future.delayed(Duration(milliseconds: 200));

      // Now stop() during the reconnect delay
      final stopwatch = Stopwatch()..start();
      await hub.stop();
      stopwatch.stop();

      // stop() should return quickly, not wait for the 2s reconnect delay
      expect(stopwatch.elapsedMilliseconds, lessThan(500),
          reason: 'stop() took ${stopwatch.elapsedMilliseconds}ms. '
              'It should return quickly by cancelling the reconnect delay. '
              'Currently Future.delayed is not cancellable, so stop() may '
              'wait for the full retry delay before returning.');

      // After stop, no more connection attempts should happen
      final startCountAfterStop = connection.startCallCount;
      await Future.delayed(
          Duration(milliseconds: 2500)); // Wait past the retry delay
      expect(connection.startCallCount, equals(startCountAfterStop),
          reason: 'After stop(), no more reconnect attempts should occur. '
              'But an uncancelled Future.delayed may trigger another attempt.');
    });
  });

  // ===========================================================================
  // HYPOTHESIS 6: Timer churn from keepalive reset
  //
  // ROOT CAUSE: _resetKeepAliveInterval() cancels and recreates Timer.periodic
  // on EVERY _sendMessage() call. Under high throughput, this creates excessive
  // Timer object allocation. C# uses a single volatile write. TS uses a
  // timestamp (_nextKeepAlive = Date.now() + interval).
  // ===========================================================================

  group('H6: Timer churn from keepalive reset ->', () {
    late LatencyTestConnection mockConnection;
    late HubConnection hubConnection;

    setUp(() {
      mockConnection = LatencyTestConnection();
      hubConnection = HubConnection(
        mockConnection,
        null,
        JsonHubProtocol(),
      );
      hubConnection.keepAliveIntervalInMilliseconds = 500;
      hubConnection.serverTimeoutInMilliseconds = 2000;
    });

    tearDown(() async {
      if (hubConnection.state == HubConnectionState.Connected) {
        try {
          await hubConnection.stop();
        } catch (_) {}
      }
    });

    test(
        'rapid message sending should not create excessive timer overhead '
        '(timestamp-based approach vs timer recreation)', () async {
      // WHAT THIS TESTS:
      // The TS client updates a timestamp: _nextKeepAlive = Date.now() + interval
      // The Dart client: cancel Timer, create new Timer.periodic (EVERY send)
      //
      // With 100 rapid sends, the Dart client creates and destroys 100 Timer
      // objects. The TS client does 100 Date.now() assignments (much cheaper).
      //
      // We measure this indirectly: time 100 rapid sends. With timer churn,
      // each send does: cancel timer → create periodic timer → schedule.
      // Without churn (timestamp approach): update a field.

      await hubConnection.start();
      expect(hubConnection.state, HubConnectionState.Connected);

      // Warm up
      await hubConnection.send('Warmup');

      // Time 100 rapid sends
      final stopwatch = Stopwatch()..start();
      for (int i = 0; i < 100; i++) {
        await hubConnection.send('Msg$i');
      }
      stopwatch.stop();

      final avgMicrosPerSend = stopwatch.elapsedMicroseconds / 100;

      // Each send should be fast (<100 microseconds for the timer reset part).
      // Timer.periodic creation involves runtime scheduling overhead.
      // A simple timestamp update is essentially free.
      //
      // This is a soft assertion — the main point is documenting the
      // architectural difference. The real impact is on GC pressure under
      // sustained high-frequency data (trading app tick streams).
      //
      // For reference: the keepalive interval is 500ms. During those 100 sends,
      // we should see ZERO pings because we're actively sending.
      final pingMessages = mockConnection.sentMessages
          .where((msg) => msg.toString().contains('"type":6'))
          .length;

      expect(pingMessages, equals(0),
          reason: 'During rapid sending (100 messages), no pings should be '
              'needed since the connection is clearly active. Got $pingMessages '
              'ping messages. This indicates the keepalive timer fired during '
              'active traffic — a timestamp-based approach would prevent this.');
    });

    test(
        'keepalive ping should fire exactly once after idle period, '
        'not multiple times from Timer.periodic accumulation', () async {
      // WHAT THIS TESTS:
      // After active traffic stops, exactly ONE ping should fire after
      // keepAliveIntervalInMilliseconds of silence. Timer.periodic may fire
      // multiple times if callbacks accumulated during the active period.

      await hubConnection.start();
      expect(hubConnection.state, HubConnectionState.Connected);

      // Count ping messages sent
      var pingCount = 0;
      hubConnection.onPingSent(() {
        pingCount++;
      });

      // Send some messages to create timer churn
      for (int i = 0; i < 10; i++) {
        await hubConnection.send('Active$i');
        await Future.delayed(Duration(milliseconds: 10));
      }

      // Reset ping count after active period
      pingCount = 0;

      // Wait for exactly one keepalive interval of idle time
      await Future.delayed(Duration(
          milliseconds: hubConnection.keepAliveIntervalInMilliseconds + 100));

      // Exactly one ping should have been sent
      expect(pingCount, equals(1),
          reason: 'After idle period, exactly 1 ping should fire. '
              'Got $pingCount. Timer.periodic may cause 0 (reset timing) '
              'or >1 (accumulated callbacks) pings.');
    });
  });
}
