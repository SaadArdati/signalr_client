import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:signalr_netcore/hub_connection.dart';
import 'package:signalr_netcore/iconnection.dart';
import 'package:signalr_netcore/ihub_protocol.dart';
import 'package:signalr_netcore/itransport.dart';
import 'package:signalr_netcore/json_hub_protocol.dart';
import 'package:signalr_netcore/text_message_format.dart';

/// Simulates various zombie states that can occur when app is backgrounded/foregrounded

/// Mock connection that can simulate various failure modes
class ZombieTestConnection extends IConnection {
  final List<Object?> sentMessages = [];
  final List<String> eventLog = [];
  bool _handshakeResponseSent = false;

  // Zombie state controls
  bool simulateSendFailure = false;
  bool simulateSendHangs = false;
  bool simulateServerGone = false;  // Server closed connection but client doesn't know
  bool simulateSlowSend = false;
  int sendDelayMs = 0;

  Completer<void>? _hangingCompleter;

  @override
  String? connectionId = 'test-connection-id';

  @override
  String? baseUrl = 'http://test.com';

  @override
  Future<void> start({TransferFormat? transferFormat}) async {
    eventLog.add('start called');
    _handshakeResponseSent = false;
  }

  @override
  Future<void> send(Object? data) async {
    eventLog.add('send called: ${data.toString().substring(0, 20)}...');

    if (simulateSendHangs) {
      eventLog.add('send hanging indefinitely');
      _hangingCompleter = Completer<void>();
      await _hangingCompleter!.future;  // Never completes
      return;
    }

    if (simulateSendFailure) {
      eventLog.add('send throwing error');
      throw Exception('Simulated send failure - connection dead');
    }

    if (simulateSlowSend) {
      eventLog.add('send delayed by ${sendDelayMs}ms');
      await Future.delayed(Duration(milliseconds: sendDelayMs));
    }

    sentMessages.add(data);

    // Trigger handshake response on first message (handshake request)
    if (!_handshakeResponseSent) {
      _handshakeResponseSent = true;
      Timer.run(() {
        if (!simulateServerGone) {
          final handshakeResponse = '{}${TextMessageFormat.recordSeparator}';
          onreceive?.call(handshakeResponse);
        } else {
          eventLog.add('server gone - not sending handshake response');
        }
      });
    }
  }

  @override
  Future<void>? stop({Exception? error}) async {
    eventLog.add('stop called: ${error?.toString() ?? 'no error'}');
    onclose?.call(error: error);
  }

  /// Simulate receiving data from server
  void receiveData(Object? data) {
    eventLog.add('receiveData: ${data.toString().substring(0, 20)}...');
    onreceive?.call(data);
  }

  /// Simulate the server closing the connection without client knowing
  void simulateServerDisconnect() {
    eventLog.add('simulateServerDisconnect - calling onclose');
    onclose?.call(error: Exception('Server disconnected'));
  }

  /// Simulate connection dying silently (no onclose callback)
  void simulateSilentDeath() {
    eventLog.add('simulateSilentDeath - connection is dead but no callback');
    simulateSendFailure = true;
    // Note: NOT calling onclose - this is the zombie state
  }

  /// Release any hanging operations
  void releaseHanging() {
    if (_hangingCompleter != null && !_hangingCompleter!.isCompleted) {
      _hangingCompleter!.completeError('Released');
    }
  }
}


void main() {
  group('Zombie State Diagnostics ->', () {
    late ZombieTestConnection mockConnection;
    late HubConnection hubConnection;
    late JsonHubProtocol protocol;

    setUp(() {
      mockConnection = ZombieTestConnection();
      protocol = JsonHubProtocol();
      hubConnection = HubConnection(mockConnection, null, protocol);
      hubConnection.keepAliveIntervalInMilliseconds = 100;
      hubConnection.serverTimeoutInMilliseconds = 500;
    });

    tearDown(() async {
      mockConnection.releaseHanging();
      if (hubConnection.state == HubConnectionState.Connected ||
          hubConnection.state == HubConnectionState.Connecting) {
        try {
          await hubConnection.stop();
        } catch (e) {
          // Ignore cleanup errors
        }
      }
    });

    group('Zombie State 1: Server gone but client thinks connected ->', () {
      test('send fails after server silently closes connection', () async {
        // Connect normally
        await hubConnection.start();
        expect(hubConnection.state, equals(HubConnectionState.Connected));

        // Simulate server going away without proper close
        mockConnection.simulateSilentDeath();

        // Try to invoke a method - what happens?
        bool sendFailed = false;
        try {
          await hubConnection.send('TestMethod');
        } catch (e) {
          sendFailed = true;
          print('Send failed with: $e');
        }

        // Document the behavior
        print('Event log: ${mockConnection.eventLog}');
        print('Hub state after failed send: ${hubConnection.state}');

        // The question: Does the hub detect the dead connection?
        // If state is still Connected, we have a zombie!
        if (hubConnection.state == HubConnectionState.Connected && sendFailed) {
          print('ZOMBIE DETECTED: Hub thinks connected but cannot send');
        }
      });

      test('ping timer continues when connection is dead', () async {
        var pingSentCount = 0;
        var pingErrors = <String>[];

        hubConnection.onPingSent(() {
          pingSentCount++;
        });

        await hubConnection.start();
        expect(hubConnection.state, equals(HubConnectionState.Connected));

        // Let a ping succeed
        await Future.delayed(Duration(milliseconds: 150));
        final pingBeforeDeath = pingSentCount;
        print('Pings before death: $pingBeforeDeath');

        // Kill the connection silently
        mockConnection.simulateSilentDeath();

        // Wait for more ping attempts
        await Future.delayed(Duration(milliseconds: 300));

        print('Event log: ${mockConnection.eventLog}');
        print('Pings after death: $pingSentCount');
        print('Hub state: ${hubConnection.state}');

        // If ping count increased but connection is dead, timer is zombie
        if (pingSentCount > pingBeforeDeath) {
          print('ZOMBIE TIMER: Ping timer still running on dead connection');
        }
      });
    });

    group('Zombie State 2: Reconnection with stale state ->', () {
      test('reconnect attempt uses stale connection state', () async {
        var reconnectAttempts = 0;
        var reconnectErrors = <String>[];

        hubConnection.onreconnecting(({error}) {
          reconnectAttempts++;
          reconnectErrors.add(error?.toString() ?? 'no error');
          print('Reconnecting attempt $reconnectAttempts: $error');
        });

        hubConnection.onreconnected(({connectionId}) {
          print('Reconnected with ID: $connectionId');
        });

        // Connect normally
        await hubConnection.start();
        expect(hubConnection.state, equals(HubConnectionState.Connected));

        // Simulate server disconnect (proper close notification)
        mockConnection.simulateServerDisconnect();

        // Give time for reconnection to start
        await Future.delayed(Duration(milliseconds: 100));

        print('Event log: ${mockConnection.eventLog}');
        print('Reconnect attempts: $reconnectAttempts');
        print('Hub state: ${hubConnection.state}');
      });

      test('reconnect while previous reconnect is in progress', () async {
        var reconnectAttempts = 0;

        hubConnection.onreconnecting(({error}) {
          reconnectAttempts++;
          print('Reconnecting attempt $reconnectAttempts');
        });

        await hubConnection.start();

        // Make reconnection slow
        mockConnection.simulateSlowSend = true;
        mockConnection.sendDelayMs = 200;

        // Trigger disconnect
        mockConnection.simulateServerDisconnect();

        await Future.delayed(Duration(milliseconds: 50));

        // While reconnecting, trigger another disconnect
        // This simulates rapid network flapping
        mockConnection.simulateServerDisconnect();

        await Future.delayed(Duration(milliseconds: 100));

        print('Event log: ${mockConnection.eventLog}');
        print('Final state: ${hubConnection.state}');
        print('Reconnect attempts: $reconnectAttempts');
      });
    });

    group('Zombie State 3: Handshake timeout scenarios ->', () {
      test('handshake never completes (server does not respond)', () async {
        mockConnection.simulateServerGone = true;

        var startFailed = false;
        var startError = '';

        try {
          // This should timeout eventually
          final startFuture = hubConnection.start();
          if (startFuture != null) {
            await startFuture.timeout(
              Duration(seconds: 2),
              onTimeout: () {
                throw TimeoutException('Start timed out');
              },
            );
          }
        } catch (e) {
          startFailed = true;
          startError = e.toString();
        }

        print('Event log: ${mockConnection.eventLog}');
        print('Start failed: $startFailed');
        print('Error: $startError');
        print('Hub state: ${hubConnection.state}');

        // Check if we're stuck in Connecting state
        if (hubConnection.state == HubConnectionState.Connecting) {
          print('ZOMBIE: Connection stuck in Connecting state');
        }
      });

      test('server sends handshake but then dies before first message', () async {
        await hubConnection.start();
        expect(hubConnection.state, equals(HubConnectionState.Connected));

        // Now make the server unresponsive
        mockConnection.simulateServerGone = true;
        mockConnection.simulateSendFailure = true;

        // Wait for server timeout
        await Future.delayed(Duration(
          milliseconds: hubConnection.serverTimeoutInMilliseconds + 100,
        ));

        print('Event log: ${mockConnection.eventLog}');
        print('Hub state after timeout: ${hubConnection.state}');
      });
    });

    group('Zombie State 4: Send queue issues ->', () {
      test('messages queued while connection is dying', () async {
        await hubConnection.start();

        // Start multiple sends and collect errors
        var errors = 0;

        try { await hubConnection.send('Method1'); } catch (e) { errors++; print('Send1 error: $e'); }
        try { await hubConnection.send('Method2'); } catch (e) { errors++; print('Send2 error: $e'); }

        // Kill connection mid-send
        mockConnection.simulateSendFailure = true;

        try { await hubConnection.send('Method3'); } catch (e) { errors++; print('Send3 error: $e'); }

        print('Event log: ${mockConnection.eventLog}');
        print('Send errors: $errors out of 3');
        print('Hub state: ${hubConnection.state}');

        // Document the zombie behavior
        if (hubConnection.state == HubConnectionState.Connected && errors > 0) {
          print('ZOMBIE: State still Connected despite send failures');
        }
      });

      test('send hangs indefinitely', () async {
        await hubConnection.start();

        mockConnection.simulateSendHangs = true;

        var sendCompleted = false;
        var sendError = '';

        // This send should hang
        hubConnection.send('HangingMethod').then((_) {
          sendCompleted = true;
        }).catchError((e) {
          sendError = e.toString();
        });

        // Wait a bit
        await Future.delayed(Duration(milliseconds: 200));

        print('Event log: ${mockConnection.eventLog}');
        print('Send completed: $sendCompleted');
        print('Send error: $sendError');
        print('Hub state: ${hubConnection.state}');

        if (!sendCompleted && sendError.isEmpty) {
          print('ZOMBIE: Send hanging - no completion, no error');
        }
      });
    });

    group('Zombie State 5: State machine inconsistencies ->', () {
      test('stop during start', () async {
        // Don't send handshake response
        mockConnection.simulateServerGone = true;

        // Start connecting
        var startFuture = hubConnection.start();

        // Immediately try to stop
        await Future.delayed(Duration(milliseconds: 10));
        var stopFuture = hubConnection.stop();

        print('State after stop called: ${hubConnection.state}');

        try {
          if (startFuture != null) {
            await startFuture.timeout(Duration(milliseconds: 500));
          }
        } catch (e) {
          print('Start error: $e');
        }

        try {
          await stopFuture;
        } catch (e) {
          print('Stop error: $e');
        }

        print('Event log: ${mockConnection.eventLog}');
        print('Final state: ${hubConnection.state}');

        // Should be Disconnected
        if (hubConnection.state != HubConnectionState.Disconnected) {
          print('ZOMBIE: Not in Disconnected state after stop');
        }
      });

      test('double stop', () async {
        await hubConnection.start();

        // Stop twice
        await hubConnection.stop();
        print('State after first stop: ${hubConnection.state}');

        await hubConnection.stop();
        print('State after second stop: ${hubConnection.state}');

        print('Event log: ${mockConnection.eventLog}');
      });

      test('start after stop without waiting', () async {
        await hubConnection.start();

        // Start stop but don't await
        var stopFuture = hubConnection.stop();

        // Immediately try to start again
        bool startFailed = false;
        try {
          await hubConnection.start();
        } catch (e) {
          startFailed = true;
          print('Start error: $e');
        }

        await stopFuture;

        print('Event log: ${mockConnection.eventLog}');
        print('Start failed: $startFailed');
        print('Final state: ${hubConnection.state}');
      });
    });

    group('Zombie State 6: Background/Foreground simulation ->', () {
      test('simulated backgrounding kills connection, foregrounding reconnects', () async {
        var reconnectCount = 0;
        var stateChanges = <HubConnectionState>[];

        hubConnection.stateStream.listen((state) {
          stateChanges.add(state);
          print('State changed to: $state');
        });

        hubConnection.onreconnecting(({error}) {
          reconnectCount++;
          print('Reconnecting #$reconnectCount');
        });

        // Connect
        await hubConnection.start();
        expect(hubConnection.state, equals(HubConnectionState.Connected));

        print('\n--- SIMULATING BACKGROUND ---');
        // Simulate backgrounding: server closes connection
        mockConnection.simulateServerDisconnect();

        // Wait for reconnection attempts
        await Future.delayed(Duration(milliseconds: 300));

        print('\n--- EVENT LOG ---');
        for (var event in mockConnection.eventLog) {
          print('  $event');
        }

        print('\n--- STATE CHANGES ---');
        for (var state in stateChanges) {
          print('  $state');
        }

        print('\nReconnect attempts: $reconnectCount');
        print('Final state: ${hubConnection.state}');
      });
    });
  });
}
