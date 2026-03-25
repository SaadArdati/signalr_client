import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:signalr_netcore/hub_connection.dart';
import 'package:signalr_netcore/iconnection.dart';
import 'package:signalr_netcore/ihub_protocol.dart';
import 'package:signalr_netcore/itransport.dart';
import 'package:signalr_netcore/json_hub_protocol.dart';
import 'package:signalr_netcore/text_message_format.dart';

/// Mock implementation of IConnection for testing
class MockConnection extends IConnection {
  final List<Object?> sentMessages = [];
  bool _isStarted = false;
  bool _handshakeResponseSent = false;
  bool failNextSend = false;

  @override
  String? connectionId = 'test-connection-id';

  @override
  String? baseUrl = 'http://test.com';

  @override
  Future<void> start({TransferFormat? transferFormat}) async {
    _isStarted = true;
    _handshakeResponseSent = false;
  }

  @override
  Future<void> send(Object? data) async {
    if (failNextSend) {
      failNextSend = false;
      throw Exception('Simulated send failure');
    }

    sentMessages.add(data);
    // After handshake request is sent, trigger handshake response
    if (!_handshakeResponseSent) {
      _handshakeResponseSent = true;
      // Use Timer.run to ensure it runs after current sync operations
      Timer.run(() {
        // Send handshake response with proper format
        final handshakeResponse = '{}${TextMessageFormat.recordSeparator}';
        onreceive?.call(handshakeResponse);
      });
    }
  }

  @override
  Future<void>? stop({Exception? error}) async {
    _isStarted = false;
    onclose?.call(error: error);
  }

  /// Simulate receiving data from server
  void receiveData(Object? data) {
    onreceive?.call(data);
  }
}

void main() {
  group('HubConnection Ping Callbacks ->', () {
    late MockConnection mockConnection;
    late HubConnection hubConnection;
    late JsonHubProtocol protocol;

    setUp(() {
      mockConnection = MockConnection();
      protocol = JsonHubProtocol();
      hubConnection = HubConnection(mockConnection, null, protocol);
      // Set a short keepalive interval for faster tests
      hubConnection.keepAliveIntervalInMilliseconds = 50;
    });

    tearDown(() async {
      // Clean up connection to stop timers
      if (hubConnection.state == HubConnectionState.Connected) {
        await hubConnection.stop();
      }
    });

    test('onPingReceived callback is invoked when ping message is received',
        () async {
      var pingReceivedCount = 0;

      hubConnection.onPingReceived(() {
        pingReceivedCount++;
      });

      // Start connection and wait for it to be connected
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Simulate receiving a ping message from server (type 6 is Ping)
      final pingMessage = TextMessageFormat.write('{"type":6}');
      mockConnection.receiveData(pingMessage);

      expect(pingReceivedCount, equals(1));

      // Receive another ping
      mockConnection.receiveData(pingMessage);
      expect(pingReceivedCount, equals(2));
    });

    test('multiple onPingReceived callbacks are all invoked', () async {
      var callback1Count = 0;
      var callback2Count = 0;

      hubConnection.onPingReceived(() {
        callback1Count++;
      });

      hubConnection.onPingReceived(() {
        callback2Count++;
      });

      // Start connection
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Simulate receiving a ping message from server
      final pingMessage = TextMessageFormat.write('{"type":6}');
      mockConnection.receiveData(pingMessage);

      expect(callback1Count, equals(1));
      expect(callback2Count, equals(1));
    });

    test('onPingSent callback is invoked when ping message is sent', () async {
      var pingSentCount = 0;

      hubConnection.onPingSent(() {
        pingSentCount++;
      });

      // Start connection
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Wait for keepalive interval to trigger ping send
      await Future.delayed(Duration(
          milliseconds: hubConnection.keepAliveIntervalInMilliseconds + 20));

      expect(pingSentCount, greaterThanOrEqualTo(1));
    });

    test('multiple onPingSent callbacks are all invoked', () async {
      var callback1Count = 0;
      var callback2Count = 0;

      hubConnection.onPingSent(() {
        callback1Count++;
      });

      hubConnection.onPingSent(() {
        callback2Count++;
      });

      // Start connection
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Wait for keepalive interval to trigger ping send
      await Future.delayed(Duration(
          milliseconds: hubConnection.keepAliveIntervalInMilliseconds + 20));

      expect(callback1Count, greaterThanOrEqualTo(1));
      expect(callback2Count, greaterThanOrEqualTo(1));
    });

    test('onPingReceived callback handles exceptions gracefully', () async {
      // First callback throws
      hubConnection.onPingReceived(() {
        throw Exception('Test exception');
      });

      // Start connection
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Simulate receiving a ping message - should not crash connection processing
      final pingMessage = TextMessageFormat.write('{"type":6}');
      mockConnection.receiveData(pingMessage);

      // Connection should still be in Connected state (exception was caught)
      expect(hubConnection.state, equals(HubConnectionState.Connected));
    });

    test('callbacks can be registered before connection starts', () async {
      var pingReceivedCount = 0;

      // Register callback before starting
      hubConnection.onPingReceived(() {
        pingReceivedCount++;
      });

      // Start connection
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Simulate receiving a ping message
      final pingMessage = TextMessageFormat.write('{"type":6}');
      mockConnection.receiveData(pingMessage);

      expect(pingReceivedCount, equals(1));
    });

    test('ping sent message appears in connection sent messages', () async {
      // Start connection
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Clear any messages from handshake
      mockConnection.sentMessages.clear();

      // Wait for keepalive interval to trigger ping send
      await Future.delayed(Duration(
          milliseconds: hubConnection.keepAliveIntervalInMilliseconds + 20));

      // Verify ping message was sent
      expect(mockConnection.sentMessages.length, greaterThanOrEqualTo(1));
    });

    test('onPingSent is not invoked if connection is not connected', () async {
      var pingSentCount = 0;

      hubConnection.onPingSent(() {
        pingSentCount++;
      });

      // Don't start the connection, just wait
      await Future.delayed(Duration(
          milliseconds: hubConnection.keepAliveIntervalInMilliseconds + 20));

      // No ping should be sent since we're not connected
      expect(pingSentCount, equals(0));
    });
  });

  group('PingCallback typedef ->', () {
    test('PingCallback is a void Function()', () {
      // This test ensures the typedef is correctly defined
      PingCallback callback = () {
        // Do nothing
      };
      expect(callback, isA<Function>());
    });
  });

  group('HubConnection checkHealth ->', () {
    late MockConnection mockConnection;
    late HubConnection hubConnection;
    late JsonHubProtocol protocol;

    setUp(() {
      mockConnection = MockConnection();
      protocol = JsonHubProtocol();
      hubConnection = HubConnection(mockConnection, null, protocol);
      hubConnection.keepAliveIntervalInMilliseconds = 100;
    });

    tearDown(() async {
      if (hubConnection.state == HubConnectionState.Connected) {
        await hubConnection.stop();
      }
    });

    test('checkHealth returns true when connection is alive', () async {
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Simulate server responding to our ping with a ping back
      Timer(Duration(milliseconds: 50), () {
        final pingMessage = TextMessageFormat.write('{"type":6}');
        mockConnection.receiveData(pingMessage);
      });

      final isHealthy = await hubConnection.checkHealth(
        timeout: Duration(seconds: 1),
      );

      expect(isHealthy, isTrue);
    });

    test('checkHealth returns false when connection is dead (timeout)',
        () async {
      await hubConnection.start();
      expect(hubConnection.state, equals(HubConnectionState.Connected));

      // Don't simulate any response - let it timeout

      final isHealthy = await hubConnection.checkHealth(
        timeout: Duration(milliseconds: 100),
      );

      expect(isHealthy, isFalse);
    });

    test('checkHealth returns false when not connected', () async {
      // Don't start the connection
      expect(hubConnection.state, equals(HubConnectionState.Disconnected));

      final isHealthy = await hubConnection.checkHealth();

      expect(isHealthy, isFalse);
    });

    test('checkHealth returns false when send fails', () async {
      await hubConnection.start();

      // Make the mock fail on send
      mockConnection.failNextSend = true;

      final isHealthy = await hubConnection.checkHealth(
        timeout: Duration(milliseconds: 100),
      );

      expect(isHealthy, isFalse);
    });
  });
}
