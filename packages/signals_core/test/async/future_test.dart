import 'dart:async';

import 'package:signals_core/signals_core.dart';
import 'package:test/test.dart';

void main() {
  SignalsObserver.instance = null;
  group('Future', () {
    test('awaiters receive late success without publishing after disposal',
        () async {
      final operation = Completer<int>();
      final s = futureSignal(() => operation.future);
      final pending = s.future;
      final state = s.peek();
      s.dispose();
      operation.complete(42);
      expect(await pending, 42);
      expect(s.peek(), same(state));
      expect(
        () => s.setValue(7),
        throwsA(isA<SignalsWriteAfterDisposeError>()),
      );
    });

    test('awaiters receive the original late error and stack after disposal',
        () async {
      final operation = Completer<int>();
      final error = StateError('load failed');
      final stack = StackTrace.current;
      final s = futureSignal(() => operation.future);
      final pending = s.future.then<void>(
        (_) => fail('Expected the original error'),
        onError: (Object caught, StackTrace trace) {
          expect(caught, same(error));
          expect(trace, same(stack));
        },
      );
      final state = s.peek();
      s.dispose();
      operation.completeError(error, stack);
      await pending;
      expect(s.peek(), same(state));
    });

    test('unawaited late errors are handled after disposal', () async {
      final operation = Completer<int>();
      final s = futureSignal(() => operation.future);
      final state = s.value;
      s.dispose();
      operation.completeError(StateError('load failed'));
      await Future<void>.delayed(Duration.zero);
      expect(s.peek(), same(state));
    });

    test('only the latest pending run settles awaiters after disposal',
        () async {
      final dependency = signal(0);
      final first = Completer<int>();
      final latest = Completer<int>();
      var calls = 0;
      final s = futureSignal(() {
        calls++;
        return dependency.value == 0 ? first.future : latest.future;
      });
      var settled = false;
      final pending = s.future.then((value) {
        settled = true;
        return value;
      });
      dependency.value = 1;
      expect(calls, 2);
      final state = s.peek();
      s.dispose();
      dependency.value = 2;
      first.completeError(StateError('stale failure'));
      await Future<void>.delayed(Duration.zero);
      expect(settled, false);
      latest.complete(42);
      expect(await pending, 42);
      expect(s.peek(), same(state));
      expect(calls, 2);
    });

    test('reset keeps pending awaiters attached to the replacement run',
        () async {
      final first = Completer<int>();
      final latest = Completer<int>();
      var calls = 0;
      final s = futureSignal(() => calls++ == 0 ? first.future : latest.future);
      var settled = false;
      final pending = s.future.then((value) {
        settled = true;
        return value;
      });
      s.reset();
      final replacement = s.future;
      first.complete(1);
      await Future<void>.delayed(Duration.zero);
      expect(settled, false);
      latest.complete(2);
      expect(await pending, 2);
      expect(await replacement, 2);
      expect(s.requireValue, 2);
      s.dispose();
    });

    test('disposed lazy signals cannot start work or reattach dependencies',
        () async {
      var calls = 0;
      final s = futureSignal(() async => ++calls);
      s.dispose();
      expect(s.peek().isLoading, true);
      expect(s.init, throwsA(isA<SignalsWriteAfterDisposeError>()));
      expect(s.reset, throwsA(isA<SignalsWriteAfterDisposeError>()));
      await expectLater(
        s.reload(),
        throwsA(isA<SignalsWriteAfterDisposeError>()),
      );
      await expectLater(
        s.refresh(),
        throwsA(isA<SignalsWriteAfterDisposeError>()),
      );
      expect(calls, 0);
    });

    test('refresh and reload do not restart work across disposal', () async {
      for (final reload in [false, true]) {
        final operation = Completer<int>();
        var calls = 0;
        final s = futureSignal(() {
          calls++;
          return operation.future;
        });
        final pending = s.future;
        var settled = false;
        final refreshing = (reload ? s.reload() : s.refresh()).then((_) {
          settled = true;
        });
        s.dispose();
        await Future<void>.delayed(Duration.zero);
        expect(settled, false);
        operation.complete(42);
        expect(await pending, 42);
        await refreshing;
        expect(calls, 1);
      }
    });

    test('refresh and reload preserve late errors across disposal', () async {
      for (final reload in [false, true]) {
        final operation = Completer<int>();
        final error = StateError('refresh failed');
        final s = futureSignal(() => operation.future);
        final pending = expectLater(s.future, throwsA(same(error)));
        final refreshing = expectLater(
          reload ? s.reload() : s.refresh(),
          throwsA(same(error)),
        );
        s.dispose();
        operation.completeError(error);
        await Future.wait([pending, refreshing]);
      }
    });

    test('futureSignal', () async {
      Future<int> future() async {
        await Future.delayed(const Duration(milliseconds: 5));
        return 10;
      }

      final signal = futureSignal(() => future());
      expect(signal.peek().isLoading, true);

      final completer = Completer<int>();
      effect(() {
        signal.value;
        if (signal.value.hasValue) {
          completer.complete(signal.peek().requireValue);
        }
      });
      final result = await completer.future;

      expect(result, 10);
    });

    test('extension on Future', () async {
      Future<int> future() async {
        await Future.delayed(const Duration(milliseconds: 5));
        return 10;
      }

      final signal = future().toFutureSignal();
      expect(signal.peek().isLoading, true);

      final completer = Completer<int>();
      effect(() {
        signal.value;
        if (signal.value.hasValue) {
          completer.complete(signal.peek().requireValue);
        }
      });
      final result = await completer.future;

      expect(result, 10);
    });

    test('check repeated calls', () async {
      int calls = 0;

      Future<int> future() async {
        calls++;
        await Future.delayed(const Duration(milliseconds: 5));
        return 10;
      }

      final signal = futureSignal(() => future());
      expect(signal.peek().isLoading, true);
      expect(calls, 1);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);
    });

    test('check reload calls', () async {
      int calls = 0;

      Future<int> future() async {
        calls++;
        await Future.delayed(const Duration(milliseconds: 5));
        return 10;
      }

      final signal = futureSignal(() => future());
      expect(signal.peek().isLoading, true);
      expect(calls, 1);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.reload();

      expect(calls, 2);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);
    });

    test('check refresh calls', () async {
      int calls = 0;

      Future<int> future() async {
        calls++;
        await Future.delayed(const Duration(milliseconds: 5));
        return 10;
      }

      final signal = futureSignal(() => future());
      expect(signal.peek().isLoading, true);
      expect(calls, 1);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.future;

      expect(calls, 1);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);

      await signal.refresh();

      expect(calls, 2);
      expect(signal.value.value, 10);
      expect(signal.value.error, null);
    });

    test('dependencies', () async {
      final prefix = signal('a');
      final val = signal(0);
      final f = futureSignal(() async {
        final p = prefix();
        final v = val();
        await Future.delayed(const Duration(seconds: 1));
        return '$p$v';
      });
      expect(f.peek().isLoading, true);

      var result = await f.future;

      expect(result, 'a0');

      prefix.value = 'b';
      await f.future;
      result = f.requireValue;

      expect(result, 'b0');
    });

    test('computed future emits once (issue #433)', () async {
      int calls = 0;
      int signalCalls = 0;
      int computedCalls = 0;
      int heavyComputationCalls = 0;

      Future<int> future() async {
        calls++;
        await Future.delayed(const Duration(milliseconds: 5));
        return 10;
      }

      final signal = futureSignal(() async {
        signalCalls++;
        return await future();
      });

      Future<int> heavyComputation(int value) async {
        heavyComputationCalls++;
        await Future.delayed(const Duration(milliseconds: 5));
        return value * 2;
      }

      final computed = futureSignal(
        () async {
          computedCalls++;
          final value = await signal.future;
          return await heavyComputation(value);
        },
        options: AsyncSignalOptions(dependencies: [signal]),
      );

      final result = await computed.future;

      expect(result, 20, reason: 'unexpected result');
      expect(
        calls,
        1,
        reason: 'the future function was called an unexpected number of times',
      );
      expect(
        signalCalls,
        1,
        reason: 'the signal function was called an unexpected number of times',
      );
      expect(
        heavyComputationCalls,
        1,
        reason:
            'the heavy computation function was called an unexpected number of times',
      );
      expect(
        computedCalls,
        1,
        reason:
            'the computed function was called an unexpected number of times',
      );
    });

    test('deprecated parameters and basic fallback options', () async {
      final s = futureSignal(
        () async => 42,
        initialValue: 0,
        lazy: false,
        autoDispose: true,
        debugLabel: 'dep-test',
      );
      expect(s.peek().value, 0);
      await s.future;
      expect(s.peek().value, 42);
    });

    test('dependencies containing AsyncSignal', () async {
      final dep = asyncSignal<int>(AsyncState.data(1));
      final s = futureSignal(
        () async => dep.value.value ?? 0,
        options: AsyncSignalOptions(dependencies: [dep]),
      );

      await s.future;
      expect(s.value.value, 1);

      // Trigger dependency update to AsyncLoading
      dep.value = AsyncState.loading();
      // Then trigger loaded value to hit loaded bypass branch
      dep.value = AsyncState.data(2);

      await s.future;
      expect(s.value.value, 2);
    });

    test('dispose cleanups', () async {
      final dep = signal(0);
      final s = futureSignal(
        () async => dep.value,
        options: AsyncSignalOptions(dependencies: [dep]),
      );
      await s.future;
      s.dispose();
      expect(s.disposed, true);
    });
  });
}
